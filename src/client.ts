import { VoiceAgentApiError, VoiceAgentStateError, VoiceAgentStreamError, VoiceAgentTimeoutError } from './errors.js';
import { EventStream, type EventsOptions } from './events.js';
import type { AgentState, AgentStatus, InviteRequest, InviteResponse, LifecycleEvent, SnapshotAgent } from './types.js';
import { conferenceParts, normalizeAgentId, readText, sleep } from './util.js';

export interface VoiceAgentClientOptions {
    /** `https://<jaas-api>/v1/voice-agent`, or `https://<prosody>/voice-agent` when self-hosted. */
    baseUrl: string;
    /** The bearer token, or a function returning the current one (for short-lived JWTs). */
    token: string | (() => string | Promise<string>);
    /** Defaults to the global `fetch` (Node 18+). Called as a plain function, without a `this`. */
    fetch?: typeof globalThis.fetch;
    /** Extra headers on every request. */
    headers?: Record<string, string>;
    /**
     * Accept an `http://` base URL on a host other than loopback. Off by default: the bearer token travels in
     * every request, so plain HTTP is for a private network behind its own TLS termination only.
     */
    allowHttp?: boolean;
    /**
     * How requests reach the deployment. `direct` (the default) calls `<baseUrl>/<route>`: one prosody, or a
     * gateway that routes by itself. `haproxy` addresses a sharded Jitsi deployment through its HAProxy, which
     * picks the shard holding the room from the URL: the tenant becomes a path prefix and the room a `room`
     * query parameter appended last, spelled as jitsi-meet spells it on the meeting's own connection,
     * `https://<signal-api host>/<tenant>/voice-agent/invite?room=<room>`. The conference is then required on
     * every call, `events()` included.
     */
    routing?: 'direct' | 'haproxy';
}

export interface RequestOptions {
    signal?: AbortSignal;
}

export interface InviteOptions extends RequestOptions {
    /**
     * A room exists once its first participant joins, so an early invite answers 404. With this set the call
     * retries that answer until the room appears: `true` means 30 s at 1 s intervals.
     */
    waitForRoom?: boolean | { timeoutMs?: number; intervalMs?: number };
}

export interface WaitForOptions {
    /** Default 30 s. */
    timeoutMs?: number;
    signal?: AbortSignal;
}

const TERMINAL: ReadonlySet<AgentState> = new Set<AgentState>([ 'failed', 'ended' ]);
/** Largest REST response body the client reads; the API's answers are a few hundred bytes. */
const MAX_BODY_BYTES = 4_194_304;

const isLoopback = (hostname: string) => hostname === 'localhost' || hostname === '[::1]' || /^127\./.test(hostname);

export class VoiceAgentClient {
    private readonly baseUrl: string;
    private readonly base: URL;
    private readonly basePath: string;
    private readonly routing: 'direct' | 'haproxy';
    private readonly token: VoiceAgentClientOptions['token'];
    private readonly fetchImpl: typeof globalThis.fetch;
    private readonly headers: Record<string, string>;

    constructor(options: VoiceAgentClientOptions) {
        if (!options.baseUrl) {
            throw new TypeError('baseUrl is required');
        }
        let base: URL;

        try {
            base = new URL(options.baseUrl);
        } catch {
            throw new TypeError('baseUrl must be an absolute URL');
        }
        if (base.protocol === 'http:' ? !(options.allowHttp || isLoopback(base.hostname)) : base.protocol !== 'https:') {
            throw new TypeError(`baseUrl must be https:// (got ${base.protocol}//); set allowHttp: true for a private network`);
        }
        if (!options.token) {
            throw new TypeError('token is required');
        }
        const fetchImpl = options.fetch ?? globalThis.fetch;

        if (typeof fetchImpl !== 'function') {
            throw new TypeError('no fetch available; pass one in options.fetch');
        }
        if (options.routing !== undefined && options.routing !== 'direct' && options.routing !== 'haproxy') {
            throw new TypeError(`routing must be "direct" or "haproxy" (got ${JSON.stringify(options.routing)})`);
        }
        this.baseUrl = options.baseUrl.replace(/\/+$/, '');
        this.base = base;
        this.basePath = base.pathname.replace(/\/+$/, '');
        this.routing = options.routing ?? 'direct';
        this.token = options.token;
        // Called as a plain function: Workers reject the global fetch invoked with the client as `this`.
        this.fetchImpl = (input, init) => fetchImpl(input, init);
        this.headers = { ...options.headers ?? {} };
    }

    /** Places an agent in a meeting. Idempotent on `agentId`: an identical repeat returns the existing agent. */
    async invite(request: InviteRequest, options: InviteOptions = {}): Promise<InviteResponse> {
        const wait = options.waitForRoom;
        const timeoutMs = typeof wait === 'object' ? wait.timeoutMs ?? 30_000 : 30_000;
        const intervalMs = typeof wait === 'object' ? wait.intervalMs ?? 1000 : 1000;
        const deadline = Date.now() + timeoutMs;

        for (;;) {
            try {
                return await this.post<InviteResponse>('invite', request, request.conference, options.signal);
            } catch (error) {
                const roomMissing = error instanceof VoiceAgentApiError && error.status === 404;

                if (!wait || !roomMissing || Date.now() + intervalMs > deadline || options.signal?.aborted) {
                    throw error;
                }
                await sleep(intervalMs, options.signal);
            }
        }
    }

    /** Removes an agent and closes its media leg. */
    dismiss(conference: string, agentId: string, options: RequestOptions = {}): Promise<{ agentId: string }> {
        return this.post<{ agentId: string }>('dismiss', { conference, agentId }, conference, options.signal);
    }

    /** Every agent in a meeting, keyed by `agentId`. */
    async list(conference: string, options: RequestOptions = {}): Promise<Record<string, AgentStatus>> {
        const response = await this.getJson<{ agents: Record<string, AgentStatus> }>('list', { conference }, conference, options.signal);

        return response.agents ?? {};
    }

    /** One agent's state, or `undefined` once it is gone. */
    async get(conference: string, agentId: string, options: RequestOptions = {}): Promise<AgentStatus | undefined> {
        try {
            return await this.getJson<AgentStatus>('get', { conference, agentId }, conference, options.signal);
        } catch (error) {
            if (error instanceof VoiceAgentApiError && error.status === 404) {
                return undefined;
            }
            throw error;
        }
    }

    /** The lifecycle event stream; see {@link EventStream}. */
    events(options: EventsOptions = {}): EventStream {
        if (this.routing === 'haproxy' && !options.conference) {
            throw new TypeError('events() needs a conference with haproxy routing: a stream lives on the shard holding the room');
        }
        const open = async (lastEventId: string | undefined, signal: AbortSignal) => {
            const headers: Record<string, string> = { ...this.headers, ...await this.authHeaders(), accept: 'text/event-stream' };

            if (lastEventId !== undefined) {
                headers['last-event-id'] = lastEventId;
            }

            return this.fetchImpl(this.url('events', { conference: options.conference }, options.conference), { method: 'GET', headers, signal });
        };

        return new EventStream(open, options);
    }

    /**
     * Resolves when the agent reaches one of the given states, from the stream's snapshot or a later event.
     * Rejects with {@link VoiceAgentStateError} when it reaches a different terminal state, and with
     * {@link VoiceAgentTimeoutError} after `timeoutMs`.
     */
    async waitFor(
            conference: string,
            agentId: string,
            state: AgentState | AgentState[],
            options: WaitForOptions = {}): Promise<SnapshotAgent | LifecycleEvent> {
        const targets = new Set(Array.isArray(state) ? state : [ state ]);
        const id = normalizeAgentId(agentId);
        const controller = new AbortController();
        const onAbort = () => controller.abort();
        const timer = setTimeout(onAbort, options.timeoutMs ?? 30_000);

        options.signal?.addEventListener('abort', onAbort, { once: true });
        const stream = this.events({ conference, signal: controller.signal });

        try {
            for await (const event of stream) {
                if (event.event === 'snapshot') {
                    const agent = event.agents.find(candidate => candidate.agentId === id);

                    if (!agent) {
                        continue;
                    }
                    if (targets.has(agent.state)) {
                        return agent;
                    }
                    if (TERMINAL.has(agent.state)) {
                        throw new VoiceAgentStateError(id, agent.state);
                    }
                } else if (event.agentId === id) {
                    if (targets.has(event.state)) {
                        return event;
                    }
                    if (TERMINAL.has(event.state)) {
                        throw new VoiceAgentStateError(id, event.state, event.reason);
                    }
                }
            }
            throw new VoiceAgentStreamError(`the event stream ended before ${id} reached ${[ ...targets ].join(' or ')}`);
        } catch (error) {
            if (controller.signal.aborted && !options.signal?.aborted) {
                throw new VoiceAgentTimeoutError(`${id} did not reach ${[ ...targets ].join(' or ')} within ${options.timeoutMs ?? 30_000} ms`);
            }
            throw error;
        } finally {
            clearTimeout(timer);
            options.signal?.removeEventListener('abort', onAbort);
            stream.close();
        }
    }

    private async authHeaders(): Promise<Record<string, string>> {
        const token = typeof this.token === 'function' ? await this.token() : this.token;

        return { authorization: token.startsWith('Bearer ') ? token : `Bearer ${token}` };
    }

    private url(path: string, query: Record<string, string | undefined> = {}, conference?: string): string {
        let url: URL;
        let room: string | undefined;

        if (this.routing === 'haproxy') {
            if (!conference) {
                throw new TypeError('a conference is required with haproxy routing');
            }
            if (conference.includes('@')) {
                throw new TypeError('haproxy routing needs the conference as the meeting URL spells it (<tenant>/<room>), not a JID');
            }
            const parts = conferenceParts(conference);

            // The tenant stays one path segment whatever the name holds.
            url = new URL(`${this.base.origin}${parts.tenant ? `/${encodeURIComponent(parts.tenant)}` : ''}${this.basePath}/${path}`);
            room = parts.room;
        } else {
            url = new URL(`${this.baseUrl}/${path}`);
        }
        for (const [ key, value ] of Object.entries(query)) {
            if (value !== undefined) {
                url.searchParams.set(key, value);
            }
        }
        if (room !== undefined) {
            // HAProxy keys the shard on the tenant path prefix plus this parameter, compared byte for byte (lower-cased,
            // %25 → %) with the one jitsi-meet appends to the meeting's own connection with URLSearchParams: the same
            // form encoding of the same decoded, lower-cased name (a space is +, parentheses are escaped), last as there.
            url.searchParams.set('room', room);
        }

        return url.toString();
    }

    private async post<T>(path: string, body: unknown, conference: string, signal?: AbortSignal): Promise<T> {
        const response = await this.fetchImpl(this.url(path, {}, conference), {
            method: 'POST',
            headers: { ...this.headers, ...await this.authHeaders(), accept: 'application/json', 'content-type': 'application/json' },
            body: JSON.stringify(body),
            signal
        });

        return this.json<T>(response);
    }

    private async getJson<T>(path: string, query: Record<string, string | undefined>, conference: string, signal?: AbortSignal): Promise<T> {
        const response = await this.fetchImpl(this.url(path, query, conference), {
            method: 'GET',
            headers: { ...this.headers, ...await this.authHeaders(), accept: 'application/json' },
            signal
        });

        return this.json<T>(response);
    }

    private async json<T>(response: Response): Promise<T> {
        const { text, truncated } = await readText(response, MAX_BODY_BYTES);

        if (truncated) {
            throw new VoiceAgentStreamError(`response body exceeds ${MAX_BODY_BYTES} bytes`);
        }
        let parsed: unknown = text;

        try {
            parsed = text ? JSON.parse(text) : undefined;
        } catch {
            // keep the text as the body
        }
        if (!response.ok) {
            throw new VoiceAgentApiError(response.status, parsed);
        }

        return parsed as T;
    }
}
