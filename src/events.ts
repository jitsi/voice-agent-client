import { VoiceAgentApiError, VoiceAgentStreamError } from './errors.js';
import { SseParser, type SseMessage } from './sse.js';
import type { SnapshotAgent, StreamEvent } from './types.js';
import { delayOption, EVENT_ID_SHAPE, isRecord, isString, MIN_RETRY_MS, parseJson, printable, readText, redirectNote, sleep } from './util.js';

export interface EventsOptions {
    /** Only this room's events; omit for every room the credential can see. */
    conference?: string;
    signal?: AbortSignal;
    /** Resume from an id seen on an earlier stream. */
    lastEventId?: string;
    /** Reconnect after a dropped or server-closed stream (default: on, 1 s doubling to 30 s). */
    reconnect?: boolean | { initialDelayMs?: number; maxDelayMs?: number };
}

export type StreamOpener = (lastEventId: string | undefined, signal: AbortSignal) => Promise<Response>;

const LIFECYCLE = new Set([ 'agent.connected', 'agent.failed', 'agent.ended' ]);
/** A stream that ends sooner than this did not work: it is redialed with backoff, not at once. */
const MIN_STREAM_MS = 1000;
/** How much of an error body is kept for the error message. */
const MAX_ERROR_BODY_BYTES = 65_536;

const jitter = (ms: number) => Math.round(ms * (0.8 + Math.random() * 0.4));

/**
 * One logical stream of lifecycle events across reconnects. Iterate it with `for await`; stop with `close()` or
 * the `signal` passed in. The server closes a stream at its max age; the client reconnects with `Last-Event-ID`
 * and resumes where it left off, or receives a fresh `snapshot` when the gap can no longer be replayed.
 */
export class EventStream implements AsyncIterable<StreamEvent> {
    private readonly controller = new AbortController();
    private readonly onAbort = () => this.close();
    private closed = false;
    private _lastEventId?: string;

    constructor(private readonly open: StreamOpener, private readonly options: EventsOptions) {
        if (options.lastEventId !== undefined && EVENT_ID_SHAPE.test(options.lastEventId)) {
            this._lastEventId = options.lastEventId;
        }
        if (options.signal?.aborted) {
            this.close();
        } else {
            options.signal?.addEventListener('abort', this.onAbort, { once: true });
        }
    }

    /** The id of the last event received; pass it as `lastEventId` to resume on a new stream. */
    get lastEventId(): string | undefined {
        return this._lastEventId;
    }

    close(): void {
        this.closed = true;
        this.options.signal?.removeEventListener('abort', this.onAbort);
        this.controller.abort();
    }

    async *[Symbol.asyncIterator](): AsyncGenerator<StreamEvent> {
        const reconnect = this.options.reconnect ?? true;
        const initialDelay = delayOption(typeof reconnect === 'object' ? reconnect.initialDelayMs : undefined, 1000);
        const maxDelay = Math.max(delayOption(typeof reconnect === 'object' ? reconnect.maxDelayMs : undefined, 30_000), initialDelay);
        let delay = initialDelay;
        // The server's `retry:` hint, clamped so that a hostile or broken server cannot ask for a hot loop.
        let retryHint: number | undefined;
        const backoff = async () => {
            await sleep(jitter(Math.max(delay, retryHint ?? 0)), this.controller.signal);
            delay = Math.min(delay * 2, maxDelay);
        };

        while (!this.closed) {
            let response: Response;

            try {
                response = await this.open(this._lastEventId, this.controller.signal);
            } catch (error) {
                if (this.closed) {
                    return;
                }
                if (!reconnect) {
                    throw error;
                }
                await backoff();
                continue;
            }

            if (response.status !== 200) {
                const body = await readBody(response);
                const error = new VoiceAgentApiError(response.status, redirectNote(response) ?? body);

                // 429 and 5xx are the server's problem for now; anything else 4xx will not get better by retrying.
                if (!reconnect || (response.status < 500 && response.status !== 429)) {
                    throw error;
                }
                await backoff();
                continue;
            }
            const type = response.headers.get('content-type') ?? '';

            if (!type.startsWith('text/event-stream')) {
                throw new VoiceAgentStreamError(`expected text/event-stream, got ${printable(type, 100) || 'no content type'}`);
            }
            if (!response.body) {
                throw new VoiceAgentStreamError('the event stream has no body');
            }
            const opened = performance.now();
            let failed = false;

            try {
                for await (const message of readMessages(response.body, this.controller.signal)) {
                    if (message.retry !== undefined) {
                        retryHint = Math.min(Math.max(message.retry, MIN_RETRY_MS), maxDelay);
                    }
                    if (message.id !== undefined && EVENT_ID_SHAPE.test(message.id)) {
                        this._lastEventId = message.id;
                    }
                    const event = toStreamEvent(message);

                    if (event) {
                        yield event;
                    }
                }
            } catch (error) {
                if (this.closed) {
                    return;
                }
                if (!reconnect) {
                    throw error;
                }
                failed = true;
            }
            if (this.closed || !reconnect) {
                return;
            }
            const lived = performance.now() - opened >= MIN_STREAM_MS;

            if (lived) {
                delay = initialDelay;
            }
            if (failed || !lived) {
                // A dropped connection, an oversized message, or a stream that ended as soon as it opened.
                await backoff();
            } else {
                // A stream the server closed at its max age is resumed at once, or after its own hint.
                await sleep(retryHint ?? 0, this.controller.signal);
            }
        }
    }
}

async function readBody(response: Response): Promise<unknown> {
    const { text } = await readText(response, MAX_ERROR_BODY_BYTES).catch(() => ({ text: '' }));

    try {
        return text ? parseJson(text) : undefined;
    } catch {
        return text;
    }
}

async function* readMessages(body: ReadableStream<Uint8Array>, signal: AbortSignal): AsyncGenerator<SseMessage> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    const cancel = () => {
        reader.cancel().catch(() => undefined);
    };

    signal.addEventListener('abort', cancel, { once: true });
    try {
        for (;;) {
            const { value, done } = await reader.read();

            if (done) {
                break;
            }
            for (const message of parser.feed(decoder.decode(value, { stream: true }))) {
                yield message;
            }
        }
    } finally {
        signal.removeEventListener('abort', cancel);
        reader.releaseLock();
    }
}

const isStringOrAbsent = (value: unknown) => value === undefined || isString(value);

function isSnapshotAgent(value: unknown): value is SnapshotAgent {
    return isRecord(value) && isString(value.agentId) && isString(value.state);
}

/**
 * The event a message carries, or `undefined` for one the client drops: an unknown event type (additive by
 * contract), or a known one whose data is not JSON or lacks its required fields. Dropping rather than throwing
 * keeps one bad message from costing a reconnect, and keeps a consumer's loop free of foreign exceptions.
 */
function toStreamEvent(message: SseMessage): StreamEvent | undefined {
    let data: unknown;

    try {
        data = parseJson(message.data);
    } catch {
        return undefined;
    }
    if (!isRecord(data)) {
        return undefined;
    }
    const record = data;
    const name = message.event ?? (isString(record.event) ? record.event : undefined);

    if (name === 'snapshot') {
        return {
            event: 'snapshot',
            conference: isString(record.conference) ? record.conference : undefined,
            agents: Array.isArray(record.agents) ? record.agents.filter(isSnapshotAgent) : [],
            id: message.id
        };
    }
    if (name && LIFECYCLE.has(name) && isString(record.agentId) && isString(record.conference) && isString(record.state)
            && isStringOrAbsent(record.sourceName) && isStringOrAbsent(record.timestamp)) {
        if (!isString(record.reason)) {
            delete record.reason;
        }

        return { ...record, event: name, id: message.id } as StreamEvent;
    }

    return undefined;
}
