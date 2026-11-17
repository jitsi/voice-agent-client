import http from 'node:http';
import type { AddressInfo } from 'node:net';

type LifecycleType = 'agent.connected' | 'agent.failed' | 'agent.ended';
interface Agent { conference: string; displayName: string; sourceName: string; state: string }
interface Stream { res: http.ServerResponse; conference?: string }

function json(res: http.ServerResponse, status: number, body: unknown): void {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
}

function text(req: http.IncomingMessage): Promise<string> {
    return new Promise(resolve => {
        let out = '';

        req.setEncoding('utf8');
        req.on('data', chunk => {
            out += chunk;
        });
        req.on('end', () => resolve(out));
    });
}

function normalize(agentId: string): string {
    return agentId.startsWith('agent-') ? agentId : `agent-${agentId}`;
}

/** The provisioning API as the client sees it: the four routes plus the event stream with replay. */
export class MockProvisioningServer {
    /** Every request as `METHOD /path?query`, newest last. */
    readonly requests: string[] = [];
    readonly token = 'secret';
    readonly epoch = 'abcd1234';
    readonly agents = new Map<string, Agent>();
    roomExists = true;
    inviteCalls = 0;
    streamOpens = 0;
    /** When set, every request is answered with a 307 to this URL. */
    redirectTo?: string;
    /** Ends each stream as soon as it has opened: what a proxy that cannot hold a stream does. */
    closeOnOpen = false;
    /** The headers of the newest request. */
    lastHeaders: http.IncomingHttpHeaders = {};
    private server?: http.Server;
    private readonly streams = new Set<Stream>();
    private readonly buffer: { id: string; conference: string; frame: string }[] = [];
    private seq = 0;

    async start(): Promise<string> {
        const server = http.createServer((req, res) => {
            this.handle(req, res).catch(error => json(res, 500, { error: String(error) }));
        });

        this.server = server;
        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const { port } = server.address() as AddressInfo;

        return `http://127.0.0.1:${port}/voice-agent`;
    }

    async stop(): Promise<void> {
        this.closeStreams();
        await new Promise<void>((resolve, reject) => this.server?.close(error => (error ? reject(error) : resolve())));
    }

    /** What the server does at a stream's max age. */
    closeStreams(): void {
        for (const stream of this.streams) {
            stream.res.end();
        }
        this.streams.clear();
    }

    reset(): void {
        this.closeStreams();
        this.agents.clear();
        this.roomExists = true;
        this.inviteCalls = 0;
        this.requests.length = 0;
        this.streamOpens = 0;
        this.redirectTo = undefined;
        this.closeOnOpen = false;
        this.lastHeaders = {};
    }

    /** Writes a frame as given to every open stream: what a broken or hostile server might send. */
    emitRaw(frame: string): void {
        for (const stream of this.streams) {
            stream.res.write(frame);
        }
    }

    emit(event: LifecycleType, agentId: string, reason?: string): string {
        const agent = this.agents.get(agentId);

        if (!agent) {
            throw new Error(`no agent ${agentId}`);
        }
        agent.state = event === 'agent.connected' ? 'active' : event === 'agent.failed' ? 'failed' : 'ended';
        const payload = {
            event,
            agentId,
            conference: agent.conference,
            sourceName: agent.sourceName,
            state: agent.state,
            timestamp: new Date().toISOString(),
            ...reason ? { reason } : {}
        };

        if (event !== 'agent.connected') {
            this.agents.delete(agentId);
        }
        const id = `${this.epoch}-${++this.seq}`;
        const frame = `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;

        this.buffer.push({ id, conference: agent.conference, frame });
        for (const stream of this.streams) {
            if (!stream.conference || stream.conference === agent.conference) {
                stream.res.write(frame);
            }
        }

        return id;
    }

    private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
        const url = new URL(req.url ?? '/', 'http://localhost');

        this.requests.push(`${req.method} ${url.pathname}${url.search}`);
        this.lastHeaders = req.headers;
        if (this.redirectTo) {
            req.resume();
            res.writeHead(307, { location: this.redirectTo });
            res.end();

            return;
        }
        // The tenant-prefixed form a HAProxy-routed caller sends; nginx strips it before prosody sees the path.
        url.pathname = url.pathname.replace(/^\/[^/]+(?=\/voice-agent\/)/, '');

        if (req.headers.authorization !== `Bearer ${this.token}`) {
            return json(res, 401, {});
        }
        const body = req.method === 'POST' ? JSON.parse((await text(req)) || '{}') as Record<string, string> : {};

        switch (`${req.method} ${url.pathname}`) {
        case 'POST /voice-agent/invite': {
            this.inviteCalls++;
            if (!this.roomExists) {
                return json(res, 404, { error: 'Room not found' });
            }
            const agentId = normalize(body.agentId ?? 'generated');

            this.agents.set(agentId, {
                conference: body.conference,
                displayName: body.displayName,
                sourceName: `${agentId}-a0`,
                state: 'provisioning'
            });

            return json(res, 200, { agentId, sourceName: `${agentId}-a0` });
        }
        case 'POST /voice-agent/dismiss': {
            const agentId = normalize(body.agentId);

            if (!this.agents.has(agentId)) {
                return json(res, 404, { error: 'Agent not found' });
            }
            this.emit('agent.ended', agentId, 'dismissed');

            return json(res, 200, { agentId });
        }
        case 'GET /voice-agent/list': {
            const conference = url.searchParams.get('conference');
            const agents: Record<string, unknown> = {};

            for (const [ agentId, agent ] of this.agents) {
                if (agent.conference === conference) {
                    agents[agentId] = { displayName: agent.displayName, sourceName: agent.sourceName, state: agent.state };
                }
            }

            return json(res, 200, { agents });
        }
        case 'GET /voice-agent/get': {
            const agent = this.agents.get(normalize(url.searchParams.get('agentId') ?? ''));

            if (!agent) {
                return json(res, 404, { error: 'Agent not found' });
            }

            return json(res, 200, { displayName: agent.displayName, sourceName: agent.sourceName, state: agent.state });
        }
        case 'GET /voice-agent/events':
            return this.stream(req, res, url.searchParams.get('conference') ?? undefined);
        default:
            return json(res, 404, { error: 'no route' });
        }
    }

    private stream(req: http.IncomingMessage, res: http.ServerResponse, conference?: string): void {
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        this.streamOpens++;
        const stream: Stream = { res, conference };

        this.streams.add(stream);
        req.on('close', () => this.streams.delete(stream));

        const last = req.headers['last-event-id'];
        const index = typeof last === 'string' ? this.buffer.findIndex(entry => entry.id === last) : -1;

        if (index !== -1) {
            for (const entry of this.buffer.slice(index + 1)) {
                if (!conference || entry.conference === conference) {
                    res.write(entry.frame);
                }
            }
        } else {
            const agents = [ ...this.agents ]
                .filter(([ , agent ]) => !conference || agent.conference === conference)
                .map(([ agentId, agent ]) => ({ agentId, conference: agent.conference, displayName: agent.displayName, sourceName: agent.sourceName, state: agent.state }));

            res.write(`id: ${this.epoch}-${this.seq}\nevent: snapshot\ndata: ${JSON.stringify({ conference, agents })}\n\n`);
        }
        res.write(': keepalive\n\n');
        if (this.closeOnOpen) {
            res.end();
        }
    }
}
