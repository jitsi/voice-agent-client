import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, test } from 'node:test';

import {
    VoiceAgentApiError,
    VoiceAgentClient,
    VoiceAgentStateError,
    VoiceAgentStreamError,
    VoiceAgentTimeoutError,
    type LifecycleEvent,
    type SnapshotEvent,
    type StreamEvent
} from '../src/index.js';
import { MockProvisioningServer } from './mock-server.js';

function snapshot(event: StreamEvent | undefined): SnapshotEvent & { id?: string } {
    if (!event || event.event !== 'snapshot') {
        assert.fail(`expected a snapshot, got ${event?.event}`);
    }

    return event;
}

function lifecycle(event: StreamEvent | undefined): LifecycleEvent & { id?: string } {
    if (!event || event.event === 'snapshot') {
        assert.fail(`expected a lifecycle event, got ${event?.event}`);
    }

    return event;
}

// Top-level hooks do not run for top-level tests in node:test; the suite scopes them.
describe('VoiceAgentClient', () => {
    const server = new MockProvisioningServer();
    let baseUrl = '';

    before(async () => {
        baseUrl = await server.start();
    });
    after(async () => {
        await server.stop();
    });
    beforeEach(() => server.reset());

    const client = () => new VoiceAgentClient({ baseUrl, token: server.token });
    const CONFERENCE = 'acme/room1';
    const inviteSupport = (c: VoiceAgentClient) => c.invite({
        conference: CONFERENCE,
        displayName: 'Support Bot',
        agentId: 'support',
        endpoint: { url: 'wss://bot.example.com/ws', authorization: 'Bearer s3cret' }
    });

    test('invite, get, list and dismiss round trip', async () => {
        const c = client();
        const invited = await inviteSupport(c);

        assert.equal(invited.agentId, 'agent-support');
        assert.equal(invited.sourceName, 'agent-support-a0');
        assert.deepEqual(await c.get(CONFERENCE, 'support'), { displayName: 'Support Bot', sourceName: 'agent-support-a0', state: 'provisioning' });
        assert.deepEqual(Object.keys(await c.list(CONFERENCE)), [ 'agent-support' ]);
        assert.deepEqual(await c.dismiss(CONFERENCE, 'agent-support'), { agentId: 'agent-support' });
        assert.equal(await c.get(CONFERENCE, 'support'), undefined);
    });

    test('calls fetch as a plain function, as runtimes that check the receiver require', async () => {
        const receivers: unknown[] = [];
        const c = new VoiceAgentClient({
            baseUrl,
            token: server.token,
            fetch(this: unknown, input, init) {
                receivers.push(this);

                return fetch(input, init);
            }
        });

        await c.list(CONFERENCE);
        assert.deepEqual(receivers, [ undefined ]);
    });

    test('a bad token surfaces as VoiceAgentApiError 401', async () => {
        const c = new VoiceAgentClient({ baseUrl, token: 'nope' });

        await assert.rejects(c.list(CONFERENCE), (error: unknown) => error instanceof VoiceAgentApiError && error.status === 401);
    });

    test('a token function is called per request and may already carry the scheme', async () => {
        let calls = 0;
        const c = new VoiceAgentClient({ baseUrl, token: () => {
            calls++;

            return `Bearer ${server.token}`;
        } });

        await inviteSupport(c);
        await c.list(CONFERENCE);
        assert.equal(calls, 2);
    });

    test('invite without waitForRoom fails fast on 404', async () => {
        server.roomExists = false;
        await assert.rejects(inviteSupport(client()), (error: unknown) => error instanceof VoiceAgentApiError && error.status === 404);
        assert.equal(server.inviteCalls, 1);
    });

    test('invite waits for the room when asked', async () => {
        server.roomExists = false;
        setTimeout(() => {
            server.roomExists = true;
        }, 120);
        const invited = await client().invite({
            conference: CONFERENCE,
            displayName: 'Support Bot',
            agentId: 'support',
            endpoint: { url: 'wss://bot.example.com/ws' }
        }, { waitForRoom: { timeoutMs: 2000, intervalMs: 40 } });

        assert.equal(invited.agentId, 'agent-support');
        assert.ok(server.inviteCalls >= 2, `expected retries, saw ${server.inviteCalls} calls`);
    });

    test('the event stream starts with a snapshot, then delivers lifecycle events with ids', async () => {
        const c = client();

        await inviteSupport(c);
        const stream = c.events({ conference: CONFERENCE });
        const iterator = stream[Symbol.asyncIterator]();
        const first = snapshot((await iterator.next()).value);

        assert.equal(first.agents[0]?.agentId, 'agent-support');

        const connectedId = server.emit('agent.connected', 'agent-support');
        const second = lifecycle((await iterator.next()).value);

        assert.equal(second.event, 'agent.connected');
        assert.equal(second.state, 'active');
        assert.equal(second.id, connectedId);
        assert.equal(stream.lastEventId, connectedId);

        stream.close();
        assert.equal((await iterator.next()).done, true);
    });

    test('reconnects after the server closes the stream and resumes with Last-Event-ID', async () => {
        const c = client();

        await inviteSupport(c);
        const stream = c.events({ conference: CONFERENCE, reconnect: { initialDelayMs: 10, maxDelayMs: 50 } });
        const iterator = stream[Symbol.asyncIterator]();

        await iterator.next();
        const connectedId = server.emit('agent.connected', 'agent-support');
        const connected = lifecycle((await iterator.next()).value);

        assert.equal(connected.id, connectedId);
        server.closeStreams();
        server.emit('agent.ended', 'agent-support', 'dismissed');
        const ended = lifecycle((await iterator.next()).value);

        assert.equal(ended.event, 'agent.ended', 'no second snapshot: the gap was replayed');
        assert.equal(ended.reason, 'dismissed');
        stream.close();
    });

    test('a stream without a conference follows every room', async () => {
        const c = client();

        await inviteSupport(c);
        await c.invite({ conference: 'acme/room2', displayName: 'Other', agentId: 'other', endpoint: { url: 'wss://bot.example.com/ws' } });
        const stream = c.events();
        const iterator = stream[Symbol.asyncIterator]();
        const first = snapshot((await iterator.next()).value);

        assert.equal(first.agents.length, 2);
        assert.equal(first.conference, undefined);
        stream.close();
    });

    test('a 401 on the stream is not retried', async () => {
        const c = new VoiceAgentClient({ baseUrl, token: 'nope' });

        await assert.rejects((async () => {
            for await (const event of c.events({ conference: CONFERENCE })) {
                assert.fail(`unexpected event ${event.event}`);
            }
        })(), (error: unknown) => error instanceof VoiceAgentApiError && error.status === 401);
    });

    test('waitFor resolves on the awaited state and rejects on another terminal state', async () => {
        const c = client();

        await inviteSupport(c);
        const waiting = c.waitFor(CONFERENCE, 'support', 'active', { timeoutMs: 2000 });

        setTimeout(() => server.emit('agent.connected', 'agent-support'), 50);
        assert.equal((await waiting).state, 'active');

        const failing = c.waitFor(CONFERENCE, 'support', 'ended', { timeoutMs: 2000 });

        setTimeout(() => server.emit('agent.failed', 'agent-support', 'endpoint refused: HTTP 401'), 50);
        await assert.rejects(failing, (error: unknown) =>
            error instanceof VoiceAgentStateError && error.state === 'failed' && error.reason === 'endpoint refused: HTTP 401');
    });

    test('waitFor answers from the snapshot when the agent is already there', async () => {
        const c = client();

        await inviteSupport(c);
        server.emit('agent.connected', 'agent-support');
        const reached = await c.waitFor(CONFERENCE, 'agent-support', [ 'active', 'ended' ], { timeoutMs: 2000 });

        assert.equal(reached.state, 'active');
    });

    test('rejects a plain-http base URL off loopback unless allowHttp is set', () => {
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'http://api.example.com/voice-agent', token: 't' }), TypeError);
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'ftp://api.example.com/voice-agent', token: 't' }), TypeError);
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'not a url', token: 't' }), TypeError);
        assert.ok(new VoiceAgentClient({ baseUrl: 'http://api.example.com/voice-agent', token: 't', allowHttp: true }));
        assert.ok(new VoiceAgentClient({ baseUrl: 'https://api.example.com/voice-agent', token: 't' }));
        assert.ok(new VoiceAgentClient({ baseUrl: 'http://localhost:5280/voice-agent', token: 't' }));
        assert.ok(new VoiceAgentClient({ baseUrl: 'http://127.0.0.1:5280/voice-agent', token: 't' }));
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'http://127.example.com/voice-agent', token: 't' }), TypeError,
            'a host name that only starts like a loopback address is not one');
    });

    test('rejects a base URL with credentials, a query or a fragment', () => {
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'https://user:hunter2@api.example.com/voice-agent', token: 't' }),
            (error: unknown) => error instanceof TypeError && !error.message.includes('hunter2'));
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'https://api.example.com/voice-agent?x=1', token: 't' }), /query or a fragment/);
        assert.throws(() => new VoiceAgentClient({ baseUrl: 'https://api.example.com/voice-agent#x', token: 't' }), /query or a fragment/);
    });

    test('a redirect is an error answer and is not followed', async () => {
        const other = new MockProvisioningServer();

        server.redirectTo = `${await other.start()}/invite`;
        try {
            await assert.rejects(inviteSupport(client()), (error: unknown) =>
                error instanceof VoiceAgentApiError && error.status === 307 && error.message.includes(`${server.redirectTo}, not followed`));
            await assert.rejects((async () => {
                for await (const event of client().events({ conference: CONFERENCE })) {
                    assert.fail(`unexpected event ${event.event}`);
                }
            })(), (error: unknown) => error instanceof VoiceAgentApiError && error.status === 307);
            assert.deepEqual(other.requests, [], 'nothing reached the redirect target');
        } finally {
            await other.stop();
        }
    });

    test('the headers the client sets replace a caller header of any spelling', async () => {
        const c = new VoiceAgentClient({
            baseUrl,
            token: server.token,
            headers: { Authorization: 'Basic other', ACCEPT: 'text/html', 'Last-Event-ID': 'x', 'X-Extra': '1' }
        });

        await c.list(CONFERENCE);
        assert.equal(server.lastHeaders.authorization, `Bearer ${server.token}`);
        assert.equal(server.lastHeaders.accept, 'application/json');
        assert.equal(server.lastHeaders['last-event-id'], undefined);
        assert.equal(server.lastHeaders['x-extra'], '1');
    });

    test('a token that a header cannot carry is refused without being named', async () => {
        const hidden = (error: unknown) => error instanceof TypeError && !error.message.includes('s3cret');
        const noToken = undefined as unknown as string;

        assert.throws(() => new VoiceAgentClient({ baseUrl, token: 's3cret\u0000tail' }), hidden);
        await assert.rejects(new VoiceAgentClient({ baseUrl, token: () => 's3cret\u0000tail' }).list(CONFERENCE), hidden);
        await assert.rejects(new VoiceAgentClient({ baseUrl, token: () => 's3cret\u00e9' }).list(CONFERENCE), hidden);
        await assert.rejects(new VoiceAgentClient({ baseUrl, token: () => noToken }).list(CONFERENCE), TypeError);
        assert.deepEqual(Object.keys(await new VoiceAgentClient({ baseUrl, token: `${server.token}\n` }).list(CONFERENCE)), [],
            'white space around a token is not part of it');
    });

    test('a 2xx answer of the wrong shape is refused', async () => {
        const answering = (body: unknown) =>
            new VoiceAgentClient({ baseUrl, token: 't', fetch: async () => new Response(JSON.stringify(body), { status: 200 }) });

        for (const body of [ null, [], 7, { agentId: 5 }, { agents: {} } ]) {
            const c = answering(body);

            await assert.rejects(inviteSupport(c), VoiceAgentStreamError);
            await assert.rejects(c.dismiss(CONFERENCE, 'support'), VoiceAgentStreamError);
            await assert.rejects(c.get(CONFERENCE, 'support'), VoiceAgentStreamError);
        }
        for (const body of [ null, [], 7, { agents: 'none' }, { agents: [ 1 ] } ]) {
            await assert.rejects(answering(body).list(CONFERENCE), VoiceAgentStreamError);
        }
    });

    test('list returns only agents, in an object without a prototype', async () => {
        const answer = '{"agents":{"agent-a":{"displayName":"A","sourceName":"agent-a-a0","state":"active","__proto__":{"x":1}},'
            + '"__proto__":{"displayName":"P","sourceName":"p","state":"active"},"agent-b":5,"agent-c":{"state":"active"}}}';
        const c = new VoiceAgentClient({ baseUrl, token: 't', fetch: async () => new Response(answer, { status: 200 }) });
        const agents = await c.list(CONFERENCE);

        assert.deepEqual(Object.keys(agents), [ 'agent-a' ]);
        assert.equal(Object.getPrototypeOf(agents), null);
        assert.equal(agents.constructor, undefined);
        assert.equal(Object.hasOwn(agents['agent-a'] ?? {}, '__proto__'), false);
        assert.equal(Object.keys(await new VoiceAgentClient({ baseUrl, token: 't', fetch: async () => new Response('{"agents":[]}') }).list(CONFERENCE)).length, 0);
    });

    test('server text in an error message is bounded and on one line', async () => {
        const text = `bad\n2026-01-01 forged line \u001b[31m${'x'.repeat(5000)}`;
        const c = new VoiceAgentClient({ baseUrl, token: 't', fetch: async () => new Response(JSON.stringify({ error: text }), { status: 400 }) });
        const oneLine = (message: string) => message.length < 300 && !/[\u0000-\u001f\u007f]/.test(message);

        await assert.rejects(c.list(CONFERENCE), (error: unknown) =>
            error instanceof VoiceAgentApiError && oneLine(error.message) && (error.body as { error: string }).error === text);
        assert.ok(oneLine(new VoiceAgentStateError('agent-a', 'failed', text).message));
    });

    test('lifecycle events lose unsafe keys and optional fields of the wrong type', async () => {
        const c = client();

        await inviteSupport(c);
        const stream = c.events({ conference: CONFERENCE });
        const iterator = stream[Symbol.asyncIterator]();

        await iterator.next();
        server.emitRaw('event: agent.ended\ndata: {"agentId":"agent-support","conference":"acme/room1","state":"ended","timestamp":5}\n\n');
        server.emitRaw('event: agent.failed\ndata: {"agentId":"agent-support","conference":"acme/room1","state":"failed",'
            + '"reason":{"x":1},"__proto__":{"polluted":true},"constructor":1,"later":{"kept":true,"__proto__":{"polluted":true}}}\n\n');
        const event = lifecycle((await iterator.next()).value) as LifecycleEvent & { later?: object };

        assert.equal(event.event, 'agent.failed', 'an event whose timestamp is not a string is dropped');
        assert.equal(event.reason, undefined);
        assert.deepEqual(event.later, { kept: true }, 'a field the client does not know is passed on, cleaned at every depth');
        assert.equal(Object.hasOwn(event.later ?? {}, '__proto__'), false);
        assert.equal(Object.hasOwn(event, '__proto__'), false);
        assert.equal(Object.hasOwn(event, 'constructor'), false);
        assert.equal(({} as { polluted?: boolean }).polluted, undefined);
        stream.close();
    });

    test('a stream that ends as soon as it opens is redialed with backoff', async () => {
        const c = client();

        server.closeOnOpen = true;
        const stream = c.events({ conference: CONFERENCE, reconnect: { initialDelayMs: 100, maxDelayMs: 200 } });

        setTimeout(() => stream.close(), 700);
        for await (const event of stream) {
            assert.equal(event.event, 'snapshot');
        }
        assert.ok(server.streamOpens >= 2 && server.streamOpens <= 8, `expected a few paced redials, saw ${server.streamOpens}`);
    });

    test('a delay under the floor is raised to it', async () => {
        server.roomExists = false;
        await assert.rejects(client().invite({ conference: CONFERENCE, displayName: 'A', endpoint: { url: 'wss://bot.example.com/ws' } },
            { waitForRoom: { timeoutMs: 350, intervalMs: 0 } }), VoiceAgentApiError);
        assert.ok(server.inviteCalls >= 2 && server.inviteCalls <= 5, `expected a call per 100 ms, saw ${server.inviteCalls}`);
    });

    test('a stream that lived is redialed at once when the server closes it', async () => {
        const c = client();

        await inviteSupport(c);
        const stream = c.events({ conference: CONFERENCE, reconnect: { initialDelayMs: 5000 } });
        const iterator = stream[Symbol.asyncIterator]();

        await iterator.next();
        await new Promise(resolve => setTimeout(resolve, 1100));
        server.closeStreams();
        const closedAt = Date.now();

        server.emit('agent.connected', 'agent-support');
        assert.equal(lifecycle((await iterator.next()).value).event, 'agent.connected');
        assert.ok(Date.now() - closedAt < 2000, 'no backoff after a stream that stayed open');
        stream.close();
    });

    test('drops malformed events and ids it cannot send back, without reconnecting', async () => {
        const c = client();

        await inviteSupport(c);
        const stream = c.events({ conference: CONFERENCE });
        const iterator = stream[Symbol.asyncIterator]();

        await iterator.next();
        const snapshotId = stream.lastEventId;

        server.emitRaw('event: agent.connected\ndata: {not json\n\n');
        server.emitRaw('event: agent.ended\ndata: {"agentId":5}\n\n');
        server.emitRaw('event: agent.ended\ndata: [1,2]\n\n');
        server.emitRaw('id: not a valid id\nevent: agent.failed\ndata: {"agentId":"agent-support","conference":"acme/room1","state":"failed"}\n\n');
        server.emitRaw('event: snapshot\ndata: {"agents":[null, 1, {"agentId":"agent-x","state":"active"}]}\n\n');
        const connectedId = server.emit('agent.connected', 'agent-support');
        const first = lifecycle((await iterator.next()).value);

        assert.equal(first.event, 'agent.failed', 'a valid event with an unusable id is still delivered');
        assert.equal(stream.lastEventId, snapshotId, 'an id outside the allowed shape is not remembered');
        const second = snapshot((await iterator.next()).value);

        assert.deepEqual(second.agents, [ { agentId: 'agent-x', state: 'active' } ], 'snapshot entries are filtered');
        const third = lifecycle((await iterator.next()).value);

        assert.equal(third.id, connectedId);
        assert.equal(server.streamOpens, 1, 'no reconnect was needed');
        stream.close();
    });

    test('haproxy routing puts the tenant in the path and the room in the query on every call', async () => {
        const c = new VoiceAgentClient({ baseUrl, token: server.token, routing: 'haproxy' });

        await inviteSupport(c);
        await c.get('Acme/Room1', 'support');
        await c.list(CONFERENCE);
        const stream = c.events({ conference: CONFERENCE });

        snapshot((await stream[Symbol.asyncIterator]().next()).value);
        stream.close();
        await c.dismiss(CONFERENCE, 'support');
        assert.deepEqual(server.requests, [
            '/acme/voice-agent/invite?room=room1',
            '/acme/voice-agent/get?conference=Acme%2FRoom1&agentId=support&room=room1',
            '/acme/voice-agent/list?conference=acme%2Froom1&room=room1',
            '/acme/voice-agent/events?conference=acme%2Froom1&room=room1',
            '/acme/voice-agent/dismiss?room=room1'
        ].map((path, i) => `${i === 0 || i === 4 ? 'POST' : 'GET'} ${path}`));
    });

    test('haproxy routing without a tenant has no prefix, and events() needs a conference', async () => {
        const c = new VoiceAgentClient({ baseUrl, token: server.token, routing: 'haproxy' });

        await c.list('standup');
        assert.equal(server.requests.at(-1), 'GET /voice-agent/list?conference=standup&room=standup');
        assert.throws(() => c.events(), /needs a conference/);
        assert.throws(() => new VoiceAgentClient({ baseUrl, token: server.token, routing: 'nginx' as 'direct' }), /routing must be/);
    });

    test('the room parameter is spelled as jitsi-meet spells its own: decoded, lower-cased, form-encoded', async () => {
        const c = new VoiceAgentClient({ baseUrl, token: server.token, routing: 'haproxy' });

        await c.list('acme/Team Sync');
        assert.match(server.requests.at(-1) ?? '', /&room=team\+sync$/, 'lower-cased, a space is +');
        await c.list('acme/team%20sync');
        assert.match(server.requests.at(-1) ?? '', /&room=team\+sync$/, 'an encoded name is decoded first, so both spellings key the same shard');
        await c.list('acme/room(1)');
        assert.match(server.requests.at(-1) ?? '', /&room=room%281%29$/, 'parentheses are escaped, as a form field');
        await c.list('acme/caf\u00e9');
        assert.match(server.requests.at(-1) ?? '', /&room=caf%C3%A9$/);
        await c.list('acme/100%');
        assert.match(server.requests.at(-1) ?? '', /&room=100%25$/, 'a malformed escape is kept as typed');
        await assert.rejects(c.list('[acme]room1@conference.example.com'), /not a JID/);
        await c.list('a b?x#y/room1');
        assert.match(server.requests.at(-1) ?? '', /^GET \/a%20b%3Fx%23y\/voice-agent\/list\?/, 'the tenant stays one path segment');
        await assert.rejects(c.list('a/b/c'), /does not name one room/);
        await assert.rejects(c.list('../room1'), /does not name a tenant/);
        await assert.rejects(c.list(`acme/${'r'.repeat(3000)}`), /longer than/);
        await c.list('//acme/room1//');
        assert.match(server.requests.at(-1) ?? '', /^GET \/acme\/voice-agent\/list\?.*&room=room1$/, 'slashes around the name are not part of it');
    });

    test('direct routing (the default) leaves the URL alone', async () => {
        await client().list(CONFERENCE);
        assert.equal(server.requests.at(-1), 'GET /voice-agent/list?conference=acme%2Froom1');
    });

    test('waitFor times out', async () => {
        const c = client();

        await inviteSupport(c);
        await assert.rejects(c.waitFor(CONFERENCE, 'support', 'active', { timeoutMs: 100 }),
            (error: unknown) => error instanceof VoiceAgentTimeoutError);
    });
});
