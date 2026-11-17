# @jitsi/voice-agent-client

Client for the Jitsi Meet / JaaS voice-agent provisioning API. It wraps the four REST routes and the lifecycle
event stream, so a backend can place an agent in a meeting and follow it with a few lines, and it doubles as a
reference for the wire contract in any other language.

Status: pre-release. The API it targets is frozen at v1 (`AGENT_REST_API.md` in
[jitsi/opus-transcriber-proxy](https://github.com/jitsi/opus-transcriber-proxy)). Requires Node 18 or newer (global
`fetch`); no runtime dependencies.

## Two halves: control and media

A voice agent has a control half and a media half, and this package covers only the first.

| Half | What happens | Who runs it |
| --- | --- | --- |
| Control | `invite` places the agent in a meeting, `dismiss` removes it, the event stream reports `connected`, `failed` and `ended`. | Your backend, through this package |
| Media | The meeting's audio is sent to the agent, per speaker, and the agent's speech is played into the meeting. | **Your WebSocket server** at `endpoint.url`, dialed by the Jitsi media gateway |

Nothing in this package carries audio. The `endpoint` you pass to `invite` names a WebSocket server you operate;
when the agent is placed, the media gateway connects **to it** and speaks the `jitsi-agent-media` protocol over
that socket. Your speech-to-text, LLM and text-to-speech, or a speech-to-speech model, run behind that server. See
[The agent endpoint](#the-agent-endpoint-the-media-half) below for what it must do.

## Usage

```ts
import { VoiceAgentClient } from '@jitsi/voice-agent-client';

const client = new VoiceAgentClient({
    baseUrl: 'https://<api>/v1/voice-agent',   // self-hosted: https://<prosody>/voice-agent
    token: process.env.VOICE_AGENT_TOKEN!       // or () => currentJwt()
});

const agent = await client.invite({
    conference: 'acme/room1',
    displayName: 'Support Bot',
    agentId: 'support',
    // The WebSocket server you run for the agent's audio; the media gateway dials it (see below).
    endpoint: { url: 'wss://bot.example.com/ws', authorization: 'Bearer <secret>' },
    fanIn: 'dominant'
}, { waitForRoom: true });                      // a room exists once its first participant joins

await client.waitFor('acme/room1', agent.agentId, 'active');

for await (const event of client.events({ conference: 'acme/room1' })) {
    // event.event: 'snapshot' | 'agent.connected' | 'agent.failed' | 'agent.ended'
}

await client.dismiss('acme/room1', agent.agentId);
```

## API

| Method | Does | Notes |
| --- | --- | --- |
| `invite(request, { waitForRoom?, signal? })` | `POST invite` | Idempotent on `agentId`. `waitForRoom` retries the 404 a room answers before its first participant joins (30 s at 1 s by default, or `{ timeoutMs, intervalMs }`). |
| `dismiss(conference, agentId)` | `POST dismiss` | Closes the agent's media leg; `agent.ended` follows. |
| `list(conference)` | `GET list` | Every agent in the meeting, keyed by `agentId`. |
| `get(conference, agentId)` | `GET get` | The agent's state, or `undefined` once it is gone. |
| `events({ conference?, lastEventId?, reconnect?, signal? })` | `GET events` | An `EventStream`: iterate with `for await`, stop with `close()`. |
| `waitFor(conference, agentId, state, { timeoutMs?, signal? })` | | Resolves when the agent reaches the state (from the snapshot or a later event); rejects with `VoiceAgentStateError` on another terminal state, `VoiceAgentTimeoutError` after `timeoutMs`. |

Errors: a non-2xx answer is a `VoiceAgentApiError` with `status` and the parsed `body`; a stream that is not
`text/event-stream` or carries malformed event data is a `VoiceAgentStreamError`.

## The agent endpoint (the media half)

`endpoint.url` must be a `wss://` WebSocket server on a public host that is up before you call `invite`. The media
gateway dials it once the agent is placed, sends `endpoint.authorization` verbatim as the `Authorization` header,
and keeps one connection open per meeting the agent is in. If the dial is refused or the host is unreachable, the
agent becomes `failed` with a reason such as `endpoint refused: HTTP 401`, and a re-invite with the same `agentId`
retries.

Over that socket the gateway speaks **`jitsi-agent-media` v1**: one WebSocket, one JSON text frame per message, audio
base64-encoded inside it:

| Direction | Message | Meaning |
| --- | --- | --- |
| gateway → you | `info` | First message: protocol version, audio format, `fanIn`, `agentId`, `conference`, `meetingId`, your `customParameters` |
| gateway → you | `start` | A participant's audio stream begins; carries its `tag` |
| gateway → you | `media` | 20 ms of one participant's audio: `tag`, counters, base64 payload |
| gateway → you | `speakers` | The current speaker order, dominant first, whenever it changes |
| you → gateway | `media` | The agent's speech, base64 audio, any chunking; the gateway paces it into the meeting |
| you → gateway | `clear` | Barge-in: drop the agent audio not yet played |
| you → gateway | `mark` | A playback checkpoint, echoed back once played |
| you → gateway | `end` | Hang up: the agent leaves and `agent.ended` fires |

Audio is PCM16 mono at 24 kHz by default, or Opus passthrough at 48 kHz when the invite sets
`mediaFormat.encoding: 'audio/opus'`. By default the gateway does not mix: every participant arrives as its own `tag`,
so your server demultiplexes by `tag`. If your model takes one stream, invite with `fanIn: 'dominant'` to hear one
speaker at a time, or with `fanIn: 'mix'` (PCM only) to hear everyone the bridge forwards summed into a single stream
tagged `mix`, which keeps people who talk over each other audible. A mix has costs: the audio no longer says who is
speaking (the `speakers` message still does, and names who is in the mix), it arrives a jitter buffer later (60 ms by
default), and every open microphone the bridge forwards is in the sum. The invite's `mix` object sets its options,
such as `maxSpeakers`. Both sides ignore unknown messages and fields, so the protocol grows without breaking a
conforming server.

The full contract, with every field, is `AGENT_PROTOCOL.md` in
[jitsi/opus-transcriber-proxy](https://github.com/jitsi/opus-transcriber-proxy). Any framework that can accept a
WebSocket and handle 20 ms audio frames can implement it, and the protocol is small enough to hand-roll. For
[Pipecat](https://github.com/pipecat-ai/pipecat), a `JitsiFrameSerializer` turns the endpoint into a
standard Pipecat WebSocket transport; the reference implementation is being contributed upstream, and is available
from the Jitsi team in the meantime.

## The event stream

`events()` opens `GET <base>/events?conference=<room>` with `Accept: text/event-stream`. The server first sends a
`snapshot` (every agent in scope with its state) and then each lifecycle event as it happens; without `conference`
the stream covers every room the credential can see. Each event carries an opaque `id`.

The client keeps one logical stream across reconnects: when the server closes a stream at its max age or the
connection drops, it reconnects (immediately after a server close, with doubling backoff from 1 s to 30 s after a
failure) and sends `Last-Event-ID`. The server then replays the events missed when it still has them, or sends a
fresh `snapshot`, so your loop sees either an exact continuation or the current state, never a gap. Unknown event
types are ignored, as the contract requires, and 4xx answers other than 429 are not retried.

## Sharded deployments

A Jitsi deployment with several shards routes each meeting's traffic through HAProxy, which picks the shard from
the URL: the tenant path prefix and a `room` query parameter. `routing: 'haproxy'` makes the client speak that
form against the deployment's signal-api host, so a call reaches the shard holding the room:

```ts
const client = new VoiceAgentClient({
    baseUrl: 'https://<signal-api-host>/voice-agent',
    token: () => currentJwt(),
    routing: 'haproxy'                          // → /<tenant>/voice-agent/invite?room=<room>
});
```

Every call then needs the conference, as the meeting URL spells it (never a JID), `events()` included: a stream
lives on one shard. The `room` parameter is spelled as jitsi-meet spells its own (decoded, lower-cased,
form-encoded), since HAProxy compares the bytes: `acme/Team Sync` and `acme/team%20sync` key the same shard. The
default, `direct`, is for a single prosody or a gateway that routes by itself.

## Security notes

- The base URL must be `https://`. Plain `http://` is accepted for loopback hosts, and elsewhere only with
  `allowHttp: true`, since every request carries the bearer token.
- The token is never logged and never appears in an error message; `headers` cannot override `Authorization`.
- Everything the server sends is treated as untrusted: a stream line or message over 1 MiB, or a REST body over
  4 MiB, ends the read instead of growing the process; a known event whose data is not JSON or lacks its required
  fields is dropped; only ids of a safe shape are sent back as `Last-Event-ID`; a server `retry:` hint is clamped
  so that it cannot ask for a hot loop, and failures back off with jitter.
- No runtime dependencies.

Report a vulnerability as described in [SECURITY.md](SECURITY.md).

## The wire contract, for other languages

Everything the package does is plain HTTPS:

- `Authorization: Bearer <token>` on every request.
- `POST <base>/invite`, `POST <base>/dismiss` with a JSON body; `GET <base>/list?conference=`, `GET <base>/get?conference=&agentId=`.
- `GET <base>/events[?conference=]` with `Accept: text/event-stream`; messages are `id:` / `event:` / `data:` lines
  with a blank line between messages and `:` comment lines as keepalive; send `Last-Event-ID` on reconnect.

The payload of a lifecycle event is identical to the webhook body, so a backend may use either transport or both.

## Development

```bash
npm install
npm run build
npm test
```

The tests run against an in-process mock of the provisioning API (`test/mock-server.ts`), which also serves as a
readable spec of what the client expects from the server.
