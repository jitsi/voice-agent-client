# Changelog

## Unreleased

- `fanIn: 'mix'` and its `mix` options: the gateway sums the participants into one stream (PCM only).
- `routing: 'haproxy'`: address a sharded deployment through its HAProxy, with the tenant as a path prefix and
  the room as a `room` query parameter spelled as jitsi-meet spells its own (decoded, lower-cased, form-encoded);
  `conferenceParts()` exported.
- Call the configured or global `fetch` as a plain function: with the client as `this`, Cloudflare Workers
  reject it ("Illegal invocation").
- Initial client: `invite`, `dismiss`, `list`, `get`, the lifecycle event stream with reconnection and
  `Last-Event-ID`, and `waitFor`.
- Hardening: bounded stream and body sizes, event validation, clamped `retry:` hints with jittered backoff,
  `https://` required off loopback.
- Redirects are not followed: a 3xx answer is a `VoiceAgentApiError`.
- `baseUrl` is refused with credentials, a query or a fragment, and `http://` only passes for `localhost` or a
  loopback address.
- 2xx answers are checked against the shape of their route (`VoiceAgentStreamError` otherwise); `list()` returns
  an object without a prototype and leaves out entries that are not agents.
- A stream that ends within a second of opening is redialed with backoff; reconnect and `waitForRoom` delays
  under 100 ms are raised to 100 ms.
- The headers the client sets replace a caller's of any spelling; a token that a header cannot carry is refused
  before the request.
- The keys `__proto__`, `constructor` and `prototype` are left out of every parsed answer and event. A lifecycle
  event whose `sourceName` or `timestamp` is not a string is dropped, and a `reason` that is not a string is removed.
- Stream limits count `data:` lines without a value; `conferenceParts()` refuses a name over 2048 characters.
