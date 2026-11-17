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
