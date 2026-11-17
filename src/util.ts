/** Resolves after `ms`, or as soon as `signal` aborts. Never rejects. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise(resolve => {
        if (ms <= 0 || signal?.aborted) {
            resolve();

            return;
        }
        const timer = setTimeout(done, ms);

        function done() {
            clearTimeout(timer);
            signal?.removeEventListener('abort', done);
            resolve();
        }
        signal?.addEventListener('abort', done, { once: true });
    });
}

/**
 * Reads a response body as text, up to `maxBytes`. A longer body is cut there and reported as `truncated`, and
 * the rest is cancelled rather than downloaded, so a misbehaving server cannot grow the process without bound.
 */
export async function readText(response: Response, maxBytes: number): Promise<{ text: string; truncated: boolean }> {
    if (!response.body) {
        return { text: '', truncated: false };
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = '';
    let bytes = 0;

    try {
        for (;;) {
            const { value, done } = await reader.read();

            if (done) {
                break;
            }
            bytes += value.byteLength;
            if (bytes > maxBytes) {
                const keep = value.byteLength - (bytes - maxBytes);

                text += decoder.decode(value.subarray(0, Math.max(0, keep)));
                await reader.cancel().catch(() => undefined);

                return { text, truncated: true };
            }
            text += decoder.decode(value, { stream: true });
        }
        text += decoder.decode();

        return { text, truncated: false };
    } finally {
        reader.releaseLock();
    }
}

/** Visible ASCII, no spaces, at most 256 characters: the only shape of event id the client will send back. */
export const EVENT_ID_SHAPE = /^[\x21-\x7E]{1,256}$/;

/** Longest conference name the client routes; a tenant and a room are far shorter. */
const MAX_CONFERENCE_LENGTH = 2048;

/** Keys that would reach an object's prototype if a consumer merged the parsed data. */
const UNSAFE_KEYS: ReadonlySet<string> = new Set([ '__proto__', 'constructor', 'prototype' ]);

/** `value` without its leading and trailing slashes, in one pass whatever the input. */
export function trimSlashes(value: string, leading = true): string {
    let start = 0;
    let end = value.length;

    while (leading && start < end && value[start] === '/') {
        start++;
    }
    while (end > start && value[end - 1] === '/') {
        end--;
    }

    return value.slice(start, end);
}

/** Text from the server as an error message may carry it: bounded, one line, no control or direction characters. */
export function printable(text: string, max = 200): string {
    return text.slice(0, max)
        .replace(/[\ud800-\udbff]$/, '')
        .replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g, ' ');
}

export const isString = (value: unknown): value is string => typeof value === 'string';

export const isRecord = (value: unknown): value is Record<string, unknown> =>
    typeof value === 'object' && value !== null && !Array.isArray(value);

/** `JSON.parse` that leaves out the keys in {@link UNSAFE_KEYS}, at every depth. */
export function parseJson(text: string): unknown {
    return JSON.parse(text, (key, value) => (UNSAFE_KEYS.has(key) ? undefined : value));
}

/** Where a 3xx answer points, for the error that reports it; the client does not go there. */
export function redirectNote(response: Response): string | undefined {
    return response.status >= 300 && response.status < 400
        ? `redirect to ${response.headers.get('location') ?? 'an unnamed location'}, not followed`
        : undefined;
}

/** The shortest delay a caller's option or a server's `retry:` hint can ask for; below it is a hot loop. */
export const MIN_RETRY_MS = 100;

/** A delay option as the client uses it: the default when it is not a number, never under {@link MIN_RETRY_MS}. */
export const delayOption = (ms: number | undefined, fallback: number) =>
    Math.max(typeof ms === 'number' && Number.isFinite(ms) ? ms : fallback, MIN_RETRY_MS);

/**
 * `<tenant>/<room>` → both parts, `<room>` → the room alone: the tenant lower-cased as the meeting URL spells it,
 * the room as jitsi-meet normalises it (see {@link normalizeRoom}), which is what HAProxy keys its stick table on.
 */
export function conferenceParts(conference: string): { tenant?: string; room: string } {
    if (conference.length > MAX_CONFERENCE_LENGTH) {
        throw new TypeError(`conference is longer than ${MAX_CONFERENCE_LENGTH} characters`);
    }
    const trimmed = trimSlashes(conference.trim());
    const slash = trimmed.indexOf('/');

    if (!trimmed) {
        throw new TypeError('conference is empty');
    }
    const parts = slash === -1
        ? { room: trimmed }
        : { tenant: trimmed.slice(0, slash).toLowerCase(), room: trimmed.slice(slash + 1) };

    // One tenant and one room: anything else would change the request, not pick a shard.
    if (!parts.room || parts.room.includes('/')) {
        throw new TypeError(`conference ${JSON.stringify(conference)} does not name one room`);
    }
    if (parts.tenant !== undefined && (!parts.tenant || parts.tenant === '.' || parts.tenant === '..')) {
        throw new TypeError(`conference ${JSON.stringify(conference)} does not name a tenant`);
    }
    parts.room = normalizeRoom(parts.room);
    if (!parts.room) {
        throw new TypeError(`conference ${JSON.stringify(conference)} does not name one room`);
    }

    return parts;
}

/**
 * The room as jitsi-meet normalises it before putting it on the meeting's own requests (`getNormalizedRoomName`):
 * decoded when it decodes (a malformed escape stays as typed), NFKC, lower case. The meeting's shard is keyed on
 * that spelling, form-encoded.
 */
export function normalizeRoom(room: string): string {
    let decoded = room;

    try {
        decoded = decodeURIComponent(room);
    } catch {
        // not an encoded name, only one that looks like it
    }

    return decoded.normalize('NFKC').toLowerCase();
}

/** The server stores every agent under the reserved `agent-` prefix; accept either spelling. */
export function normalizeAgentId(agentId: string): string {
    return agentId.startsWith('agent-') ? agentId : `agent-${agentId}`;
}
