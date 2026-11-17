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

/**
 * `<tenant>/<room>` → both parts, `<room>` → the room alone: the tenant lower-cased as the meeting URL spells it,
 * the room as jitsi-meet normalises it (see {@link normalizeRoom}), which is what HAProxy keys its stick table on.
 */
export function conferenceParts(conference: string): { tenant?: string; room: string } {
    const trimmed = conference.trim().replace(/^\/+|\/+$/g, '');
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
