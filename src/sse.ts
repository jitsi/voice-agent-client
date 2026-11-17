import { VoiceAgentStreamError } from './errors.js';

export interface SseMessage {
    id?: string;
    event?: string;
    data: string;
    retry?: number;
}

/** Longest line, and largest pending message, a parser accepts by default before the stream counts as broken. */
export const DEFAULT_MAX_MESSAGE_BYTES = 1_048_576;

/**
 * Incremental parser for `text/event-stream`. Feed it decoded text in any chunking; it returns the messages
 * completed by that chunk. Comments and unknown fields are ignored, `id` persists across messages, and `retry`
 * rides on the message that carried it. A line or a pending message longer than `maxMessageBytes` throws
 * {@link VoiceAgentStreamError}: a stream that never ends its lines must not grow the process without bound.
 */
export class SseParser {
    private buffer = '';
    private data: string[] = [];
    private dataLength = 0;
    private eventName?: string;
    private id?: string;
    private retry?: number;
    private first = true;

    constructor(private readonly maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES) {}

    feed(text: string): SseMessage[] {
        if (this.first) {
            this.first = false;
            if (text.startsWith('\uFEFF')) {
                text = text.slice(1);
            }
        }
        this.buffer += text;
        const out: SseMessage[] = [];

        for (;;) {
            const idx = this.buffer.search(/\r\n|\n|\r/);

            if (idx === -1) {
                break;
            }
            // A \r at the very end may be the first half of \r\n: wait for the next chunk.
            if (this.buffer[idx] === '\r' && idx === this.buffer.length - 1) {
                break;
            }
            const line = this.buffer.slice(0, idx);
            const separator = this.buffer.startsWith('\r\n', idx) ? 2 : 1;

            this.buffer = this.buffer.slice(idx + separator);
            const message = this.line(line);

            if (message) {
                out.push(message);
            }
        }
        if (this.buffer.length > this.maxMessageBytes) {
            throw new VoiceAgentStreamError(`event stream line exceeds ${this.maxMessageBytes} bytes`);
        }

        return out;
    }

    private line(line: string): SseMessage | undefined {
        if (line === '') {
            return this.dispatch();
        }
        if (line.startsWith(':')) {
            return undefined;
        }
        const colon = line.indexOf(':');
        const field = colon === -1 ? line : line.slice(0, colon);
        let value = colon === -1 ? '' : line.slice(colon + 1);

        if (value.startsWith(' ')) {
            value = value.slice(1);
        }
        switch (field) {
        case 'event':
            this.eventName = value;
            break;
        case 'data':
            this.dataLength += value.length;
            if (this.dataLength > this.maxMessageBytes) {
                throw new VoiceAgentStreamError(`event stream message exceeds ${this.maxMessageBytes} bytes`);
            }
            this.data.push(value);
            break;
        case 'id':
            if (!value.includes('\0')) {
                this.id = value;
            }
            break;
        case 'retry':
            if (/^\d+$/.test(value)) {
                this.retry = Number(value);
            }
            break;
        default:
            break;
        }

        return undefined;
    }

    private dispatch(): SseMessage | undefined {
        if (this.data.length === 0) {
            this.eventName = undefined;

            return undefined;
        }
        const message: SseMessage = { data: this.data.join('\n') };

        if (this.eventName !== undefined) {
            message.event = this.eventName;
        }
        if (this.id !== undefined) {
            message.id = this.id;
        }
        if (this.retry !== undefined) {
            message.retry = this.retry;
        }
        this.data = [];
        this.dataLength = 0;
        this.eventName = undefined;
        this.retry = undefined;

        return message;
    }
}
