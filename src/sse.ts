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
 * {@link VoiceAgentStreamError}: a stream that never ends its lines or its messages must not grow the process
 * without bound. After that error the parser is not reusable.
 */
export class SseParser {
    // The pieces of a line whose end has not arrived; kept apart so that each chunk is scanned once.
    private pending: string[] = [];
    private pendingLength = 0;
    // The last chunk ended with \r: a \n that opens the next one belongs to the same line end.
    private skipLf = false;
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
        if (this.skipLf && text.length > 0) {
            this.skipLf = false;
            if (text.startsWith('\n')) {
                text = text.slice(1);
            }
        }
        const out: SseMessage[] = [];
        const lineEnd = /\r\n|\n|\r/g;
        let start = 0;

        for (;;) {
            lineEnd.lastIndex = start;
            const match = lineEnd.exec(text);

            if (!match) {
                break;
            }
            const piece = text.slice(start, match.index);
            const line = this.pending.length === 0 ? piece : this.takePending() + piece;

            start = match.index + match[0].length;
            this.skipLf = match[0] === '\r' && start === text.length;
            const message = this.line(line);

            if (message) {
                out.push(message);
            }
        }
        if (start < text.length) {
            this.pending.push(text.slice(start));
            this.pendingLength += text.length - start;
            if (this.pendingLength > this.maxMessageBytes) {
                throw new VoiceAgentStreamError(`event stream line exceeds ${this.maxMessageBytes} bytes`);
            }
        }

        return out;
    }

    private takePending(): string {
        const head = this.pending.join('');

        this.pending = [];
        this.pendingLength = 0;

        return head;
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
            // Plus the newline that joins it to the line before: a line without a value still counts.
            this.dataLength += value.length + (this.data.length > 0 ? 1 : 0);
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
