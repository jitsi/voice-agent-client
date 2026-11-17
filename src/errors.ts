import type { AgentState } from './types.js';
import { printable } from './util.js';

function describe(body: unknown): string {
    if (body && typeof body === 'object' && 'error' in body && typeof (body as { error: unknown }).error === 'string') {
        return printable((body as { error: string }).error);
    }
    if (typeof body === 'string' && body) {
        return printable(body);
    }
    return 'no error body';
}

/** A non-2xx answer from the provisioning API. `body` is the parsed JSON when there was any, else the text. */
export class VoiceAgentApiError extends Error {
    readonly status: number;
    readonly body: unknown;

    constructor(status: number, body: unknown) {
        super(`voice-agent API answered ${status}: ${describe(body)}`);
        this.name = 'VoiceAgentApiError';
        this.status = status;
        this.body = body;
    }
}

/**
 * A response the client refuses to read: not an event stream, no body, a body or message past its size limit, or a
 * 2xx answer that does not have the shape of its route.
 */
export class VoiceAgentStreamError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'VoiceAgentStreamError';
    }
}

export class VoiceAgentTimeoutError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'VoiceAgentTimeoutError';
    }
}

/** `waitFor` saw the agent reach a terminal state other than the one awaited. */
export class VoiceAgentStateError extends Error {
    readonly state: AgentState;
    readonly reason?: string;

    constructor(agentId: string, state: AgentState, reason?: string) {
        super(`agent ${agentId} is ${printable(state, 32)}${reason ? ` (${printable(reason)})` : ''}`);
        this.name = 'VoiceAgentStateError';
        this.state = state;
        this.reason = reason;
    }
}
