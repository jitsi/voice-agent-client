export type AgentState = 'provisioning' | 'connecting' | 'active' | 'failed' | 'ended';
/**
 * Whose audio the agent hears: every participant as its own stream, only the dominant speaker, or everyone the
 * bridge forwards summed into one stream (`mix`: PCM only, so not with `audio/opus`).
 */
export type FanIn = 'per-source' | 'dominant' | 'mix';
export type MediaEncoding = 'audio/l16' | 'audio/opus';

/**
 * The media half of an agent: a WebSocket server **you run**, which the Jitsi media gateway dials once the agent
 * is placed and speaks the `jitsi-agent-media` protocol over. This package never connects to it; it only tells
 * the provisioning API where it is.
 */
export interface AgentEndpoint {
    /** `wss://` on a public host, up before `invite` is called; a refused or unreachable dial fails the agent. */
    url: string;
    /** Sent verbatim as the `Authorization` header on the gateway's dial; verify it on your server. */
    authorization?: string;
}

export interface InviteRequest {
    /** The room as the meeting URL spells it: `<room>`, or `<tenant>/<room>`. */
    conference: string;
    displayName: string;
    /** Stable id, normalised to `agent-<id>` by the server; also the idempotency key. */
    agentId?: string;
    endpoint: AgentEndpoint;
    customParameters?: Record<string, string>;
    callbackUrl?: string;
    mediaFormat?: { encoding: MediaEncoding };
    /** Unset: `mix`, or `per-source` with `audio/opus`; a deployment from before that default gives `per-source`. */
    fanIn?: FanIn;
    /** Options of the `mix` fan-in (the media contract's **Mix options**); the gateway checks them when it dials. */
    mix?: Record<string, number | boolean | string>;
}

export interface InviteResponse {
    agentId: string;
    sourceName: string;
}

export interface AgentStatus {
    displayName: string;
    sourceName: string;
    state: AgentState;
}

export type LifecycleEventType = 'agent.connected' | 'agent.failed' | 'agent.ended';

/** The lifecycle payload, identical on webhooks and on the event stream. */
export interface LifecycleEvent {
    event: LifecycleEventType;
    agentId: string;
    conference: string;
    sourceName: string;
    state: AgentState;
    timestamp: string;
    reason?: string;
}

export interface SnapshotAgent extends AgentStatus {
    agentId: string;
    conference: string;
}

/** The first event on a new stream: every agent in scope with its current state. */
export interface SnapshotEvent {
    event: 'snapshot';
    conference?: string;
    agents: SnapshotAgent[];
}

export type StreamEvent = (LifecycleEvent | SnapshotEvent) & { id?: string };
