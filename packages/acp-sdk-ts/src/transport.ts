/**
 * Transport abstraction (spec §9-11): transports move bytes, nothing else.
 * Op routing, validation and streaming live in the server core.
 */
import type { AcpEvent, AcpRequest, AcpServerMessage } from "./types.js";

/** Server-side handle on one client connection. */
export interface Connection {
  /** Remote metadata (ip, headers, ...), transport-specific. */
  meta: Record<string, unknown>;
  /** Push a response/chunk/error/event frame to the client. */
  send(msg: AcpServerMessage): void;
  close(): Promise<void>;
  /** Aborted when the underlying connection drops; optional per transport. */
  signal?: AbortSignal;
  /** Number of queued outgoing event frames (for bounded delivery, spec v0.2 §6.2). */
  eventBacklog?(): number;
}

/**
 * Core dispatch: takes an already-JSON-parsed request object, routes it,
 * and sends every reply frame through the connection.
 * Resolves when the handler has finished (all frames sent).
 */
export type ServerDispatch = (raw: unknown, conn: Connection) => Promise<void>;

/** Connection lifecycle hooks for stateful transports (spec v0.2 §4.4).
 *  HTTP never calls these — every POST is an ephemeral connection. */
export interface TransportLifecycle {
  onConnection?(conn: Connection): void;
  onDisconnect?(conn: Connection): void;
}

/** Server-side transport; starts listening and feeds envelopes to the dispatch. */
export interface ServerTransport {
  start(dispatch: ServerDispatch, lifecycle?: TransportLifecycle): Promise<void>;
  stop(): Promise<void>;
}

export interface ClientRequestOptions {
  signal?: AbortSignal;
}

/**
 * Client-side transport. `request` resolves with the single reply envelope;
 * `requestStream` iterates chunk frames until the end frame, throwing AcpError
 * when an error frame terminates the stream.
 * `eventsSupported`/`onEvent` back $subscribe/$event (spec v0.2 §4.4):
 * connectionless transports (HTTP) report false.
 */
export interface ClientTransport {
  connect(): Promise<void>;
  request(req: AcpRequest, opts?: ClientRequestOptions): Promise<AcpServerMessage>;
  requestStream(req: AcpRequest, opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage>;
  close(): Promise<void>;
  eventsSupported?(): boolean;
  /** Registers an event listener; returns an unregister function. */
  onEvent?(handler: (event: AcpEvent) => void): () => void;
  /** Incoming server-initiated `$ping` is auto-answered by stateful transports. */
}
