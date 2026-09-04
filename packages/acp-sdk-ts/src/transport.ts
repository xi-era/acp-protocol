/**
 * Transport abstraction (spec §9-11): transports move bytes, nothing else.
 * Op routing, validation and streaming live in the server core.
 */
import type { AcpRequest, AcpServerMessage } from "./types.js";

/** Server-side handle on one client connection. */
export interface Connection {
  /** Remote metadata (ip, headers, ...), transport-specific. */
  meta: Record<string, unknown>;
  /** Push a response/chunk/error frame to the client. */
  send(msg: AcpServerMessage): void;
  close(): Promise<void>;
  /** Aborted when the underlying connection drops; optional per transport. */
  signal?: AbortSignal;
}

/**
 * Core dispatch: takes an already-JSON-parsed request object, routes it,
 * and sends every reply frame through the connection.
 * Resolves when the handler has finished (all frames sent).
 */
export type ServerDispatch = (raw: unknown, conn: Connection) => Promise<void>;

/** Server-side transport; starts listening and feeds envelopes to the dispatch. */
export interface ServerTransport {
  start(dispatch: ServerDispatch): Promise<void>;
  stop(): Promise<void>;
}

export interface ClientRequestOptions {
  signal?: AbortSignal;
}

/**
 * Client-side transport. `request` resolves with the single reply envelope;
 * `requestStream` iterates chunk frames until the end frame, throwing AcpError
 * when an error frame terminates the stream.
 */
export interface ClientTransport {
  connect(): Promise<void>;
  request(req: AcpRequest, opts?: ClientRequestOptions): Promise<AcpServerMessage>;
  requestStream(req: AcpRequest, opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage>;
  close(): Promise<void>;
}
