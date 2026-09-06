/**
 * AcpServer core (spec v0.2 §3-6, §12): transport-agnostic op routing, schema
 * validation, streaming, reserved ops ($ping/$subscribe), event fan-out.
 * Transports feed parsed envelopes to `handle`.
 */
import { Ajv, type ErrorObject, type ValidateFunction } from "ajv";
import { createServer as createHttpServer, type Server as HttpServer } from "node:http";
import { HttpServerTransport } from "./http-transport.js";
import { WsServerTransport } from "./ws-transport.js";
import { StdioServerTransport } from "./stdio-transport.js";
import { AcpError, AcpErrorCode } from "./errors.js";
import {
  PROTOCOL_VERSION,
  errorEnvelope,
  isVersionSupported,
  validateEnvelope,
} from "./codec.js";
import { Registry, type AnyComponentDef } from "./registry.js";
import type { AcpEvent, AcpRequest, AcpServerInfo, AcpServerMessage, DiscoverResult } from "./types.js";
import type { Connection, ServerDispatch, TransportLifecycle } from "./transport.js";
import type { OutgoingEvent } from "./component.js";

export interface AcpServerOptions {
  /** Server self-description name, reported in discover results. */
  name: string;
  /** Server semantic version (independent of the protocol version). */
  version?: string;
  /** ACP protocol version served; defaults to "0.2". */
  protocolVersion?: string;
  /** Validate `input` against inputSchema; default true. */
  validateInput?: boolean;
  /** Validate output against outputSchema (dev-time self-check); default false. */
  validateOutput?: boolean;
  /** Event push tuning (spec v0.2 §4.4 / §6.2). */
  events?: {
    /** Max subscriptions per stateful connection; default 64. */
    maxSubscriptionsPerConn?: number;
    /** Max queued events per connection before dropping new ones; default 256. */
    queueLimit?: number;
  };
}

/** Marker produced by {@link binaryChunk} to emit a base64 chunk. */
export interface BinaryChunk {
  bin: true;
  data: string;
}

/** Wraps a base64 string so a streaming component emits a `bin: true` chunk (spec §6). */
export function binaryChunk(base64: string): BinaryChunk {
  return { bin: true, data: base64 };
}

function isBinaryChunk(value: unknown): value is BinaryChunk {
  return typeof value === "object" && value !== null && (value as BinaryChunk).bin === true;
}

interface Subscription {
  id: string;
  component?: string;
  tags?: string[];
}

interface ConnState {
  subscriptions: Map<string, Subscription>;
  /** Events queued for delivery since the last transport drain signal. */
  backlog: number;
}

function isHttpConn(conn: Connection): boolean {
  return conn.meta.transport === "http";
}

export class AcpServer {
  readonly #registry = new Registry();
  readonly #options: {
    name: string;
    version: string;
    protocolVersion: string;
    validateInput: boolean;
    validateOutput: boolean;
    maxSubscriptionsPerConn: number;
    queueLimit: number;
  };
  readonly #ajv = new Ajv({ allErrors: true });
  readonly #schemaCache = new WeakMap<object, ValidateFunction>();
  readonly #conns = new Map<Connection, ConnState>();
  #subSeq = 0;
  #transports: { start(d: ServerDispatch, lc?: TransportLifecycle): Promise<void>; stop(): Promise<void> }[] = [];
  #httpServer: HttpServer | undefined;

  constructor(options: AcpServerOptions) {
    this.#options = {
      name: options.name,
      version: options.version ?? "0.0.0",
      protocolVersion: options.protocolVersion ?? PROTOCOL_VERSION,
      validateInput: options.validateInput ?? true,
      validateOutput: options.validateOutput ?? false,
      maxSubscriptionsPerConn: options.events?.maxSubscriptionsPerConn ?? 64,
      queueLimit: options.events?.queueLimit ?? 256,
    };
  }

  register(def: AnyComponentDef): this {
    this.#registry.register(def);
    return this;
  }

  get descriptors() {
    return this.#registry.descriptors();
  }

  get serverInfo(): AcpServerInfo {
    return {
      name: this.#options.name,
      version: this.#options.version,
      protocol: this.#options.protocolVersion,
    };
  }

  // -------------------------------------------------------------------------
  // Connection registry (stateful transports only; HTTP conns never attach)
  // -------------------------------------------------------------------------

  /** Registers a stateful connection. Called by transports via lifecycle hooks. */
  attachConnection(conn: Connection): void {
    if (!this.#conns.has(conn)) this.#conns.set(conn, { subscriptions: new Map(), backlog: 0 });
  }

  /** Removes a connection and all of its subscriptions. */
  detachConnection(conn: Connection): void {
    this.#conns.delete(conn);
  }

  /**
   * Core dispatch (spec §3.2 validation order). `raw` must be an already
   * JSON-parsed request; replies (including stream chunks) go over `conn`.
   * Responses echo the request's `acp` value (spec v0.2 §5.3).
   */
  readonly handle: ServerDispatch = async (raw: unknown, conn: Connection): Promise<void> => {
    const validation = validateEnvelope(raw);
    if (!validation.ok) {
      conn.send(
        errorEnvelope(validation.id, validation.code, validation.message, validation.data, validation.id ? String((raw as AcpRequest).acp) : undefined)
      );
      return;
    }

    const req = validation.request;
    if (!isVersionSupported(req.acp, this.#options.protocolVersion)) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.UNSUPPORTED_VERSION, "unsupported protocol version", {
          supported: [this.#options.protocolVersion],
        }, req.acp)
      );
      return;
    }

    try {
      switch (req.op) {
        case "discover":
          await this.#discover(req, conn);
          break;
        case "call":
          await this.#call(req, conn);
          break;
        case "$ping":
          this.#ping(req, conn);
          break;
        case "$subscribe":
          this.#subscribe(req, conn);
          break;
        case "$unsubscribe":
          this.#unsubscribe(req, conn);
          break;
      }
    } catch (error) {
      conn.send(this.#toErrorEnvelope(req.id, req.acp, error));
    }
  };

  #ping(req: AcpRequest, conn: Connection): void {
    const input = (req.input ?? {}) as { ts?: number };
    const result: { pong: number; ts?: number } = { pong: Date.now() };
    if (typeof input.ts === "number") result.ts = input.ts;
    conn.send({ acp: req.acp, id: req.id, ok: true, result });
  }

  #subscribe(req: AcpRequest, conn: Connection): void {
    if (isHttpConn(conn)) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.EVENT_UNSUPPORTED, "events unsupported on connectionless transport", undefined, req.acp)
      );
      return;
    }
    const state = this.#conns.get(conn);
    if (!state) {
      conn.send(errorEnvelope(req.id, AcpErrorCode.INTERNAL_ERROR, "connection not registered", undefined, req.acp));
      return;
    }
    if (state.subscriptions.size >= this.#options.maxSubscriptionsPerConn) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.SUBSCRIPTION_LIMIT, `subscription limit reached (${this.#options.maxSubscriptionsPerConn})`, undefined, req.acp)
      );
      return;
    }
    const input = (req.input ?? {}) as { component?: string; tags?: string[] };
    const sub: Subscription = { id: `s-${(++this.#subSeq).toString(16)}` };
    if (input.component !== undefined) sub.component = input.component;
    else sub.tags = input.tags;
    state.subscriptions.set(sub.id, sub);
    conn.send({ acp: req.acp, id: req.id, ok: true, result: { subscription: sub.id } });
  }

  #unsubscribe(req: AcpRequest, conn: Connection): void {
    if (isHttpConn(conn)) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.EVENT_UNSUPPORTED, "events unsupported on connectionless transport", undefined, req.acp)
      );
      return;
    }
    const state = this.#conns.get(conn);
    if (state) {
      const input = (req.input ?? null) as { component?: string; tags?: string[] } | null;
      if (!input) {
        state.subscriptions.clear();
      } else {
        for (const [id, sub] of state.subscriptions) {
          const match =
            input.component !== undefined ? sub.component === input.component : sub.tags !== undefined && input.tags !== undefined && input.tags.every((t) => sub.tags!.includes(t));
          if (match) state.subscriptions.delete(id);
        }
      }
    }
    conn.send({ acp: req.acp, id: req.id, ok: true, result: null });
  }

  /**
   * Pushes an $event to every matching subscription on every stateful
   * connection (spec v0.2 §6.2). Best-effort, at-most-once; bounded queues.
   */
  emit(event: { component?: string; tags?: string[]; data: unknown; ts?: number }): void {
    const tags = event.tags ?? (event.component ? this.#registry.get(event.component)?.tags : undefined);
    if (!event.component && !tags?.length) return;
    const frame: AcpServerMessage = {
      acp: this.#options.protocolVersion,
      id: null,
      event: { ...(event.component !== undefined ? { component: event.component } : {}), ...(tags !== undefined ? { tags } : {}), data: event.data, ...(event.ts !== undefined ? { ts: event.ts } : {}) } as AcpEvent,
    };
    for (const [conn, state] of this.#conns) {
      if (state.subscriptions.size === 0) continue;
      let matched = false;
      for (const sub of state.subscriptions.values()) {
        if (sub.component !== undefined) {
          if (sub.component === event.component) { matched = true; break; }
        } else if (sub.tags !== undefined && tags !== undefined) {
          if (sub.tags.every((t) => tags.includes(t))) { matched = true; break; }
        }
      }
      if (!matched) continue;
      const backlog = (conn.eventBacklog?.() ?? state.backlog) + 0;
      if (backlog >= this.#options.queueLimit) continue; // drop new events when backlogged
      state.backlog++;
      conn.send(frame);
    }
  }

  async #discover(req: AcpRequest, conn: Connection): Promise<void> {
    let descriptors = this.#registry.descriptors();
    if (req.component !== undefined) {
      descriptors = descriptors.filter((d) => d.id === req.component);
    }
    if (req.tags !== undefined) {
      descriptors = descriptors.filter((d) => req.tags!.every((t) => d.tags?.includes(t)));
    }
    const result: DiscoverResult = { server: this.serverInfo, components: descriptors };
    conn.send({ acp: req.acp, id: req.id, ok: true, result });
  }

  async #call(req: AcpRequest, conn: Connection): Promise<void> {
    const def = this.#registry.get(req.component!);
    if (!def) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.COMPONENT_NOT_FOUND, `component not found: ${req.component}`, undefined, req.acp)
      );
      return;
    }

    if (this.#options.validateInput && def.inputSchema) {
      const validate = this.#compile(def.inputSchema);
      const input = req.input ?? null;
      if (!validate(input)) {
        conn.send(
          errorEnvelope(req.id, AcpErrorCode.INVALID_INPUT, "input validation failed", {
            errors: validate.errors?.map((e: ErrorObject) => `${e.instancePath || "input"} ${e.message ?? ""}`.trim()),
          }, req.acp)
        );
        return;
      }
    }

    const controller = new AbortController();
    const ctx = {
      conn,
      request: req,
      meta: req.meta,
      signal: conn.signal ?? controller.signal,
      emit: (event: OutgoingEvent) => {
        this.emit({ component: def.id, tags: event.tags ?? def.tags, data: event.data, ts: event.ts });
      },
    };

    const output = await def.handle(req.input, ctx);

    if (isAsyncIterable(output)) {
      if (req.stream !== true) {
        conn.send(
          errorEnvelope(req.id, AcpErrorCode.STREAM_REQUIRED, `component ${def.id} requires stream:true`, undefined, req.acp)
        );
        return;
      }
      await this.#sendChunks(req, conn, output);
      return;
    }

    if (this.#options.validateOutput && def.outputSchema) {
      const validate = this.#compile(def.outputSchema);
      if (!validate(output)) {
        conn.send(
          errorEnvelope(req.id, AcpErrorCode.INVALID_OUTPUT, "output validation failed", {
            errors: validate.errors?.map((e: ErrorObject) => `${e.instancePath || "output"} ${e.message ?? ""}`.trim()),
          }, req.acp)
        );
        return;
      }
    }

    if (req.stream === true) {
      // Non-streaming component called with stream:true: wrap in a single
      // terminated chunk (spec §4.2 — uniform handling, no special cases).
      conn.send({ acp: req.acp, id: req.id, chunk: { seq: 0, end: true, data: output } });
      return;
    }

    conn.send({ acp: req.acp, id: req.id, ok: true, result: output });
  }

  async #sendChunks(req: AcpRequest, conn: Connection, chunks: AsyncIterable<unknown>): Promise<void> {
    let seq = 0;
    for await (const value of chunks) {
      if (isBinaryChunk(value)) {
        conn.send({ acp: req.acp, id: req.id, chunk: { seq, end: false, bin: true, data: value.data } });
      } else {
        conn.send({ acp: req.acp, id: req.id, chunk: { seq, end: false, data: value } });
      }
      seq++;
    }
    conn.send({ acp: req.acp, id: req.id, chunk: { seq, end: true, data: null } });
  }

  #toErrorEnvelope(id: string, acp: string, error: unknown): AcpServerMessage {
    if (error instanceof AcpError) {
      return errorEnvelope(id, error.code, error.message, error.data, acp);
    }
    const message = error instanceof Error ? error.message : String(error);
    return errorEnvelope(id, AcpErrorCode.COMPONENT_ERROR, `component handler threw: ${message}`, undefined, acp);
  }

  #compile(schema: object): ValidateFunction {
    const cached = this.#schemaCache.get(schema);
    if (cached) return cached;
    const validate = this.#ajv.compile(schema);
    this.#schemaCache.set(schema, validate);
    return validate;
  }

  // -------------------------------------------------------------------------
  // Lifecycle: HTTP + WebSocket share one port (spec §9-10); stdio is a mode.
  // -------------------------------------------------------------------------

  #lifecycle(): TransportLifecycle {
    return {
      onConnection: (conn) => this.attachConnection(conn),
      onDisconnect: (conn) => this.detachConnection(conn),
    };
  }

  /** Connection lifecycle for wiring custom transports (stdio pair tests etc.). */
  get connectionLifecycle(): TransportLifecycle {
    return this.#lifecycle();
  }

  /**
   * Starts HTTP + WebSocket on the same port (POST http://host:port/acp handles
   * envelopes; ws://host:port/acp upgrades; GET /acp/discover for browsing).
   */
  async listen(options: ListenOptions = {}): Promise<ListenResult> {
    const httpServer = createHttpServer();
    const http = new HttpServerTransport({ server: httpServer });
    const ws = new WsServerTransport({ server: httpServer });
    const lifecycle = this.#lifecycle();
    await http.start(this.handle);
    await ws.start(this.handle, lifecycle);
    await new Promise<void>((resolve) => httpServer.listen(options.port ?? 0, options.host, resolve));
    this.#transports.push(http, ws);
    this.#httpServer = httpServer;
    const addr = httpServer.address();
    return { port: typeof addr === "object" && addr !== null ? addr.port : 0 };
  }

  /** Serves the registry over stdin/stdout (spec §11; used for MCP bridging). */
  async serveStdio(): Promise<void> {
    const stdio = new StdioServerTransport();
    await stdio.start(this.handle, this.#lifecycle());
    this.#transports.push(stdio);
  }

  /** Stops every started transport. */
  async shutdown(): Promise<void> {
    for (const t of this.#transports.splice(0)) await t.stop();
    const httpServer = this.#httpServer;
    this.#httpServer = undefined;
    if (httpServer) {
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    }
  }
}

export interface ListenOptions {
  /** Port to listen on; default 0 (ephemeral). */
  port?: number;
  host?: string;
}

export interface ListenResult {
  port: number;
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in (value as Record<symbol, unknown>)
  );
}
