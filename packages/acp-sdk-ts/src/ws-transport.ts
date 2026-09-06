/**
 * WebSocket transport (spec §10): one text frame = one envelope; no handshake
 * packet; per-message version negotiation; concurrent calls multiplexed by id.
 * Shares the HTTP server's port via the "upgrade" event.
 */
import WebSocket, { WebSocketServer } from "ws";
import { createServer as createHttpServer } from "node:http";
import type { Duplex } from "node:stream";
import type { IncomingMessage, Server as HttpServer } from "node:http";
import { AcpError, AcpErrorCode } from "./errors.js";
import { PROTOCOL_VERSION, errorEnvelope } from "./codec.js";
import type { AcpEvent, AcpRequest, AcpServerMessage } from "./types.js";
import type { ClientRequestOptions, ClientTransport, Connection, ServerDispatch, ServerTransport, TransportLifecycle } from "./transport.js";

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

export interface WsServerTransportOptions {
  /** Existing node:http server whose "upgrade" events serve ws://host/acp. */
  server?: HttpServer;
  /** Standalone port when no server is supplied. */
  port?: number;
  host?: string;
}

export class WsServerTransport implements ServerTransport {
  readonly #options: WsServerTransportOptions;
  #wss: WebSocketServer | undefined;
  #connections = new Set<WebSocket>();
  #standaloneServer: HttpServer | undefined;
  #onUpgrade: ((req: IncomingMessage, socket: Duplex, head: Buffer) => void) | undefined;

  constructor(options: WsServerTransportOptions = {}) {
    this.#options = options;
  }

  async start(dispatch: ServerDispatch, lifecycle?: TransportLifecycle): Promise<void> {
    this.#wss = new WebSocketServer({ noServer: true, perMessageDeflate: true });

    this.#wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
      this.#connections.add(ws);
      ws.on("close", () => this.#connections.delete(ws));
      const conn: Connection = {
        meta: { transport: "ws", ip: req.socket.remoteAddress },
        send: (msg) => ws.send(JSON.stringify(msg)),
        close: async () => ws.close(),
        eventBacklog: () => ws.bufferedAmount,
      };
      lifecycle?.onConnection?.(conn);
      ws.on("close", () => lifecycle?.onDisconnect?.(conn));
      ws.on("message", (data: unknown) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(String(data));
        } catch {
          conn.send(errorEnvelope(null, AcpErrorCode.PARSE_ERROR, "frame is not valid JSON"));
          return;
        }
        void dispatch(parsed, conn);
      });
      ws.on("error", () => {});
    });

    const existing = this.#options.server;
    if (existing) {
      this.#onUpgrade = (req, socket, head) => {
        const path = (req.url ?? "").split("?")[0];
        if (path !== "/acp") {
          socket.destroy();
          return;
        }
        this.#wss!.handleUpgrade(req, socket, head, (ws) => this.#wss!.emit("connection", ws, req));
      };
      existing.on("upgrade", this.#onUpgrade);
    } else {
      this.#standaloneServer = createHttpServer();
      this.#standaloneServer.on("upgrade", (req, socket, head) => {
        this.#wss!.handleUpgrade(req, socket, head, (ws) => this.#wss!.emit("connection", ws, req));
      });
      await new Promise<void>((resolve) =>
        this.#standaloneServer!.listen(this.#options.port ?? 0, this.#options.host, resolve)
      );
    }
  }

  async stop(): Promise<void> {
    if (this.#onUpgrade) {
      this.#options.server?.off("upgrade", this.#onUpgrade);
      this.#onUpgrade = undefined;
    }
    // Terminate open connections so close() doesn't wait on them.
    for (const ws of this.#connections) ws.terminate();
    this.#connections.clear();
    this.#wss?.close();
    this.#wss = undefined;
    const standalone = this.#standaloneServer;
    this.#standaloneServer = undefined;
    if (standalone) {
      await new Promise<void>((resolve) => standalone.close(() => resolve()));
    }
  }
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

interface Pending {
  resolve: (msg: AcpServerMessage) => void;
  chunks: AcpServerMessage[];
  notify: (() => void) | undefined;
  done: boolean;
}

export interface WsClientTransportOptions {
  url: string;
  headers?: Record<string, string>;
}

export interface WsClientTransportOptions {
  url: string;
  headers?: Record<string, string>;
  /** Idle-time keepalive interval in ms via `$ping` (spec v0.2 §4.3); 0 disables. Default 30000. */
  keepAliveMs?: number;
  /** `$ping` reply timeout in ms; exceeding it kills the connection. Default 10000. */
  pongTimeoutMs?: number;
}

export class WsClientTransport implements ClientTransport {
  readonly #url: string;
  readonly #headers: Record<string, string>;
  readonly #keepAliveMs: number;
  readonly #pongTimeoutMs: number;
  #ws: WebSocket | undefined;
  #pending = new Map<string, Pending>();
  #eventHandlers = new Set<(event: AcpEvent) => void>();
  #closedByUser = false;
  #kaTimer: ReturnType<typeof setTimeout> | undefined;
  #pongTimer: ReturnType<typeof setTimeout> | undefined;
  #kaSeq = 0;
  #keepaliveSupported = true;

  constructor(options: WsClientTransportOptions) {
    this.#url = options.url;
    this.#headers = options.headers ?? {};
    this.#keepAliveMs = options.keepAliveMs ?? 30_000;
    this.#pongTimeoutMs = options.pongTimeoutMs ?? 10_000;
  }

  async connect(): Promise<void> {
    if (this.#ws) return;
    this.#closedByUser = false;
    const ws = new WebSocket(this.#url, { headers: this.#headers });
    await new Promise<void>((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    ws.on("message", (data: unknown) => this.#onMessage(String(data)));
    ws.on("close", () => this.#onSocketClosed());
    ws.on("error", () => {});
    this.#ws = ws;
    // Auto-resubscribe after reconnect (spec v0.2 §4.4: SDK responsibility).
    for (const filter of this.#subscriptions.values()) {
      void this.request({ acp: PROTOCOL_VERSION, id: `resub-${++this.#kaSeq}`, op: "$subscribe", input: filter } as AcpRequest).catch(() => {});
    }
    this.#armKeepalive();
  }

  async request(req: AcpRequest, _opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const ws = this.#assertConnected();
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    this.#trackSubscription(req);
    this.#sendFrame(ws, req);
    const msg = await new Promise<AcpServerMessage>((resolve) => {
      pending.resolve = resolve;
    });
    this.#pending.delete(req.id);
    return msg;
  }

  #subscriptions = new Map<string, { component?: string; tags?: string[] }>();

  #trackSubscription(req: AcpRequest): void {
    if (req.op === "$subscribe") {
      this.#subscriptions.set(JSON.stringify(req.input ?? {}), (req.input ?? {}) as { component?: string; tags?: string[] });
    } else if (req.op === "$unsubscribe") {
      if (req.input === undefined || req.input === null) this.#subscriptions.clear();
      else this.#subscriptions.delete(JSON.stringify(req.input));
    }
  }

  async *requestStream(req: AcpRequest, _opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    const ws = this.#assertConnected();
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    this.#sendFrame(ws, req);

    try {
      while (true) {
        while (pending.chunks.length > 0) {
          const msg = pending.chunks.shift()!;
          yield msg;
          if ("chunk" in msg && msg.chunk.end) return;
          if ("ok" in msg) return;
        }
        if (pending.done) return;
        await new Promise<void>((resolve) => (pending.notify = resolve));
      }
    } finally {
      this.#pending.delete(req.id);
    }
  }

  async close(): Promise<void> {
    const ws = this.#ws;
    this.#ws = undefined;
    this.#closedByUser = true;
    this.#disarmKeepalive();
    this.#failAll("client closed");
    if (!ws) return;
    await new Promise<void>((resolve) => {
      ws.once("close", resolve);
      ws.close();
    });
  }

  eventsSupported(): boolean {
    return true;
  }

  onEvent(handler: (event: AcpEvent) => void): () => void {
    this.#eventHandlers.add(handler);
    return () => this.#eventHandlers.delete(handler);
  }

  #assertConnected(): WebSocket {
    if (!this.#ws) throw new Error("ws transport not connected — call connect() first");
    return this.#ws;
  }

  #sendFrame(ws: WebSocket, frame: unknown): void {
    ws.send(JSON.stringify(frame));
    if (this.#keepAliveMs > 0 && this.#keepaliveSupported) this.#armKeepalive(); // reset idle timer
  }

  // -------------------------------------------------------------------------
  // Keepalive (spec v0.2 §4.3): idle $ping with pong timeout -> dead conn
  // -------------------------------------------------------------------------

  #armKeepalive(): void {
    this.#disarmKeepalive();
    if (this.#keepAliveMs <= 0 || !this.#keepaliveSupported) return;
    this.#kaTimer = setTimeout(() => void this.#sendPing(), this.#keepAliveMs);
  }

  #disarmKeepalive(): void {
    if (this.#kaTimer) clearTimeout(this.#kaTimer);
    if (this.#pongTimer) clearTimeout(this.#pongTimer);
    this.#kaTimer = undefined;
    this.#pongTimer = undefined;
  }

  async #sendPing(): Promise<void> {
    const ws = this.#ws;
    if (!ws || this.#keepaliveSupported === false) return;
    const id = `ka-${++this.#kaSeq}`;
    const sentAt = Date.now();
    this.#startPongTimer();
    try {
      await this.request({ acp: PROTOCOL_VERSION, id, op: "$ping", input: { ts: sentAt } } as AcpRequest);
      // Reply arrived: connection alive; arm the next idle ping.
      this.#armKeepalive();
    } catch (error) {
      if (error instanceof AcpError && error.code === AcpErrorCode.UNKNOWN_OP) {
        // 0.1 server: permanently disable keepalive on this connection.
        this.#keepaliveSupported = false;
        this.#disarmKeepalive();
        return;
      }
      // Timeout / dead connection: terminate; close handler decides on reconnect.
      ws.terminate();
    }
  }

  #startPongTimer(): void {
    if (this.#pongTimer) clearTimeout(this.#pongTimer);
    this.#pongTimer = setTimeout(() => {
      this.#ws?.terminate();
    }, this.#pongTimeoutMs);
  }

  #onSocketClosed(): void {
    this.#disarmKeepalive();
    this.#failAll("connection closed");
    this.#ws = undefined;
    // Reconnect unless the user closed the transport (basic retry, 1s).
    if (!this.#closedByUser) {
      setTimeout(() => {
        if (!this.#closedByUser && !this.#ws) {
          this.connect().catch(() => {
            setTimeout(() => this.#onSocketClosed(), 1000);
          });
        }
      }, 1000);
    }
  }

  #onMessage(text: string): void {
    let msg: AcpServerMessage;
    try {
      msg = JSON.parse(text) as AcpServerMessage;
    } catch {
      return;
    }
    if ("event" in msg) {
      for (const h of this.#eventHandlers) h(msg.event);
      return;
    }
    if ("op" in msg && msg.op === "$ping") {
      // Server-initiated keepalive: MUST answer (spec v0.2 §4.3).
      const input = ((msg as AcpRequest).input ?? {}) as { ts?: number };
      const result: { pong: number; ts?: number } = { pong: Date.now() };
      if (typeof input.ts === "number") result.ts = input.ts;
      this.#ws?.send(JSON.stringify({ acp: msg.acp, id: msg.id, ok: true, result }));
      return;
    }
    const pending = this.#pending.get(msg.id ?? "");
    if (!pending) return;
    if ("ok" in msg && msg.ok === true) this.#disarmPongTimer(msg.id ?? "");
    const isTerminal =
      "ok" in msg || ("chunk" in msg && msg.chunk.end === true);
    if (isTerminal) {
      pending.done = true;
      pending.chunks.push(msg);
      pending.resolve(msg);
      pending.notify?.();
    } else {
      pending.chunks.push(msg);
      pending.notify?.();
    }
  }

  #disarmPongTimer(_id: string): void {
    if (this.#pongTimer) clearTimeout(this.#pongTimer);
    this.#pongTimer = undefined;
  }

  #failAll(reason: string): void {
    for (const pending of this.#pending.values()) {
      pending.done = true;
      pending.resolve({
        acp: PROTOCOL_VERSION,
        id: null,
        ok: false,
        error: { code: AcpErrorCode.INTERNAL_ERROR, message: reason },
      });
      pending.notify?.();
    }
    this.#pending.clear();
  }
}
