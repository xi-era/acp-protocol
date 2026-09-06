/**
 * WebSocket transport (spec §10): one text frame = one envelope; no handshake
 * packet; per-message version negotiation; concurrent calls multiplexed by id.
 * Shares the HTTP server's port via the "upgrade" event.
 */
import WebSocket, { WebSocketServer } from "ws";
import { createServer as createHttpServer } from "node:http";
import type { Duplex } from "node:stream";
import type { IncomingMessage, Server as HttpServer } from "node:http";
import { AcpErrorCode } from "./errors.js";
import { PROTOCOL_VERSION, errorEnvelope } from "./codec.js";
import type { AcpRequest, AcpServerMessage } from "./types.js";
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

export class WsClientTransport implements ClientTransport {
  readonly #url: string;
  readonly #headers: Record<string, string>;
  #ws: WebSocket | undefined;
  #pending = new Map<string, Pending>();

  constructor(options: WsClientTransportOptions) {
    this.#url = options.url;
    this.#headers = options.headers ?? {};
  }

  async connect(): Promise<void> {
    if (this.#ws) return;
    const ws = new WebSocket(this.#url, { headers: this.#headers });
    await new Promise<void>((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });
    ws.on("message", (data: unknown) => this.#onMessage(String(data)));
    ws.on("close", () => this.#failAll("connection closed"));
    ws.on("error", () => {});
    this.#ws = ws;
  }

  async request(req: AcpRequest, _opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const ws = this.#assertConnected();
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    ws.send(JSON.stringify(req));
    const msg = await new Promise<AcpServerMessage>((resolve) => {
      pending.resolve = resolve;
    });
    this.#pending.delete(req.id);
    return msg;
  }

  async *requestStream(req: AcpRequest, _opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    const ws = this.#assertConnected();
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    ws.send(JSON.stringify(req));

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
    this.#failAll("client closed");
    if (!ws) return;
    await new Promise<void>((resolve) => {
      ws.once("close", resolve);
      ws.close();
    });
  }

  #assertConnected(): WebSocket {
    if (!this.#ws) throw new Error("ws transport not connected — call connect() first");
    return this.#ws;
  }

  #onMessage(text: string): void {
    let msg: AcpServerMessage;
    try {
      msg = JSON.parse(text) as AcpServerMessage;
    } catch {
      return;
    }
    const pending = this.#pending.get(msg.id ?? "");
    if (!pending) return;
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
