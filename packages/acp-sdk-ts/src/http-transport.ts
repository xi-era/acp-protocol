/**
 * HTTP transport (spec §9): POST /acp is the single MUST endpoint;
 * streaming = NDJSON. Server reuses node:http; client uses fetch.
 */
import { createServer, type IncomingMessage, type Server as HttpServer, type ServerResponse } from "node:http";
import { AcpErrorCode, acpCodeToHttpStatus } from "./errors.js";
import { PROTOCOL_VERSION, errorEnvelope } from "./codec.js";
import type { AcpRequest, AcpServerMessage } from "./types.js";
import type { ClientRequestOptions, ClientTransport, Connection, ServerDispatch, ServerTransport, TransportLifecycle } from "./transport.js";

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

export interface HttpServerTransportOptions {
  /** Existing node:http server to attach routes to (shared with WebSocket). */
  server?: HttpServer;
  /** Port when no server is supplied; default 0 (ephemeral). */
  port?: number;
  host?: string;
}

export class HttpServerTransport implements ServerTransport {
  readonly #options: HttpServerTransportOptions;
  #server: HttpServer | undefined;
  #ownsServer = false;
  #dispatch: ServerDispatch | undefined;

  constructor(options: HttpServerTransportOptions = {}) {
    this.#options = options;
  }

  /** Actual port after start() (useful when started with port 0). */
  get port(): number {
    const addr = this.#server?.address();
    return typeof addr === "object" && addr !== null ? addr.port : 0;
  }

  async start(dispatch: ServerDispatch, _lifecycle?: TransportLifecycle): Promise<void> {
    this.#dispatch = dispatch;
    if (this.#options.server) {
      this.#server = this.#options.server;
      this.#ownsServer = false;
    } else {
      this.#server = createServer();
      this.#ownsServer = true;
    }
    this.#server.on("request", this.#onRequest);
    if (this.#ownsServer) {
      await new Promise<void>((resolve) =>
        this.#server!.listen(this.#options.port ?? 0, this.#options.host, resolve)
      );
    }
  }

  async stop(): Promise<void> {
    this.#server?.off("request", this.#onRequest);
    if (this.#ownsServer) {
      await new Promise<void>((resolve, reject) =>
        this.#server!.close((err) => (err ? reject(err) : resolve()))
      );
    }
    this.#server = undefined;
  }

  #onRequest = (req: IncomingMessage, res: ServerResponse): void => {
    const url = (req.url ?? "").split("?")[0];
    if (url === "/acp") {
      if (req.method !== "POST") {
        this.#replyError(res, null, AcpErrorCode.METHOD_NOT_ALLOWED, `method ${req.method} not allowed on /acp`);
        return;
      }
      void this.#handlePost(req, res);
      return;
    }
    if (url === "/acp/discover" && req.method === "GET") {
      void this.#handleGetDiscover(res);
      return;
    }
    if (url === "/acp/health" && req.method === "GET") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false }));
  };

  async #handlePost(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const contentType = String(req.headers["content-type"] ?? "");
    if (!contentType.includes("application/json")) {
      this.#replyError(res, null, AcpErrorCode.UNSUPPORTED_MEDIA_TYPE, "content-type must be application/json");
      return;
    }

    let raw: unknown;
    try {
      raw = JSON.parse(await readBody(req));
    } catch {
      this.#replyError(res, null, AcpErrorCode.PARSE_ERROR, "request body is not valid JSON");
      return;
    }

    const dispatch = this.#dispatch!;
    const request = raw as AcpRequest;
    const streaming = typeof request === "object" && request !== null && request.stream === true;

    if (streaming) {
      res.writeHead(200, { "content-type": "application/x-ndjson", "transfer-encoding": "chunked" });
    }

    const conn: Connection & { send(msg: AcpServerMessage): void } = {
      meta: { transport: "http", ip: req.socket.remoteAddress },
      send: (msg) => {
        const line = JSON.stringify(msg);
        if (streaming) {
          res.write(line + "\n");
        } else {
          const status = "ok" in msg && msg.ok === false ? acpCodeToHttpStatus(msg.error.code) : 200;
          res.writeHead(status, { "content-type": "application/json" });
          res.end(line);
        }
      },
      close: async () => {
        res.end();
      },
    };

    if (streaming) {
      await dispatch(raw, conn);
      res.end();
    } else {
      // Buffered: non-streaming handlers send exactly one frame via conn.send.
      await dispatch(raw, conn);
      if (!res.writableEnded) {
        // Handler sent nothing (should not happen): reply with a generic error.
        conn.send(errorEnvelope(null, AcpErrorCode.INTERNAL_ERROR, "no reply from dispatcher"));
      }
    }
  }

  async #handleGetDiscover(res: ServerResponse): Promise<void> {
    let reply: AcpServerMessage | undefined;
    const conn: Connection = {
      meta: { transport: "http" },
      send: (msg) => {
        reply ??= msg;
      },
      close: async () => {},
    };
    await this.#dispatch!({ acp: PROTOCOL_VERSION, id: `get-${Date.now()}`, op: "discover" }, conn);
    const status = reply && "ok" in reply && reply.ok === false ? acpCodeToHttpStatus(reply.error.code) : 200;
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(reply && "ok" in reply && reply.ok === true ? reply.result : reply));
  }

  #replyError(res: ServerResponse, id: string | null, code: number, message: string): void {
    const body = errorEnvelope(id, code, message);
    res.writeHead(acpCodeToHttpStatus(code), { "content-type": "application/json" });
    res.end(JSON.stringify(body));
  }
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

export interface HttpClientTransportOptions {
  url: string;
  headers?: Record<string, string>;
}

export class HttpClientTransport implements ClientTransport {
  readonly #url: string;
  readonly #headers: Record<string, string>;

  constructor(options: HttpClientTransportOptions) {
    this.#url = options.url;
    this.#headers = options.headers ?? {};
  }

  async connect(): Promise<void> {}

  async request(req: AcpRequest, opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const res = await fetch(this.#url, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.#headers },
      body: JSON.stringify(req),
      signal: opts?.signal,
    });
    const text = await res.text();
    if (res.headers.get("content-type")?.includes("x-ndjson")) {
      const lines = text.split("\n").filter((l) => l.trim() !== "");
      const first = lines[0];
      if (!first) throw new Error("empty NDJSON response");
      return JSON.parse(first) as AcpServerMessage;
    }
    try {
      return JSON.parse(text) as AcpServerMessage;
    } catch {
      throw new Error(`invalid ACP response (HTTP ${res.status}): ${text.slice(0, 120)}`);
    }
  }

  async *requestStream(req: AcpRequest, opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    const res = await fetch(this.#url, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/x-ndjson", ...this.#headers },
      body: JSON.stringify(req),
      signal: opts?.signal,
    });
    if (!res.body) throw new Error("response has no body");
    const decoder = new TextDecoder();
    let buffer = "";
    for await (const chunk of res.body) {
      buffer += decoder.decode(chunk as Uint8Array, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (line !== "") yield JSON.parse(line) as AcpServerMessage;
      }
    }
    const tail = buffer.trim();
    if (tail !== "") yield JSON.parse(tail) as AcpServerMessage;
  }

  async close(): Promise<void> {}
}
