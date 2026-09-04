/**
 * Stdio transport (spec §11): one line per envelope on stdin/stdout; stderr is
 * free for logs. Primarily for MCP bridging and local debugging.
 */
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { AcpErrorCode } from "./errors.js";
import { errorEnvelope } from "./codec.js";
import type { AcpRequest, AcpServerMessage } from "./types.js";
import type { ClientRequestOptions, ClientTransport, Connection, ServerDispatch, ServerTransport } from "./transport.js";

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

export interface StdioServerTransportOptions {
  input?: Readable;
  output?: Writable;
}

export class StdioServerTransport implements ServerTransport {
  readonly #options: StdioServerTransportOptions;

  constructor(options: StdioServerTransportOptions = {}) {
    this.#options = options;
  }

  async start(dispatch: ServerDispatch): Promise<void> {
    const input = this.#options.input ?? process.stdin;
    const output = this.#options.output ?? process.stdout;
    const conn: Connection = {
      meta: { transport: "stdio" },
      send: (msg) => output.write(JSON.stringify(msg) + "\n"),
      close: async () => {},
    };
    const rl = createInterface({ input, terminal: false });
    rl.on("line", (line) => {
      const trimmed = line.trim();
      if (trimmed === "") return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        conn.send(errorEnvelope(null, AcpErrorCode.PARSE_ERROR, "line is not valid JSON"));
        return;
      }
      void dispatch(parsed, conn);
    });
  }

  async stop(): Promise<void> {}
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

export interface StdioClientTransportOptions {
  input?: Readable;
  output?: Writable;
}

export class StdioClientTransport implements ClientTransport {
  readonly #input: Readable;
  readonly #output: Writable;
  #pending = new Map<string, Pending>();
  #started = false;
  #buffer = "";

  constructor(options: StdioClientTransportOptions = {}) {
    this.#input = options.input ?? process.stdin;
    this.#output = options.output ?? process.stdout;
  }

  async connect(): Promise<void> {
    if (this.#started) return;
    this.#started = true;
    this.#input.setEncoding?.("utf8");
    this.#input.on("data", (chunk: string | Buffer) => {
      this.#buffer += String(chunk);
      let idx: number;
      while ((idx = this.#buffer.indexOf("\n")) >= 0) {
        const line = this.#buffer.slice(0, idx).trim();
        this.#buffer = this.#buffer.slice(idx + 1);
        if (line !== "") this.#onLine(line);
      }
    });
  }

  async request(req: AcpRequest, _opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    this.#send(req);
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    return new Promise<AcpServerMessage>((resolve) => {
      pending.resolve = resolve;
    });
  }

  async *requestStream(req: AcpRequest, _opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    this.#send(req);
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
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

  async close(): Promise<void> {}

  #send(req: AcpRequest): void {
    this.#output.write(JSON.stringify(req) + "\n");
  }

  #onLine(line: string): void {
    let msg: AcpServerMessage;
    try {
      msg = JSON.parse(line) as AcpServerMessage;
    } catch {
      return;
    }
    const pending = this.#pending.get(msg.id ?? "");
    if (!pending) return;
    pending.chunks.push(msg);
    const isTerminal = "ok" in msg || ("chunk" in msg && msg.chunk.end === true);
    if (isTerminal) {
      pending.done = true;
      pending.resolve(msg);
    }
    pending.notify?.();
  }
}
