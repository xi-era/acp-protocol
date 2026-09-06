/**
 * In-process transport: connects a ClientTransport directly to an AcpServer
 * dispatch without any network loopback. Used by unit tests and by adapters
 * bridging protocols inside a single process. Supports events (v0.2).
 */
import type { AcpEvent, AcpRequest, AcpServerMessage } from "./types.js";
import type { ClientRequestOptions, ClientTransport, Connection, ServerDispatch } from "./transport.js";

interface Pending {
  resolve: (msg: AcpServerMessage) => void;
  chunks: AcpServerMessage[];
  notify: (() => void) | undefined;
  done: boolean;
}

/** Client transport bound to one server instance. */
export interface MemoryClientTransportOptions {
  onConnect?(conn: Connection): void;
  onClose?(conn: Connection): void;
}

export class MemoryClientTransport implements ClientTransport {
  #dispatch: ServerDispatch;
  #options: MemoryClientTransportOptions;
  #conn: Connection | undefined;
  #pending = new Map<string, Pending>();
  #orphan: AcpServerMessage | undefined;
  #eventHandlers = new Set<(event: AcpEvent) => void>();

  constructor(dispatch: ServerDispatch, options: MemoryClientTransportOptions = {}) {
    this.#dispatch = dispatch;
    this.#options = options;
  }

  async connect(): Promise<void> {
    if (this.#conn) return;
    this.#conn = {
      meta: { transport: "memory" },
      send: (msg) => this.#onMessage(msg),
      close: async () => {},
    };
    this.#options.onConnect?.(this.#conn);
  }

  async request(req: AcpRequest, _opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const conn = this.#ensureConn();
    // Register pending BEFORE dispatch: replies can arrive synchronously.
    let resolveFn!: (msg: AcpServerMessage) => void;
    const promise = new Promise<AcpServerMessage>((r) => (resolveFn = r));
    const pending: Pending = { resolve: resolveFn, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    try {
      await this.#dispatch(req, conn);
    } catch (error) {
      if (!pending.done) {
        pending.done = true;
        pending.resolve({
          acp: req.acp,
          id: req.id,
          ok: false,
          error: { code: 50000, message: error instanceof Error ? error.message : String(error) },
        });
      }
    }
    if (!pending.done) {
      // Reply arrived with an id that didn't match (e.g. id:null error replies
      // for malformed envelopes): fall back to the stashed orphan frame.
      pending.done = true;
      const orphan = this.#orphan;
      this.#orphan = undefined;
      pending.resolve(
        orphan ?? {
          acp: req.acp,
          id: req.id,
          ok: false,
          error: { code: 50000, message: "no reply from server" },
        }
      );
    }
    this.#pending.delete(req.id);
    return promise;
  }

  async *requestStream(req: AcpRequest, _opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    const conn = this.#ensureConn();
    const pending: Pending = { resolve: () => {}, chunks: [], notify: undefined, done: false };
    this.#pending.set(req.id, pending);
    const done = this.#dispatch(req, conn).catch(() => {
      pending.done = true;
      pending.notify?.();
    });
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
      // Drain dispatch errors that happen after the terminal frame.
      done.catch(() => {});
    }
  }

  async close(): Promise<void> {
    if (this.#conn) {
      this.#options.onClose?.(this.#conn);
      this.#conn = undefined;
    }
  }

  eventsSupported(): boolean {
    return true;
  }

  onEvent(handler: (event: AcpEvent) => void): () => void {
    this.#eventHandlers.add(handler);
    return () => this.#eventHandlers.delete(handler);
  }

  #ensureConn(): Connection {
    if (!this.#conn) void this.connect();
    return this.#conn!;
  }

  #onMessage(msg: AcpServerMessage): void {
    if ("event" in msg) {
      for (const h of this.#eventHandlers) h(msg.event);
      return;
    }
    if ("op" in msg) return; // server-initiated requests are not expected in memory
    const pending = this.#pending.get(msg.id ?? "");
    if (!pending) {
      this.#orphan = msg;
      return;
    }
    pending.chunks.push(msg);
    const isTerminal = "ok" in msg || ("chunk" in msg && msg.chunk.end === true);
    if (isTerminal) {
      pending.done = true;
      pending.resolve(msg);
    }
    pending.notify?.();
  }
}

/** Server facade with connection registration (AcpServer satisfies this). */
interface AttachableServer {
  handle: ServerDispatch;
  attachConnection?(conn: Connection): void;
  detachConnection?(conn: Connection): void;
}

export function createMemoryClient(server: AttachableServer): MemoryClientTransport {
  return new MemoryClientTransport(server.handle.bind(server), {
    onConnect: (conn) => server.attachConnection?.(conn),
    onClose: (conn) => server.detachConnection?.(conn),
  });
}
