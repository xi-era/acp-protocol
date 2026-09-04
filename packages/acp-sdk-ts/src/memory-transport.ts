/**
 * In-process transport: connects a ClientTransport directly to an AcpServer
 * dispatch without any network loopback. Used by unit tests and by adapters
 * bridging protocols inside a single process.
 */
import type { AcpRequest, AcpServerMessage } from "./types.js";
import type { ClientRequestOptions, ClientTransport, ServerDispatch } from "./transport.js";

/** Minimal pushable async queue. */
class MessageQueue implements AsyncIterable<AcpServerMessage> {
  #items: AcpServerMessage[] = [];
  #waiters: ((r: IteratorResult<AcpServerMessage>) => void)[] = [];
  #failure: { error: unknown } | null = null;
  #closed = false;

  push(msg: AcpServerMessage): void {
    if (this.#closed) return;
    const waiter = this.#waiters.shift();
    if (waiter) waiter({ value: msg, done: false });
    else this.#items.push(msg);
  }

  close(): void {
    this.#closed = true;
    for (const w of this.#waiters.splice(0)) w({ value: undefined, done: true });
  }

  fail(error: unknown): void {
    this.#failure = { error };
    this.close();
  }

  [Symbol.asyncIterator](): AsyncIterator<AcpServerMessage> {
    return {
      next: (): Promise<IteratorResult<AcpServerMessage>> => {
        const item = this.#items.shift();
        if (item !== undefined) return Promise.resolve({ value: item, done: false });
        if (this.#failure) return Promise.reject(this.#failure.error);
        if (this.#closed) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => this.#waiters.push(resolve));
      },
    };
  }
}

/** Client transport bound to one server instance. */
export class MemoryClientTransport implements ClientTransport {
  #dispatch: ServerDispatch;

  constructor(dispatch: ServerDispatch) {
    this.#dispatch = dispatch;
  }

  async connect(): Promise<void> {}

  async request(req: AcpRequest, _opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const queue = new MessageQueue();
    const conn = {
      meta: { transport: "memory" },
      send: (msg: AcpServerMessage) => queue.push(msg),
      close: async () => {},
    };
    try {
      await this.#dispatch(req, conn);
    } catch (error) {
      queue.fail(error);
    }
    queue.close();

    let reply: AcpServerMessage | undefined;
    for await (const msg of queue) reply = msg;
    if (!reply) {
      throw new Error("server closed the connection without a reply");
    }
    return reply;
  }

  async *requestStream(req: AcpRequest, _opts?: ClientRequestOptions): AsyncIterable<AcpServerMessage> {
    const queue = new MessageQueue();
    const conn = {
      meta: { transport: "memory" },
      send: (msg: AcpServerMessage) => queue.push(msg),
      close: async () => {},
    };
    const done = this.#dispatch(req, conn).catch((error) => queue.fail(error));

    for await (const msg of queue) {
      yield msg;
      // Stop consuming after a terminal frame (end chunk or error/one-shot reply).
      if ("chunk" in msg && msg.chunk.end) break;
      if ("ok" in msg) break;
    }
    await done;
  }

  async close(): Promise<void> {}
}

export function createMemoryClient(server: { handle: ServerDispatch }): MemoryClientTransport {
  return new MemoryClientTransport(server.handle.bind(server));
}
