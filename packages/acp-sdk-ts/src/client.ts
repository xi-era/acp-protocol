/**
 * AcpClient (spec §3-6): discover / call / callStream over any ClientTransport.
 * The transport is auto-selected from the URL scheme, or injected directly
 * (e.g. MemoryClientTransport for tests and adapters).
 */
import { AcpError, AcpErrorCode } from "./errors.js";
import type { AcpEvent } from "./types.js";
import { PROTOCOL_VERSION } from "./codec.js";
import type { AcpRequest, AcpChunk, ComponentDescriptor } from "./types.js";
import type { ClientRequestOptions, ClientTransport } from "./transport.js";

export interface AcpClientOptions {
  /**
   * Endpoint URL: "http(s)://host:port/acp" or "ws(s)://host:port/acp".
   * Ignored when `transport` is injected.
   */
  url?: string;
  /** Per-call timeout in milliseconds; default 30000. */
  timeoutMs?: number;
  /** Protocol version to declare on requests; default "0.2". */
  protocolVersion?: string;
  /** HTTP headers (also merged into the WS handshake). */
  headers?: Record<string, string>;
  /** Injected transport; takes precedence over `url`. */
  transport?: ClientTransport;
  /** WS keepalive idle interval via `$ping` (spec v0.2 §4.3); 0 disables. Default 30000. */
  keepAliveMs?: number;
  /** `$ping` reply timeout; exceeding it kills and reconnects the WS. Default 10000. */
  pongTimeoutMs?: number;
}

export interface AcpReply {
  acp: string;
  id: string | null;
  ok: true;
  result: unknown;
}

/** Filter for event subscriptions: exactly one of component / tags. */
export interface AcpSubscriptionFilter {
  component?: string;
  tags?: string[];
}

/** Handle returned by AcpClient.subscribe. */
export interface AcpSubscription {
  unsubscribe(): Promise<void>;
}

export class AcpClient {
  readonly #options: Required<Omit<AcpClientOptions, "url" | "headers" | "transport">> &
    Pick<AcpClientOptions, "headers">;
  #transport: ClientTransport | undefined;
  #transportFactory: () => Promise<ClientTransport>;

  constructor(options: AcpClientOptions) {
    this.#options = {
      timeoutMs: options.timeoutMs ?? 30_000,
      protocolVersion: options.protocolVersion ?? PROTOCOL_VERSION,
      keepAliveMs: options.keepAliveMs ?? 30_000,
      pongTimeoutMs: options.pongTimeoutMs ?? 10_000,
      headers: options.headers,
    };
    if (options.transport) {
      const injected = options.transport;
      this.#transportFactory = async () => injected;
    } else {
      if (!options.url) throw new Error("AcpClient requires either url or transport");
      const url = options.url;
      const headers = options.headers;
      this.#transportFactory = async () => {
        // Lazy dynamic imports keep ./client importable without Node built-ins.
        const scheme = url.slice(0, url.indexOf(":")).toLowerCase();
        if (scheme === "http" || scheme === "https") {
          const { HttpClientTransport } = await import("./http-transport.js");
          return new HttpClientTransport({ url, headers });
        }
        if (scheme === "ws" || scheme === "wss") {
          const { WsClientTransport } = await import("./ws-transport.js");
          return new WsClientTransport({
            url,
            headers,
            keepAliveMs: this.#options.keepAliveMs,
            pongTimeoutMs: this.#options.pongTimeoutMs,
          });
        }
        throw new Error(
          `unsupported URL scheme: ${scheme} (use http(s):// or ws(s)://, or inject a transport)`
        );
      };
    }
  }

  /** Establishes the connection (no-op for connectionless transports). */
  async connect(): Promise<void> {
    await (await this.#getTransportAsync()).connect();
  }

  /** Discover all components, or a single one by id (empty array when absent). */
  async discover(componentId?: string, opts?: ClientRequestOptions): Promise<ComponentDescriptor[]> {
    const req: Partial<AcpRequest> = { op: "discover" };
    if (componentId !== undefined) req.component = componentId;
    const reply = await this.#request(req, opts);
    this.#assertOk(reply);
    const result = reply.result as { components: ComponentDescriptor[] };
    return result.components;
  }

  /** Single call; resolves with the bare result value. */
  async call<T = unknown>(
    componentId: string,
    input?: unknown,
    opts?: ClientRequestOptions
  ): Promise<T> {
    const reply = await this.#request({ op: "call", component: componentId, input }, opts);
    this.#assertOk(reply);
    return reply.result as T;
  }

  /**
   * Streamed call; yields chunk payloads in seq order and completes after the
   * end frame. Throws AcpError when an error frame terminates the stream.
   */
  async *callStream(
    componentId: string,
    input?: unknown,
    opts?: ClientRequestOptions
  ): AsyncGenerator<AcpChunk, void, undefined> {
    const req: Partial<AcpRequest> = { op: "call", component: componentId, input, stream: true };
    const transport = await this.#getTransportAsync();
    const deadline = Date.now() + this.#options.timeoutMs;
    for await (const msg of transport.requestStream(await this.#fullRequest(req), opts)) {
      if ("chunk" in msg) {
        yield msg.chunk;
        if (msg.chunk.end) return;
      } else if ("event" in msg) {
        continue; // events are routed via onEvent, not call streams
      } else if ("ok" in msg && msg.ok === false) {
        throw AcpError.from(msg.error);
      } else {
        return; // one-shot reply to a stream request: nothing to iterate
      }
      if (Date.now() > deadline) {
        throw new AcpError(AcpErrorCode.TIMEOUT, `stream timed out after ${this.#options.timeoutMs}ms`);
      }
    }
  }

  /** Low-level escape hatch: sends a full envelope and resolves with the reply. */
  async request(
    envelope: Partial<AcpRequest> & Record<string, unknown>,
    opts?: ClientRequestOptions
  ): Promise<AcpReply> {
    const reply = await this.#request(envelope as AcpRequest, opts);
    return reply as AcpReply;
  }

  async close(): Promise<void> {
    await (await this.#getTransportAsync()).close();
  }

  /**
   * Subscribes to server events (spec v0.2 §4.4). Exactly one of
   * `filter.component` / `filter.tags` must be set. Throws AcpError(50100)
   * on transports without event support (e.g. HTTP).
   */
  async subscribe(
    filter: { component?: string; tags?: string[] },
    handler: (event: AcpEvent) => void
  ): Promise<AcpSubscription> {
    if ((filter.component !== undefined) === (filter.tags !== undefined)) {
      throw new AcpError(
        AcpErrorCode.INVALID_ENVELOPE,
        "subscription filter requires exactly one of component/tags"
      );
    }
    const transport = await this.#getTransportAsync();
    if (!transport.eventsSupported?.() || !transport.onEvent) {
      throw new AcpError(
        AcpErrorCode.EVENT_UNSUPPORTED,
        "events unsupported on connectionless transport"
      );
    }
    const off = transport.onEvent(handler);
    this.#assertOk(await this.#request({ op: "$subscribe", input: filter }));
    return {
      unsubscribe: async () => {
        off();
        const reply = await this.#request({ op: "$unsubscribe", input: filter });
        this.#assertOk(reply);
      },
    };
  }

  async #getTransportAsync(): Promise<ClientTransport> {
    this.#transport ??= await this.#transportFactory();
    return this.#transport;
  }

  #assertOk(reply: AcpServerMessage): asserts reply is Extract<AcpServerMessage, { ok: true }> {
    if (!("ok" in reply) || reply.ok !== true) {
      const error =
        "error" in reply
          ? reply.error
          : { code: 50000, message: "unexpected reply shape" };
      throw AcpError.from(error);
    }
  }

  #fallbackTried = false;

  async #request(req: Partial<AcpRequest>, opts?: ClientRequestOptions): Promise<AcpServerMessage> {
    const transport = await this.#getTransportAsync();
    const full = await this.#fullRequest(req);
    const reply = await withTimeout(transport.request(full, opts), this.#options.timeoutMs, full.id);
    // Fallback ladder step 1 (spec v0.2 §12.2): 40003 is an error FRAME, not an
    // exception — check the resolved reply and retry once with the highest
    // server-supported version, locking it for this client.
    if (
      !this.#fallbackTried &&
      "ok" in reply &&
      reply.ok === false &&
      reply.error.code === AcpErrorCode.UNSUPPORTED_VERSION
    ) {
      const best = pickHighestSupported(reply.error.data);
      if (best && best !== this.#options.protocolVersion) {
        this.#options.protocolVersion = best;
        this.#fallbackTried = true;
        return this.#request(req, opts);
      }
    }
    return reply;
  }

  async #fullRequest(req: Partial<AcpRequest>): Promise<AcpRequest> {
    return {
      ...req,
      acp: req.acp ?? this.#options.protocolVersion,
      id: req.id ?? randomId(),
      op: req.op ?? "discover",
    };
  }
}

type AcpServerMessage =
  | { acp: string; id: string; ok: true; result: unknown }
  | { acp: string; id: string | null; ok: false; error: { code: number; message: string; data?: unknown } }
  | { acp: string; id: string; chunk: AcpChunk }
  | { acp: string; id: null; event: { component?: string; tags?: string[]; data: unknown; ts?: number } };

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, id: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new AcpError(AcpErrorCode.TIMEOUT, `call ${id} timed out after ${timeoutMs}ms`)),
      timeoutMs
    );
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function randomId(): string {
  return `acp-${globalThis.crypto.randomUUID()}`;
}

/** Picks the highest version from a 40003 `data.supported` payload. */
function pickHighestSupported(data: unknown): string | undefined {
  const supported = (data as { supported?: unknown } | undefined)?.supported;
  if (!Array.isArray(supported)) return undefined;
  const versions = supported
    .filter((v): v is string => typeof v === "string")
    .map((v) => ({ v, parts: /^(\d+)\.(\d+)$/.exec(v)?.slice(1).map(Number) ?? null }))
    .filter((x): x is { v: string; parts: [number, number] } => x.parts !== null);
  if (versions.length === 0) return undefined;
  versions.sort((a, b) => b.parts[0] - a.parts[0] || b.parts[1] - a.parts[1]);
  return versions[0]?.v;
}
