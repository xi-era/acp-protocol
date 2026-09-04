/**
 * AcpServer core (spec §4-6, §12): transport-agnostic op routing, schema
 * validation, streaming. Transports feed parsed envelopes to `handle`.
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
import type { AcpRequest, AcpServerInfo, AcpServerMessage, DiscoverResult } from "./types.js";
import type { Connection, ServerDispatch } from "./transport.js";

export interface AcpServerOptions {
  /** Server self-description name, reported in discover results. */
  name: string;
  /** Server semantic version (independent of the protocol version). */
  version?: string;
  /** ACP protocol version served; defaults to "0.1". */
  protocolVersion?: string;
  /** Validate `input` against inputSchema; default true. */
  validateInput?: boolean;
  /** Validate output against outputSchema (dev-time self-check); default false. */
  validateOutput?: boolean;
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

export class AcpServer {
  readonly #registry = new Registry();
  readonly #options: Required<Omit<AcpServerOptions, "name">> & { name: string };
  readonly #ajv = new Ajv({ allErrors: true });
  readonly #schemaCache = new WeakMap<object, ValidateFunction>();
  #transports: { start(d: ServerDispatch): Promise<void>; stop(): Promise<void> }[] = [];
  #httpServer: HttpServer | undefined;

  constructor(options: AcpServerOptions) {
    this.#options = {
      name: options.name,
      version: options.version ?? "0.0.0",
      protocolVersion: options.protocolVersion ?? PROTOCOL_VERSION,
      validateInput: options.validateInput ?? true,
      validateOutput: options.validateOutput ?? false,
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

  /**
   * Core dispatch (spec §3.2 validation order). `raw` must be an already
   * JSON-parsed request; replies (including stream chunks) go over `conn`.
   */
  readonly handle: ServerDispatch = async (raw: unknown, conn: Connection): Promise<void> => {
    const validation = validateEnvelope(raw);
    if (!validation.ok) {
      conn.send(errorEnvelope(validation.id, validation.code, validation.message, validation.data));
      return;
    }

    const req = validation.request;
    if (!isVersionSupported(req.acp, this.#options.protocolVersion)) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.UNSUPPORTED_VERSION, "unsupported protocol version", {
          supported: [this.#options.protocolVersion],
        })
      );
      return;
    }

    try {
      if (req.op === "discover") {
        await this.#discover(req, conn);
      } else {
        await this.#call(req, conn);
      }
    } catch (error) {
      conn.send(this.#toErrorEnvelope(req.id, error));
    }
  };

  async #discover(req: AcpRequest, conn: Connection): Promise<void> {
    let descriptors = this.#registry.descriptors();
    if (req.component !== undefined) {
      descriptors = descriptors.filter((d) => d.id === req.component);
    }
    if (req.tags !== undefined) {
      descriptors = descriptors.filter((d) => req.tags!.every((t) => d.tags?.includes(t)));
    }
    const result: DiscoverResult = { server: this.serverInfo, components: descriptors };
    conn.send({ acp: this.#options.protocolVersion, id: req.id, ok: true, result });
  }

  async #call(req: AcpRequest, conn: Connection): Promise<void> {
    const def = this.#registry.get(req.component!);
    if (!def) {
      conn.send(
        errorEnvelope(req.id, AcpErrorCode.COMPONENT_NOT_FOUND, `component not found: ${req.component}`)
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
          })
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
    };

    const output = await def.handle(req.input, ctx);

    if (isAsyncIterable(output)) {
      if (req.stream !== true) {
        conn.send(
          errorEnvelope(
            req.id,
            AcpErrorCode.STREAM_REQUIRED,
            `component ${def.id} requires stream:true`
          )
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
          })
        );
        return;
      }
    }

    if (req.stream === true) {
      // Non-streaming component called with stream:true: wrap in a single
      // terminated chunk (spec §4.2 — uniform handling, no special cases).
      conn.send({
        acp: this.#options.protocolVersion,
        id: req.id,
        chunk: { seq: 0, end: true, data: output },
      });
      return;
    }

    conn.send({ acp: this.#options.protocolVersion, id: req.id, ok: true, result: output });
  }

  async #sendChunks(
    req: AcpRequest,
    conn: Connection,
    chunks: AsyncIterable<unknown>
  ): Promise<void> {
    let seq = 0;
    for await (const value of chunks) {
      if (isBinaryChunk(value)) {
        conn.send({
          acp: this.#options.protocolVersion,
          id: req.id,
          chunk: { seq, end: false, bin: true, data: value.data },
        });
      } else {
        conn.send({
          acp: this.#options.protocolVersion,
          id: req.id,
          chunk: { seq, end: false, data: value },
        });
      }
      seq++;
    }
    conn.send({
      acp: this.#options.protocolVersion,
      id: req.id,
      chunk: { seq, end: true, data: null },
    });
  }

  #toErrorEnvelope(id: string, error: unknown): AcpServerMessage {
    if (error instanceof AcpError) {
      return errorEnvelope(id, error.code, error.message, error.data);
    }
    const message = error instanceof Error ? error.message : String(error);
    return errorEnvelope(id, AcpErrorCode.COMPONENT_ERROR, `component handler threw: ${message}`);
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

  /**
   * Starts HTTP + WebSocket on the same port (POST http://host:port/acp handles
   * envelopes; ws://host:port/acp upgrades; GET /acp/discover for browsing).
   */
  async listen(options: ListenOptions = {}): Promise<ListenResult> {
    const httpServer = createHttpServer();
    const http = new HttpServerTransport({ server: httpServer });
    const ws = new WsServerTransport({ server: httpServer });
    await http.start(this.handle);
    await ws.start(this.handle);
    await new Promise<void>((resolve) => httpServer.listen(options.port ?? 0, options.host, resolve));
    this.#transports.push(http, ws);
    this.#httpServer = httpServer;
    const addr = httpServer.address();
    return { port: typeof addr === "object" && addr !== null ? addr.port : 0 };
  }

  /** Serves the registry over stdin/stdout (spec §11; used for MCP bridging). */
  async serveStdio(): Promise<void> {
    const stdio = new StdioServerTransport();
    await stdio.start(this.handle);
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
