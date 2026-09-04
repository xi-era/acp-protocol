/**
 * Component definition (spec §7): a named unit with draft-07 schemas and a handler.
 * A handler that returns an AsyncIterable is a streaming component; each yielded
 * value becomes one chunk frame.
 */
import type { AcpRequest } from "./types.js";
import type { ComponentDescriptor } from "./types.js";
import type { Connection } from "./transport.js";

/** Per-call context handed to component handlers. */
export interface CallContext {
  /** The connection the request arrived on. */
  conn: Connection;
  /** The raw request envelope (meta included; v0.1 servers ignore meta). */
  request: AcpRequest;
  /** Shorthand for request.meta. */
  meta?: AcpRequest["meta"];
  /** Aborted when the underlying connection closes or the client cancels. */
  signal: AbortSignal;
}

export type ComponentHandle<I, O> = (
  input: I,
  ctx: CallContext
) => O | Promise<O> | AsyncIterable<unknown>;

export interface ComponentDef<I = unknown, O = unknown> {
  id: string;
  name: string;
  description: string;
  version?: string;
  inputSchema?: object;
  outputSchema?: object;
  stream?: boolean;
  tags?: string[];
  meta?: Record<string, unknown>;
  handle: ComponentHandle<I, O>;
}

/** Identity helper that gives full type inference for input/output in user code. */
export function defineComponent<I = unknown, O = unknown>(def: ComponentDef<I, O>): ComponentDef<I, O> {
  return def;
}

export function toDescriptor(def: ComponentDef): ComponentDescriptor {
  return {
    id: def.id,
    name: def.name,
    description: def.description,
    version: def.version ?? "0.0.0",
    ...(def.inputSchema !== undefined ? { inputSchema: def.inputSchema } : {}),
    ...(def.outputSchema !== undefined ? { outputSchema: def.outputSchema } : {}),
    stream: def.stream ?? false,
    ...(def.tags !== undefined ? { tags: def.tags } : {}),
    ...(def.meta !== undefined ? { meta: def.meta } : {}),
  };
}
