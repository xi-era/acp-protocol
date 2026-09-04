/**
 * Envelope codec helpers (spec §3, §12): validation order, version comparison,
 * component id grammar, error envelope construction.
 */
import type { AcpErrorFrame, AcpRequest } from "./types.js";
import { AcpErrorCode } from "./errors.js";

export const PROTOCOL_VERSION = "0.1";

const COMPONENT_ID_RE = /^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$/;

export function isValidComponentId(id: unknown): id is string {
  return typeof id === "string" && COMPONENT_ID_RE.test(id);
}

/** Maps a component id to an MCP/OpenAI tool name ("." -> "_", lossless & reversible). */
export function componentIdToToolName(id: string): string {
  return id.replaceAll(".", "_");
}

/** Inverse of {@link componentIdToToolName}. */
export function toolNameToComponentId(name: string): string {
  return name.replaceAll("_", ".");
}

/** Parses "major.minor"; returns null when malformed. */
export function parseVersion(v: string): [number, number] | null {
  const m = /^(\d+)\.(\d+)$/.exec(v);
  if (!m) return null;
  return [Number(m[1]), Number(m[2])];
}

/** Server supports the client when major is equal and server minor >= client minor. */
export function isVersionSupported(clientVersion: string, serverVersion: string): boolean {
  const c = parseVersion(clientVersion);
  const s = parseVersion(serverVersion);
  if (!c || !s) return false;
  return c[0] === s[0] && s[1] >= c[1];
}

/** Builds an error envelope; `id` is null when the request id could not be read. */
export function errorEnvelope(
  id: string | null,
  code: number,
  message: string,
  data?: unknown
): AcpErrorFrame {
  const error: AcpErrorFrame["error"] = { code, message };
  if (data !== undefined) error.data = data;
  return { acp: PROTOCOL_VERSION, id, ok: false, error };
}

export type EnvelopeValidation =
  | { ok: true; request: AcpRequest }
  | { ok: false; id: string | null; code: number; message: string; data?: unknown };

/**
 * Validates a parsed request object following the spec §3.2 check order:
 * parse (done by transport) -> envelope -> version -> op -> component.
 */
export function validateEnvelope(raw: unknown): EnvelopeValidation {
  const id = typeof (raw as AcpRequest | null)?.id === "string" ? (raw as AcpRequest).id : null;

  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "request must be a JSON object" };
  }

  const req = raw as Record<string, unknown>;

  if (typeof req.acp !== "string") {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "missing required field: acp" };
  }
  if (id === null) {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "missing required field: id" };
  }
  if (typeof req.op !== "string") {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "missing required field: op" };
  }
  if (req.op !== "discover" && req.op !== "call") {
    return { ok: false, id, code: AcpErrorCode.UNKNOWN_OP, message: `unknown op: ${req.op}` };
  }
  if (req.component !== undefined && !isValidComponentId(req.component)) {
    return {
      ok: false,
      id,
      code: AcpErrorCode.INVALID_COMPONENT_ID,
      message: `invalid component id: ${String(req.component)}`,
    };
  }
  if (req.op === "call" && req.component === undefined) {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "op=call requires component" };
  }
  if (req.tags !== undefined && !Array.isArray(req.tags)) {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "tags must be an array" };
  }
  if (req.stream !== undefined && typeof req.stream !== "boolean") {
    return { ok: false, id, code: AcpErrorCode.INVALID_ENVELOPE, message: "stream must be a boolean" };
  }

  return { ok: true, request: raw as AcpRequest };
}
