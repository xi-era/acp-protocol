/**
 * ACP 0.1 error codes (spec §8) — 5-digit numbers whose first two digits
 * align with HTTP semantic classes.
 */
export const AcpErrorCode = {
  PARSE_ERROR: 40000,
  INVALID_ENVELOPE: 40001,
  UNKNOWN_OP: 40002,
  UNSUPPORTED_VERSION: 40003,
  INVALID_COMPONENT_ID: 40004,
  STREAM_REQUIRED: 40005,
  UNAUTHORIZED: 40100,
  FORBIDDEN: 40101,
  COMPONENT_NOT_FOUND: 40400,
  COMPONENT_UNAVAILABLE: 40401,
  METHOD_NOT_ALLOWED: 40500,
  UNSUPPORTED_MEDIA_TYPE: 41500,
  INVALID_INPUT: 42200,
  INVALID_OUTPUT: 42201,
  RATE_LIMITED: 42900,
  CONCURRENCY_LIMIT: 42901,
  INTERNAL_ERROR: 50000,
  COMPONENT_ERROR: 50001,
  UPSTREAM_ERROR: 50002,
  SHUTTING_DOWN: 50300,
  TIMEOUT: 50400,
  STREAM_ABORTED: 51000,
  CHUNK_OVERFLOW: 51001,
} as const;

export type AcpErrorCodeValue = (typeof AcpErrorCode)[keyof typeof AcpErrorCode];

/** Maps an ACP error code to its HTTP status (spec §8.1; 51xxx maps to 500). */
export function acpCodeToHttpStatus(code: number): number {
  const klass = Math.floor(code / 1000);
  if (klass === 400) return 400;
  if (klass === 401) return 401;
  if (klass === 404) return 404;
  if (klass === 405) return 405;
  if (klass === 415) return 415;
  if (klass === 422) return 422;
  if (klass === 429) return 429;
  if (klass === 503) return 503;
  if (klass === 504) return 504;
  if (klass === 502) return 502;
  return 500;
}

/** Error thrown client-side (and usable inside component handlers) carrying an ACP code. */
export class AcpError extends Error {
  readonly code: number;
  readonly data?: unknown;

  constructor(code: number, message: string, data?: unknown) {
    super(message);
    this.name = "AcpError";
    this.code = code;
    this.data = data;
  }

  static from(errorBody: AcpErrorLike): AcpError {
    return new AcpError(errorBody.code, errorBody.message, errorBody.data);
  }
}

interface AcpErrorLike {
  code: number;
  message: string;
  data?: unknown;
}

/** True for codes in the private/experimental range (spec §8.2). */
export function isPrivateErrorCode(code: number): boolean {
  return code >= 59000 && code <= 59999;
}
