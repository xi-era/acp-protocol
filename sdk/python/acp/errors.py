"""ACP error codes (spec §8) — 5-digit numbers whose first two digits align
with HTTP semantic classes.

Mirrors packages/acp-sdk-ts/src/errors.ts, including the v0.2 additions
SUBSCRIPTION_LIMIT (42902) and EVENT_UNSUPPORTED (50100).
"""

from typing import Any, Optional

# --- Full error-code table (spec §8.1) ---------------------------------------

CODE_PARSE_ERROR = 40000
CODE_INVALID_ENVELOPE = 40001
CODE_UNKNOWN_OP = 40002
CODE_UNSUPPORTED_VERSION = 40003
CODE_INVALID_COMPONENT_ID = 40004
CODE_STREAM_REQUIRED = 40005
CODE_UNAUTHORIZED = 40100
CODE_FORBIDDEN = 40101
CODE_COMPONENT_NOT_FOUND = 40400
CODE_COMPONENT_UNAVAILABLE = 40401
CODE_METHOD_NOT_ALLOWED = 40500
CODE_UNSUPPORTED_MEDIA_TYPE = 41500
CODE_INVALID_INPUT = 42200
CODE_INVALID_OUTPUT = 42201
CODE_RATE_LIMITED = 42900
CODE_CONCURRENCY_LIMIT = 42901
CODE_SUBSCRIPTION_LIMIT = 42902
CODE_INTERNAL_ERROR = 50000
CODE_COMPONENT_ERROR = 50001
CODE_UPSTREAM_ERROR = 50002
CODE_EVENT_UNSUPPORTED = 50100
CODE_SHUTTING_DOWN = 50300
CODE_TIMEOUT = 50400
CODE_STREAM_ABORTED = 51000
CODE_CHUNK_OVERFLOW = 51001


class AcpErrorCode:
    """Namespace mirroring the TypeScript ``AcpErrorCode`` const object."""

    PARSE_ERROR = CODE_PARSE_ERROR
    INVALID_ENVELOPE = CODE_INVALID_ENVELOPE
    UNKNOWN_OP = CODE_UNKNOWN_OP
    UNSUPPORTED_VERSION = CODE_UNSUPPORTED_VERSION
    INVALID_COMPONENT_ID = CODE_INVALID_COMPONENT_ID
    STREAM_REQUIRED = CODE_STREAM_REQUIRED
    UNAUTHORIZED = CODE_UNAUTHORIZED
    FORBIDDEN = CODE_FORBIDDEN
    COMPONENT_NOT_FOUND = CODE_COMPONENT_NOT_FOUND
    COMPONENT_UNAVAILABLE = CODE_COMPONENT_UNAVAILABLE
    METHOD_NOT_ALLOWED = CODE_METHOD_NOT_ALLOWED
    UNSUPPORTED_MEDIA_TYPE = CODE_UNSUPPORTED_MEDIA_TYPE
    INVALID_INPUT = CODE_INVALID_INPUT
    INVALID_OUTPUT = CODE_INVALID_OUTPUT
    RATE_LIMITED = CODE_RATE_LIMITED
    CONCURRENCY_LIMIT = CODE_CONCURRENCY_LIMIT
    SUBSCRIPTION_LIMIT = CODE_SUBSCRIPTION_LIMIT
    INTERNAL_ERROR = CODE_INTERNAL_ERROR
    COMPONENT_ERROR = CODE_COMPONENT_ERROR
    UPSTREAM_ERROR = CODE_UPSTREAM_ERROR
    EVENT_UNSUPPORTED = CODE_EVENT_UNSUPPORTED
    SHUTTING_DOWN = CODE_SHUTTING_DOWN
    TIMEOUT = CODE_TIMEOUT
    STREAM_ABORTED = CODE_STREAM_ABORTED
    CHUNK_OVERFLOW = CODE_CHUNK_OVERFLOW


def acp_code_to_http_status(code: int) -> int:
    """Maps an ACP error code to its HTTP status (spec §8.1).

    ``floor(code / 100)`` except UPSTREAM_ERROR (50002 -> 502) and the
    51xxx stream segment (-> 500).
    """
    http = code // 100
    if 510 <= http <= 519:  # stream segment
        return 500
    if code == CODE_UPSTREAM_ERROR:
        return 502
    if 400 <= http <= 599:
        return http
    return 500


class AcpError(Exception):
    """Error carrying an ACP code: raised client-side (and usable inside
    component handlers to control the response code)."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    @classmethod
    def from_body(cls, error_body: Any) -> "AcpError":
        """Builds an AcpError from a wire error object (``{code, message, data?}``)."""
        body = error_body or {}
        return cls(
            int(body.get("code", CODE_INTERNAL_ERROR)),
            str(body.get("message", "unknown error")),
            body.get("data"),
        )

    def __str__(self) -> str:
        return "AcpError {}: {}".format(self.code, self.message)


def is_private_error_code(code: int) -> bool:
    """True for codes in the private/experimental range (spec §8.2)."""
    return 59000 <= code <= 59999
