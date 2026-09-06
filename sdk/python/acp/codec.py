"""Envelope codec helpers (spec §3, §12): validation order, version
comparison, component id grammar, error envelope construction.

Mirrors packages/acp-sdk-ts/src/codec.ts.
"""

import re
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .errors import (
    CODE_INVALID_COMPONENT_ID,
    CODE_INVALID_ENVELOPE,
    CODE_UNKNOWN_OP,
)

PROTOCOL_VERSION = "0.2"

# Reserved `$`-prefixed ops (spec v0.2 §4.3-4.4).
RESERVED_OPS = ("$ping", "$subscribe", "$unsubscribe")

STANDARD_OPS = ("discover", "call")

COMPONENT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$")

# Sentinel distinguishing "data omitted" from "data is None" (spec §5.2).
_UNSET = object()


def is_reserved_op(op: str) -> bool:
    return op in RESERVED_OPS


def is_valid_component_id(value: Any) -> bool:
    """Component id grammar (spec §7.1):
    ``segment ( "." segment ){1,3}`` with ``segment = [a-z][a-z0-9-]{0,62}``."""
    return isinstance(value, str) and COMPONENT_ID_RE.match(value) is not None


def component_id_to_tool_name(component_id: str) -> str:
    """Maps a component id to an MCP/OpenAI tool name ("." -> "_")."""
    return component_id.replace(".", "_")


def tool_name_to_component_id(name: str) -> str:
    """Inverse of :func:`component_id_to_tool_name`."""
    return name.replace("_", ".")


def parse_version(version: str) -> Optional[Tuple[int, int]]:
    """Parses "major.minor"; returns None when malformed."""
    if not isinstance(version, str):
        return None
    m = re.match(r"^(\d+)\.(\d+)$", version)
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2))


def is_version_supported(client_version: str, server_version: str) -> bool:
    """Server supports the client when major is equal and server minor >=
    client minor (spec §12.1)."""
    c = parse_version(client_version)
    s = parse_version(server_version)
    if c is None or s is None:
        return False
    return c[0] == s[0] and s[1] >= c[1]


def error_envelope(
    id: Optional[str],
    code: int,
    message: str,
    data: Any = _UNSET,
    acp: str = PROTOCOL_VERSION,
) -> dict:
    """Builds an error envelope; ``id`` is None when the request id could not
    be read. ``acp`` echoes the request's declared version (spec v0.2 §5.3)."""
    error: dict = {"code": code, "message": message}
    if data is not _UNSET:
        error["data"] = data
    return {"acp": acp, "id": id, "ok": False, "error": error}


def validate_reserved_input(op: str, input: Any) -> Optional[str]:
    """Validates reserved-op input shapes (spec v0.2 §4.3-4.4); None when valid."""
    if op == "$ping":
        return None  # input optional; any shape accepted (ts echoed if present)
    if op in ("$subscribe", "$unsubscribe"):
        # $unsubscribe with absent/null input = unsubscribe all (valid).
        if op == "$unsubscribe" and input is None:
            return None
        if not isinstance(input, dict):
            return "op={} requires an input object with exactly one of component/tags".format(op)
        has_component = "component" in input
        has_tags = "tags" in input
        if has_component == has_tags:
            return "op={} requires exactly one of component/tags".format(op)
        if has_component and not is_valid_component_id(input["component"]):
            return "op={}: invalid component id".format(op)
        if has_tags:
            tags = input["tags"]
            if (
                not isinstance(tags, list)
                or len(tags) == 0
                or any(not isinstance(t, str) for t in tags)
            ):
                return "op={}: tags must be a non-empty string array".format(op)
    return None


@dataclass
class EnvelopeValidation:
    """Result of :func:`validate_envelope` — mirrors the TS discriminated union."""

    ok: bool
    request: Optional[dict] = None
    id: Optional[str] = None
    code: Optional[int] = None
    message: Optional[str] = None
    data: Any = _UNSET


def validate_envelope(raw: Any) -> EnvelopeValidation:
    """Validates a parsed request object following the spec §3.2 check order:
    parse (done by transport) -> envelope -> op -> component -> reserved input."""
    rid = raw.get("id") if isinstance(raw, dict) else None
    if not isinstance(rid, str):
        rid = None

    def fail(code: int, message: str, data: Any = _UNSET) -> EnvelopeValidation:
        return EnvelopeValidation(ok=False, id=rid, code=code, message=message, data=data)

    if not isinstance(raw, dict):
        return fail(CODE_INVALID_ENVELOPE, "request must be a JSON object")

    if not isinstance(raw.get("acp"), str):
        return fail(CODE_INVALID_ENVELOPE, "missing required field: acp")
    if rid is None:
        return fail(CODE_INVALID_ENVELOPE, "missing required field: id")
    if not isinstance(raw.get("op"), str):
        return fail(CODE_INVALID_ENVELOPE, "missing required field: op")

    op = raw["op"]
    if op != "discover" and op != "call" and not is_reserved_op(op):
        return fail(CODE_UNKNOWN_OP, "unknown op: {}".format(op))
    if is_reserved_op(op):
        err = validate_reserved_input(op, raw.get("input"))
        if err is not None:
            return fail(CODE_INVALID_ENVELOPE, err)
    if "component" in raw and raw["component"] is not None:
        if not is_valid_component_id(raw["component"]):
            return fail(
                CODE_INVALID_COMPONENT_ID,
                "invalid component id: {}".format(raw["component"]),
            )
    if op == "call" and raw.get("component") is None:
        return fail(CODE_INVALID_ENVELOPE, "op=call requires component")
    if "tags" in raw and not isinstance(raw["tags"], list):
        return fail(CODE_INVALID_ENVELOPE, "tags must be an array")
    if "stream" in raw and not isinstance(raw["stream"], bool):
        return fail(CODE_INVALID_ENVELOPE, "stream must be a boolean")

    return EnvelopeValidation(ok=True, request=raw)
