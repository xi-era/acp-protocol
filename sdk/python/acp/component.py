"""Component definition (spec §7): a named unit with draft-07 schemas and a
handler.

A handler whose return value is a generator (sync or async) is a streaming
component; each yielded value becomes one chunk frame. A handler returning a
plain value (directly or via await) answers with a single result.
"""

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .transports.base import Connection


@dataclass
class CallContext:
    """Per-call context handed to component handlers (spec v0.2 §6.2)."""

    #: The connection the request arrived on.
    conn: Connection
    #: The raw request envelope (meta included; servers ignore meta semantics).
    request: dict
    #: Shorthand for request.meta.
    meta: Optional[Dict[str, Any]] = None
    #: Aborted when the underlying connection closes or the client cancels.
    signal: Optional[asyncio.Event] = None
    #: Push an $event to all subscribers of this component (spec v0.2 §6.2).
    #: Called as ``ctx.emit(data=..., tags=..., ts=...)`` (tags default to the
    #: emitting component's descriptor tags).
    emit: Optional[Callable[..., None]] = None


#: Handler signature: ``handle(input, ctx)``. May be a plain function, an
#: ``async def`` function, a generator function, or an async generator function.
ComponentHandle = Callable[[Any, CallContext], Any]


@dataclass
class ComponentDef:
    """Component definition (spec §7)."""

    id: str
    name: str
    description: str = ""
    version: str = "0.0.0"
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    stream: bool = False
    tags: Optional[List[str]] = None
    meta: Optional[Dict[str, Any]] = None
    handle: Optional[ComponentHandle] = None


def is_stream_result(result: Any) -> bool:
    """True when a handler result is a chunk stream (sync or async generator)."""
    return inspect.isgenerator(result) or inspect.isasyncgen(result)


def to_descriptor(def_: ComponentDef) -> dict:
    """Projects a ComponentDef to its discover descriptor shape (spec §7.2)."""
    d: Dict[str, Any] = {
        "id": def_.id,
        "name": def_.name,
        "description": def_.description,
        "version": def_.version or "0.0.0",
    }
    if def_.input_schema is not None:
        d["inputSchema"] = def_.input_schema
    if def_.output_schema is not None:
        d["outputSchema"] = def_.output_schema
    d["stream"] = bool(def_.stream)
    if def_.tags is not None:
        d["tags"] = list(def_.tags)
    if def_.meta is not None:
        d["meta"] = def_.meta
    return d
