"""ACP envelope and component model types (spec §3, §6, §7).

Wire frames are plain dicts (single source of truth: spec/ACP-0.2-SPEC.md);
these dataclasses are typed conveniences for client-side use.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class AcpRequest:
    """Request envelope — the only message a client sends (spec §3.1)."""

    acp: str = "0.2"
    id: str = ""
    op: str = "discover"
    component: Optional[str] = None
    tags: Optional[List[str]] = None
    input: Any = None
    stream: bool = False
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"acp": self.acp, "id": self.id, "op": self.op}
        if self.component is not None:
            out["component"] = self.component
        if self.tags is not None:
            out["tags"] = self.tags
        if self.input is not None:
            out["input"] = self.input
        if self.stream:
            out["stream"] = True
        if self.meta is not None:
            out["meta"] = self.meta
        return out


@dataclass
class AcpChunk:
    """One streamed chunk (spec §6.1); the frame with ``end=True`` is the
    mandatory terminator."""

    seq: int
    end: bool
    data: Any = None
    bin: Optional[bool] = None


@dataclass
class AcpEvent:
    """Server-pushed event payload (spec v0.2 §6.2)."""

    component: Optional[str] = None
    tags: Optional[List[str]] = None
    data: Any = None
    ts: Optional[int] = None


@dataclass
class AcpServerInfo:
    """Server self-description, part of every discover result (spec §4.1)."""

    name: str
    version: str = "0.0.0"
    protocol: str = "0.2"


@dataclass
class ComponentDescriptor:
    """Component descriptor — the discover reply unit (spec §7.2)."""

    id: str
    name: str
    description: str = ""
    version: str = "0.0.0"
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    stream: bool = False
    tags: Optional[List[str]] = None
    meta: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ComponentDescriptor":
        return cls(
            id=d.get("id", ""),
            name=d.get("name", ""),
            description=d.get("description", ""),
            version=d.get("version", "0.0.0"),
            input_schema=d.get("inputSchema"),
            output_schema=d.get("outputSchema"),
            stream=bool(d.get("stream", False)),
            tags=d.get("tags"),
            meta=d.get("meta"),
        )
