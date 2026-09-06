"""LangChain integration (spec appendix A/B): exposes ACP components as
LangChain tools via ``langchain-core``.

Requires the optional dependency ``langchain-core`` (``pip install
acp-protocol-sdk[langchain]``). Component ids are mapped to tool names by
``"." -> "_"`` (lossless and reversible, spec §7.1 / appendix A).
"""

import json
from typing import Any, Dict, List, Optional, Type

from .codec import component_id_to_tool_name
from .errors import AcpError
from .types import ComponentDescriptor

try:  # guard the optional dependency with a clear error message
    from langchain_core.tools import StructuredTool, ToolException
    from pydantic import BaseModel, create_model

    _LANGCHAIN_AVAILABLE = True
except ImportError as _exc:  # pragma: no cover - exercised only without langchain
    _LANGCHAIN_AVAILABLE = False
    _IMPORT_ERROR = _exc


def _require_langchain() -> None:
    if not _LANGCHAIN_AVAILABLE:
        raise ImportError(
            "acp.langchain requires langchain-core; install it with: "
            'pip install "acp-protocol-sdk[langchain]"'
        )


def _map_schema_type(prop: dict) -> Type:
    """Maps one JSON Schema (draft-07) property to a Python type."""
    if "enum" in prop:
        from typing import Literal

        values = tuple(prop["enum"])
        if values:
            return Literal[values]  # type: ignore[valid-type]
        return str
    json_type = prop.get("type")
    if json_type == "string":
        return str
    if json_type == "number":
        return float
    if json_type == "integer":
        return int
    if json_type == "boolean":
        return bool
    if json_type == "array":
        return list
    # "object" and anything unmapped fall back to dict.
    return dict


def json_schema_to_model(schema: Optional[dict], model_name: str = "AcpToolArgs") -> Type["BaseModel"]:
    """Converts a JSON Schema (draft-07) object schema into a pydantic model.

    Field mapping: string->str, number->float, integer->int, boolean->bool,
    enum->Literal, array->list, object/unmapped->dict. Required properties
    become mandatory fields; the rest are Optional with default None.
    """
    _require_langchain()
    properties = (schema or {}).get("properties") or {}
    required = set((schema or {}).get("required") or [])
    fields: Dict[str, Any] = {}
    for prop_name, prop in properties.items():
        annotation = _map_schema_type(prop if isinstance(prop, dict) else {})
        if prop_name in required:
            fields[prop_name] = (annotation, ...)
        else:
            from typing import Optional as _Optional

            fields[prop_name] = (_Optional[annotation], None)
    return create_model(model_name, **fields)


def component_to_tool(descriptor: Any, client: Any) -> "StructuredTool":
    """Wraps one ACP component as a LangChain ``StructuredTool``.

    ``descriptor`` may be a :class:`~acp.types.ComponentDescriptor` or a plain
    discover descriptor dict. Tool results are JSON-encoded component outputs;
    ACP errors become ``ToolException``.
    """
    _require_langchain()
    if isinstance(descriptor, dict):
        descriptor = ComponentDescriptor.from_dict(descriptor)
    component_id = descriptor.id
    args_schema = json_schema_to_model(
        descriptor.input_schema if isinstance(descriptor.input_schema, dict) else None,
        model_name="{}_Args".format(component_id.replace(".", "_")),
    )

    def _call(**kwargs: Any) -> str:
        try:
            result = client.call(component_id, kwargs)
            return json.dumps(result, ensure_ascii=False)
        except AcpError as e:
            raise ToolException("[{}] {}".format(e.code, e.message)) from e

    return StructuredTool.from_function(
        name=component_id_to_tool_name(component_id),
        description=descriptor.description or component_id,
        args_schema=args_schema,
        func=_call,
    )


def client_to_tools(client: Any, components: Optional[List[Any]] = None) -> List["StructuredTool"]:
    """Discovers components (unless given) and converts all of them to tools."""
    _require_langchain()
    descriptors = components if components is not None else client.discover()
    return [component_to_tool(d, client) for d in descriptors]
