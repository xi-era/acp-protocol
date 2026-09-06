"""LangChain integration tests (acp.langchain) — requires langchain-core."""

import json

import pytest

pytest.importorskip("langchain_core")

from langchain_core.tools import ToolException  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from acp.client import AcpClient  # noqa: E402
from acp.component import ComponentDef  # noqa: E402
from acp.langchain import client_to_tools, component_to_tool, json_schema_to_model  # noqa: E402
from acp.server import AcpServer  # noqa: E402
from acp.transports.memory import MemoryClientTransport  # noqa: E402


def make_server() -> AcpServer:
    server = AcpServer(name="lc-node", version="1.0.0")
    server.register(
        ComponentDef(
            id="sensor.temperature",
            name="Temperature Sensor",
            description="Reads current temperature from a virtual sensor",
            input_schema={
                "type": "object",
                "properties": {"unit": {"enum": ["C", "F"]}},
                "required": [],
            },
            tags=["iot", "sensor"],
            handle=lambda input, ctx: {"celsius": 23.5, "unit": (input or {}).get("unit", "C")},
        )
    )
    def failing(input, ctx):
        raise RuntimeError("boom")

    server.register(
        ComponentDef(
            id="conf.failing",
            name="Failing",
            description="Always throws",
            handle=failing,
        )
    )
    return server


def make_client(server: AcpServer) -> AcpClient:
    client = AcpClient(transport=MemoryClientTransport(server=server), timeout_ms=5000)
    client.connect()
    return client


class TestJsonSchemaToModel:
    def test_field_mapping(self):
        model = json_schema_to_model(
            {
                "type": "object",
                "properties": {
                    "s": {"type": "string"},
                    "n": {"type": "number"},
                    "i": {"type": "integer"},
                    "b": {"type": "boolean"},
                    "a": {"type": "array"},
                    "o": {"type": "object"},
                    "e": {"enum": ["C", "F"]},
                },
                "required": ["s", "i"],
            }
        )
        fields = model.model_fields
        assert fields["s"].is_required()
        assert fields["i"].is_required()
        assert not fields["n"].is_required()
        instance = model.model_validate({"s": "x", "i": 3, "e": "F"})
        assert instance.s == "x"
        assert instance.i == 3
        assert instance.e == "F"

    def test_empty_schema(self):
        model = json_schema_to_model(None)
        assert model.model_validate({}) is not None


class TestComponentToTool:
    def test_tool_shape_and_invocation(self):
        server = make_server()
        client = make_client(server)
        tools = client_to_tools(client)
        by_name = {t.name: t for t in tools}
        # "." -> "_" (spec §7.1 MCP bridge mapping)
        assert set(by_name) == {"sensor_temperature", "conf_failing"}

        tool = by_name["sensor_temperature"]
        result = tool.invoke({"unit": "C"})
        assert json.loads(result) == {"celsius": 23.5, "unit": "C"}
        client.close()

    def test_enum_validation(self):
        server = make_server()
        client = make_client(server)
        tool = component_to_tool(client.discover("sensor.temperature")[0], client)
        with pytest.raises(ValidationError):
            tool.invoke({"unit": "X"})
        client.close()

    def test_acp_error_becomes_tool_exception(self):
        server = make_server()
        client = make_client(server)
        tools = client_to_tools(client)
        failing = {t.name: t for t in tools}["conf_failing"]
        with pytest.raises(ToolException):
            failing.invoke({})
        client.close()

    def test_component_to_tool_accepts_dict_descriptor(self):
        server = make_server()
        client = make_client(server)
        descriptor = {
            "id": "sensor.temperature",
            "name": "Temperature Sensor",
            "description": "Reads temperature",
            "inputSchema": {"type": "object", "properties": {"unit": {"enum": ["C", "F"]}}},
        }
        tool = component_to_tool(descriptor, client)
        assert tool.name == "sensor_temperature"
        assert json.loads(tool.invoke({"unit": "F"}))["unit"] == "F"
        client.close()
