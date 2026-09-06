"""v0.2 reserved-op behaviors over the memory transport (spec v0.2 §4.3-4.4,
§6.2). Mirrors packages/acp-sdk-ts/test/v02-core.test.ts.
"""

import time

import pytest

from acp.client import AcpClient
from acp.component import ComponentDef
from acp.errors import AcpError, AcpErrorCode
from acp.server import AcpServer
from acp.transports.memory import MemoryClientTransport
from acp.version import PROTOCOL_VERSION

from suite import make_conformance_server


def make_v02_server() -> AcpServer:
    server = AcpServer(name="v02-node")
    server.register(
        ComponentDef(
            id="v2.sensor",
            name="Sensor",
            description="Emits readings",
            tags=["iot", "sensor"],
            handle=lambda input, ctx: {"celsius": 21},
        )
    )
    return server


def make_client(server: AcpServer) -> AcpClient:
    client = AcpClient(transport=MemoryClientTransport(server=server), timeout_ms=5000)
    client.connect()
    return client


class TestPing:
    def test_ping_roundtrips_with_echoed_ts(self):
        server = make_v02_server()
        transport = MemoryClientTransport(server=server)
        transport.connect()
        before = int(time.time() * 1000)
        reply = transport.request({"acp": PROTOCOL_VERSION, "id": "ka-1", "op": "$ping", "input": {"ts": 12345}})
        after = int(time.time() * 1000)
        assert reply["ok"] is True
        result = reply["result"]
        assert result["ts"] == 12345
        assert before <= result["pong"] <= after
        transport.close()

    def test_ping_without_input(self):
        server = make_v02_server()
        transport = MemoryClientTransport(server=server)
        transport.connect()
        reply = transport.request({"acp": PROTOCOL_VERSION, "id": "ka-2", "op": "$ping"})
        assert reply["ok"] is True
        assert isinstance(reply["result"]["pong"], int)
        assert "ts" not in reply["result"]
        transport.close()


class TestAcpEcho:
    def test_responses_echo_request_acp(self):
        server = make_v02_server()
        transport = MemoryClientTransport(server=server)
        transport.connect()
        assert transport.request({"acp": "0.2", "id": "e1", "op": "discover"})["acp"] == "0.2"
        assert transport.request({"acp": "9.9", "id": "e2", "op": "discover"})["acp"] == "9.9"
        transport.close()


class TestEvents:
    def test_subscribe_component_emit_unsubscribe(self):
        server = make_v02_server()
        client = make_client(server)
        events = []
        sub = client.subscribe(component="v2.sensor", handler=events.append)

        server.emit(component="v2.sensor", data={"celsius": 22})
        assert events == [{"component": "v2.sensor", "tags": ["iot", "sensor"], "data": {"celsius": 22}}]

        sub.unsubscribe()
        server.emit(component="v2.sensor", data={"celsius": 23})
        assert len(events) == 1
        client.close()

    def test_subscribe_by_tags_matches_subset(self):
        server = make_v02_server()
        client = make_client(server)
        events = []
        client.subscribe(tags=["iot"], handler=events.append)
        server.emit(component="v2.sensor", data=1)
        assert len(events) == 1
        server.emit(tags=["other"], data=2)  # no matching subscription
        assert len(events) == 1
        client.close()

    def test_ctx_emit_attributes_event_to_component(self):
        server = AcpServer(name="emit-ctx")

        def emitter(input, ctx):
            ctx.emit(data="hello")
            return "ok"

        server.register(
            ComponentDef(
                id="v2.emitter",
                name="Emitter",
                description="Emits from handler",
                tags=["tag-x"],
                handle=emitter,
            )
        )
        client = make_client(server)
        received = []
        client.subscribe(component="v2.emitter", handler=received.append)
        client.call("v2.emitter", {})
        assert received == [{"component": "v2.emitter", "tags": ["tag-x"], "data": "hello"}]
        client.close()

    def test_unsubscribe_all_with_null_input(self):
        server = make_v02_server()
        client = make_client(server)
        events = []
        client.subscribe(tags=["iot"], handler=events.append)
        client.request({"op": "$unsubscribe", "input": None})
        server.emit(component="v2.sensor", data=1)
        assert events == []
        client.close()

    def test_subscription_limit_42902(self):
        server = AcpServer(name="limit", max_subscriptions_per_conn=2)
        server.register(
            ComponentDef(
                id="v2.sensor",
                name="Sensor",
                description="Emits readings",
                tags=["iot", "sensor"],
                handle=lambda input, ctx: {"celsius": 21},
            )
        )
        client = make_client(server)
        client.subscribe(component="v2.sensor", handler=lambda ev: None)
        client.subscribe(tags=["iot"], handler=lambda ev: None)
        with pytest.raises(AcpError) as exc_info:
            client.subscribe(tags=["sensor"], handler=lambda ev: None)
        assert exc_info.value.code == AcpErrorCode.SUBSCRIPTION_LIMIT
        client.close()


class TestEnvelopeShapes:
    @pytest.mark.parametrize(
        "input",
        [
            {"component": "v2.sensor", "tags": ["iot"]},  # both
            {},  # neither
            None,  # subscribe requires an object
        ],
    )
    def test_malformed_subscribe_rejected_40001(self, input):
        server = make_v02_server()
        transport = MemoryClientTransport(server=server)
        transport.connect()
        reply = transport.request({"acp": PROTOCOL_VERSION, "id": "b1", "op": "$subscribe", "input": input})
        assert reply["ok"] is False
        assert reply["error"]["code"] == AcpErrorCode.INVALID_ENVELOPE
        transport.close()

    def test_unsubscribe_http_50100(self, ):
        server = make_v02_server()
        # A synthetic HTTP-transport connection must get 50100 (spec §4.4).
        from acp.transports.base import Connection

        frames = []
        conn = Connection(meta={"transport": "http"}, send=frames.append, close=lambda: None)
        server.handle({"acp": PROTOCOL_VERSION, "id": "h1", "op": "$subscribe", "input": {"component": "v2.sensor"}}, conn)
        assert frames[0]["error"]["code"] == 50100

    def test_connection_not_registered_50000(self):
        server = make_v02_server()
        from acp.transports.base import Connection

        frames = []
        conn = Connection(meta={"transport": "ws"}, send=frames.append, close=lambda: None)
        server.handle({"acp": PROTOCOL_VERSION, "id": "h2", "op": "$subscribe", "input": {"component": "v2.sensor"}}, conn)
        assert frames[0]["error"]["code"] == AcpErrorCode.INTERNAL_ERROR


class TestStreaming:
    def test_sync_generator_streams(self):
        server = make_conformance_server()
        client = make_client(server)
        chunks = list(client.call_stream("conf.counter", {"n": 2}))
        assert [c.data for c in chunks[:-1]] == [{"i": 0}, {"i": 1}]
        assert chunks[-1].end is True
        client.close()

    def test_async_generator_streams(self):
        server = AcpServer(name="async-node")

        async def agen(input, ctx):
            for i in range((input or {}).get("n", 1)):
                yield {"i": i}

        server.register(
            ComponentDef(id="v2.acounter", name="ACounter", description="Streams n items", stream=True, handle=agen)
        )
        client = make_client(server)
        chunks = list(client.call_stream("v2.acounter", {"n": 3}))
        assert [c.seq for c in chunks] == [0, 1, 2, 3]
        assert [c.data for c in chunks[:3]] == [{"i": 0}, {"i": 1}, {"i": 2}]
        assert chunks[3].end is True
        client.close()

    def test_async_handler_result(self):
        server = AcpServer(name="async-node")

        async def fetch(input, ctx):
            return {"ok": True}

        server.register(ComponentDef(id="v2.afetch", name="AFetch", description="Async result", handle=fetch))
        client = make_client(server)
        assert client.call("v2.afetch", {}) == {"ok": True}
        client.close()

    def test_acp_error_passthrough_from_handler(self):
        from acp.errors import AcpError, CODE_UPSTREAM_ERROR

        server = AcpServer(name="err-node")

        def boom(input, ctx):
            raise AcpError(CODE_UPSTREAM_ERROR, "device offline")

        server.register(ComponentDef(id="v2.boom", name="Boom", description="Raises AcpError", handle=boom))
        client = make_client(server)
        with pytest.raises(AcpError) as exc_info:
            client.call("v2.boom", {})
        assert exc_info.value.code == CODE_UPSTREAM_ERROR
        assert exc_info.value.message == "device offline"
        client.close()

    def test_non_stream_component_with_stream_true_wraps_single_chunk(self):
        server = make_v02_server()
        client = make_client(server)
        chunks = list(client.call_stream("v2.sensor", {}))
        assert len(chunks) == 1
        assert chunks[0].seq == 0
        assert chunks[0].end is True
        assert chunks[0].data == {"celsius": 21}
        client.close()


class TestRegistry:
    def test_rejects_bad_id(self):
        from acp.registry import Registry

        with pytest.raises(ValueError):
            Registry().register(ComponentDef(id="Bad.Id", name="x", description="", handle=lambda i, c: None))

    def test_rejects_duplicate(self):
        from acp.registry import Registry

        reg = Registry()
        reg.register(ComponentDef(id="a.b", name="x", description="", handle=lambda i, c: None))
        with pytest.raises(ValueError):
            reg.register(ComponentDef(id="a.b", name="x", description="", handle=lambda i, c: None))

    def test_rejects_non_callable_handle(self):
        from acp.registry import Registry

        with pytest.raises(ValueError):
            Registry().register(ComponentDef(id="a.b", name="x", description="", handle="nope"))
