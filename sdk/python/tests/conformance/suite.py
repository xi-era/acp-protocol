"""Shared conformance fixtures: the same set of behaviors must hold over
every transport (executable annotation of spec/ACP-0.2-SPEC.md §3-6, §12).

Mirrors packages/acp-sdk-ts/test/conformance/suite.ts.
"""

import time
from typing import Any, Callable, Optional

import pytest

from acp.client import AcpClient
from acp.component import ComponentDef
from acp.errors import AcpError, AcpErrorCode
from acp.server import AcpServer


def make_conformance_server() -> AcpServer:
    """Server with the three conformance components (conf.echo/counter/failing)."""
    server = AcpServer(name="conf-node", version="1.0.0")

    def echo(input: Any, ctx: Any) -> dict:
        return {"msg": (input or {}).get("msg", "")}

    def counter(input: Any, ctx: Any):
        n = (input or {}).get("n", 0)
        for i in range(n):
            yield {"i": i}

    def failing(input: Any, ctx: Any) -> None:
        raise RuntimeError("conf boom")

    server.register(
        ComponentDef(
            id="conf.echo",
            name="Echo",
            description="Echoes msg",
            input_schema={"type": "object", "properties": {"msg": {"type": "string"}}},
            tags=["conf"],
            handle=echo,
        )
    )
    server.register(
        ComponentDef(
            id="conf.counter",
            name="Counter",
            description="Streams n items",
            stream=True,
            handle=counter,
        )
    )
    server.register(
        ComponentDef(id="conf.failing", name="Failing", description="Always throws", handle=failing)
    )
    return server


class ConformanceContext:
    """Per-transport inputs for :func:`run_conformance_suite`."""

    def __init__(
        self,
        client: AcpClient,
        protocol_version: str = "0.2",
        send_raw: Optional[Callable[[str], dict]] = None,
        emit: Optional[Callable[[str, Any], None]] = None,
        event_component: str = "conf.echo",
    ) -> None:
        self.client = client
        self.protocol_version = protocol_version
        self.send_raw = send_raw
        self.emit = emit
        self.event_component = event_component


def run_conformance_suite(ctx: ConformanceContext) -> None:
    """Runs the conformance behaviors against ``ctx.client`` (see suite.ts)."""
    client = ctx.client

    # spec §4.1 discover: fixed result shape with server info
    reply = client.request({"op": "discover"})
    assert reply["ok"] is True
    result = reply["result"]
    assert result["server"]["protocol"] == ctx.protocol_version
    ids = [c["id"] for c in result["components"]]
    for expected in ("conf.echo", "conf.counter", "conf.failing"):
        assert expected in ids

    # spec §4.1 single lookup: still an array
    assert len(client.discover("conf.echo")) == 1
    assert client.discover("absent.component") == []

    # spec §5.1 bare result
    assert client.call("conf.echo", {"msg": "ping"}) == {"msg": "ping"}

    # spec §8 42200 INVALID_INPUT
    with pytest.raises(AcpError) as exc_info:
        client.call("conf.echo", {"msg": 42})
    assert exc_info.value.code == AcpErrorCode.INVALID_INPUT

    # spec §8 40400 COMPONENT_NOT_FOUND
    with pytest.raises(AcpError) as exc_info:
        client.call("absent.component", {})
    assert exc_info.value.code == AcpErrorCode.COMPONENT_NOT_FOUND

    # spec §6 streaming: seq order + terminal end frame
    chunks = list(client.call_stream("conf.counter", {"n": 3}))
    assert [c.seq for c in chunks] == [0, 1, 2, 3]
    assert chunks[3].end is True
    assert [c.data for c in chunks[:3]] == [{"i": 0}, {"i": 1}, {"i": 2}]

    # spec §4.2 40005 STREAM_REQUIRED
    with pytest.raises(AcpError) as exc_info:
        client.call("conf.counter", {"n": 1})
    assert exc_info.value.code == AcpErrorCode.STREAM_REQUIRED

    # spec §8 50001 COMPONENT_ERROR
    with pytest.raises(AcpError) as exc_info:
        client.call("conf.failing", {})
    assert exc_info.value.code == AcpErrorCode.COMPONENT_ERROR

    # spec §12 40003 version negotiation
    bad_version = client.request({"acp": "9.9", "op": "discover"})
    assert bad_version["ok"] is False
    assert bad_version["error"]["code"] == AcpErrorCode.UNSUPPORTED_VERSION
    assert ctx.protocol_version in bad_version["error"]["data"]["supported"]

    # spec v0.2 §5.3: responses echo the request's acp value
    echo_reply = client.request({"op": "discover"})
    assert echo_reply.get("acp", "absent") == "0.2"

    # spec v0.2 §4.3: $ping roundtrip
    ping = client.request({"op": "$ping", "input": {"ts": 42}})
    assert ping["ok"] is True
    pong = ping["result"]
    assert pong["ts"] == 42
    assert isinstance(pong["pong"], int)

    # spec v0.2 §4.4/§6.2: subscribe -> emit x2 -> event -> unsubscribe
    if ctx.emit is not None:
        component = ctx.event_component
        received = []
        sub = client.subscribe(component=component, handler=lambda ev: received.append(ev["data"]))
        ctx.emit(component, "evt-1")
        ctx.emit(component, "evt-2")
        time.sleep(0.1)  # event delivery is async on networked transports
        assert received == ["evt-1", "evt-2"]
        sub.unsubscribe()
        ctx.emit(component, "evt-3")
        time.sleep(0.1)
        assert received == ["evt-1", "evt-2"]

    # spec §13 meta ignored
    with_meta = client.request(
        {
            "op": "call",
            "component": "conf.echo",
            "input": {"msg": "meta"},
            "meta": {"auth": "bearer x", "traceId": "tr-1", "vendor": "ignored"},
        }
    )
    assert with_meta["ok"] is True

    # spec §3.2 40000 parse error (transport-dependent hook)
    if ctx.send_raw is not None:
        parse_error = ctx.send_raw("this is not json")
        assert parse_error["ok"] is False
        assert parse_error["error"]["code"] == AcpErrorCode.PARSE_ERROR
