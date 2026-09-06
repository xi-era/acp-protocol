"""Conformance over WebSocket (spec §10), served on the listen() port."""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from acp.client import AcpClient

from suite import ConformanceContext, make_conformance_server, run_conformance_suite


@pytest.fixture()
def ws_server():
    server = make_conformance_server()
    port = server.listen(port=0)
    yield server, port
    server.shutdown()


class TestWsConformance:
    def test_conformance_suite(self, ws_server):
        server, port = ws_server
        client = AcpClient("ws://127.0.0.1:{}/acp".format(port), timeout_ms=5000)
        client.connect()
        run_conformance_suite(
            ConformanceContext(
                client=client,
                emit=lambda component, data: server.emit(component=component, data=data),
            )
        )
        client.close()

    def test_multiplexes_concurrent_calls_by_id(self, ws_server):
        _, port = ws_server
        client = AcpClient("ws://127.0.0.1:{}/acp".format(port), timeout_ms=5000)
        client.connect()
        with ThreadPoolExecutor(max_workers=3) as pool:
            calls = list(pool.map(lambda m: client.call("conf.echo", {"msg": m}), ["a", "b", "c"]))
        assert calls == [{"msg": "a"}, {"msg": "b"}, {"msg": "c"}]
        client.close()

    def test_ping_roundtrip_over_ws(self, ws_server):
        _, port = ws_server
        client = AcpClient("ws://127.0.0.1:{}/acp".format(port), timeout_ms=5000)
        client.connect()
        reply = client.request({"op": "$ping", "input": {"ts": 7}})
        assert reply["ok"] is True
        assert reply["result"]["ts"] == 7
        client.close()

    def test_context_manager_closes(self, ws_server):
        _, port = ws_server
        with AcpClient("ws://127.0.0.1:{}/acp".format(port), timeout_ms=5000) as client:
            assert len(client.discover()) == 3
