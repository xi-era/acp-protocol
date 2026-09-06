"""Conformance over HTTP (spec §9).

The HTTP transport lives on ``server.http_port`` (WS port + 1) — see
acp/server.py for why Python's websockets cannot serve POST on the same port.
"""

import json
import urllib.error
import urllib.request

import pytest

from acp.client import AcpClient
from acp.errors import AcpError

from suite import ConformanceContext, make_conformance_server, run_conformance_suite


@pytest.fixture()
def http_port():
    server = make_conformance_server()
    server.listen(port=0)  # returns the WS port; HTTP is on server.http_port
    yield server.http_port
    server.shutdown()


def _post_raw(port: int, text: str) -> dict:
    request = urllib.request.Request(
        "http://127.0.0.1:{}/acp".format(port),
        data=text.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as e:  # ACP error envelopes keep their status
        response = e
    return json.loads(response.read().decode("utf-8"))


class TestHttpConformance:
    def test_conformance_suite(self, http_port):
        client = AcpClient("http://127.0.0.1:{}/acp".format(http_port), timeout_ms=5000)
        run_conformance_suite(
            ConformanceContext(
                client=client,
                send_raw=lambda text: _post_raw(http_port, text),
                # HTTP is connectionless: no emit hook (spec v0.2 §4.4).
            )
        )

    def test_subscribe_rejected_with_50100(self, http_port):
        client = AcpClient("http://127.0.0.1:{}/acp".format(http_port), timeout_ms=5000)
        with pytest.raises(AcpError) as exc_info:
            client.subscribe(component="conf.echo", handler=lambda ev: None)
        assert exc_info.value.code == 50100

    def test_get_acp_discover(self, http_port):
        response = urllib.request.urlopen("http://127.0.0.1:{}/acp/discover".format(http_port), timeout=5)
        assert response.status == 200
        body = json.loads(response.read().decode("utf-8"))
        assert body["server"]["name"] == "conf-node"
        assert len(body["components"]) == 3

    def test_get_acp_health(self, http_port):
        response = urllib.request.urlopen("http://127.0.0.1:{}/acp/health".format(http_port), timeout=5)
        assert json.loads(response.read().decode("utf-8")) == {"ok": True}

    def test_error_codes_map_to_http_status(self, http_port):
        body = _post_raw(
            http_port,
            json.dumps({"acp": "0.1", "id": "h1", "op": "call", "component": "absent.component", "input": {}}),
        )
        request = urllib.request.Request(
            "http://127.0.0.1:{}/acp".format(http_port),
            data=json.dumps({"acp": "0.1", "id": "h1", "op": "call", "component": "absent.component", "input": {}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(request, timeout=5)
        assert exc_info.value.code == 404
        assert body["ok"] is False
        assert body["error"]["code"] == 40400

    def test_unknown_path_404(self, http_port):
        request = urllib.request.Request("http://127.0.0.1:{}/nope".format(http_port))
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(request, timeout=5)
        assert exc_info.value.code == 404

    def test_http_error_envelope_raises_client_error(self, http_port):
        client = AcpClient("http://127.0.0.1:{}/acp".format(http_port), timeout_ms=5000)
        with pytest.raises(AcpError) as exc_info:
            client.call("conf.echo", {"msg": 42})
        assert exc_info.value.code == 42200
