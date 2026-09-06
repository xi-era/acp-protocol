"""模板冒烟测试:三个 Python 模板各起真实服务并走 discover/call 断言。

运行:pytest templates/python -q(需先 pip install -e sdk/python)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "templates" / "python"))

from acp import AcpClient  # noqa: E402


@pytest.fixture()
def http_url():
    """ boots a template server, yields its HTTP endpoint URL. """
    started = {}

    def _boot(module, port: int) -> str:
        server = module.build_server() if hasattr(module, "build_server") else _plain_server(module)
        ws_port = server.listen(port=port)
        started["server"] = server
        started["ws"] = ws_port
        return f"http://127.0.0.1:{server.http_port}/acp"

    yield _boot
    server = started.get("server")
    if server:
        server.shutdown()


def _plain_server(module):
    """Modules exposing ComponentDef constants get a fresh server wrapper."""
    from acp import AcpServer
    from acp.component import ComponentDef

    server = AcpServer(name="template-test")
    for value in vars(module).values():
        if isinstance(value, ComponentDef):
            server.register(value)
    return server


def test_sqlite_query_template(http_url):
    import sqlite_query

    url = http_url(sqlite_query, 8631)
    client = AcpClient(url, timeout_ms=10_000)
    try:
        result = client.call("db.query", {"sql": "SELECT title FROM books ORDER BY year"})
        assert result["count"] == 2
        assert result["rows"][0]["title"] == "The Pragmatic Programmer"
        with pytest.raises(Exception) as exc:
            client.call("db.query", {"sql": "DELETE FROM books"})
        assert "SELECT" in str(exc.value) or "50001" in str(exc.value)
    finally:
        client.close()


def test_mock_iot_template(http_url):
    import mock_iot

    url = http_url(mock_iot, 8632)
    client = AcpClient(url, timeout_ms=10_000)
    try:
        reading = client.call("sensor.iot.temperature", {})
        assert 10 < reading["celsius"] < 30
        chunks = list(client.call_stream("sensor.iot.watch", {"n": 2}))
        assert [c.seq for c in chunks] == [0, 1, 2]
        assert chunks[-1].end is True
    finally:
        client.close()


def test_http_proxy_template_builds_components():
    import http_proxy

    comps = http_proxy.build_components()
    assert [c.id for c in comps] == [
        "http.jsonplaceholder.user",
        "http.jsonplaceholder.post",
    ]
    assert comps[0].input_schema is not None
