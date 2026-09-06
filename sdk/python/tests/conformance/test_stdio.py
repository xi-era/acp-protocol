"""Conformance over Stdio, using in-process socket pairs (spec §11).

The server transport is wired manually via ``server.connection_lifecycle``.
"""

import json
import socket

import pytest

from acp.client import AcpClient
from acp.transports.stdio import StdioClientTransport, StdioServerTransport

from suite import ConformanceContext, make_conformance_server, run_conformance_suite


@pytest.fixture()
def stdio_pair():
    server = make_conformance_server()
    client_sock, server_sock = socket.socketpair()
    server_input = server_sock.makefile("rb")   # server reads requests
    server_output = server_sock.makefile("wb")  # server writes replies
    client_input = client_sock.makefile("rb")   # client reads replies/events
    client_output = client_sock.makefile("wb")  # client writes requests
    StdioServerTransport(input=server_input, output=server_output).start(
        server.handle, server.connection_lifecycle
    )
    client = AcpClient(
        transport=StdioClientTransport(input=client_input, output=client_output),
        timeout_ms=5000,
    )
    client.connect()
    yield server, client
    client.close()
    server.shutdown()
    client_sock.close()
    server_sock.close()


class TestStdioConformance:
    def test_conformance_suite(self, stdio_pair):
        server, client = stdio_pair
        run_conformance_suite(
            ConformanceContext(
                client=client,
                emit=lambda component, data: server.emit(component=component, data=data),
                send_raw=lambda text: _send_raw_line(server, text),
            )
        )

    def test_ping_over_stdio(self, stdio_pair):
        _, client = stdio_pair
        reply = client.request({"op": "$ping", "input": {"ts": 99}})
        assert reply["ok"] is True
        assert reply["result"]["ts"] == 99


def _send_raw_line(server, text: str) -> dict:
    """Sends a non-JSON line on a one-off stdio pair to observe the PARSE_ERROR
    frame (id: null) that pending-by-id routing would drop."""
    client_sock, server_sock = socket.socketpair()
    server_input = server_sock.makefile("rb")
    server_output = server_sock.makefile("wb")
    read_side = client_sock.makefile("rb")
    StdioServerTransport(input=server_input, output=server_output).start(
        server.handle, server.connection_lifecycle
    )
    client_sock.sendall((text + "\n").encode("utf-8"))
    line = read_side.readline()
    client_sock.close()
    server_sock.close()
    return json.loads(line.decode("utf-8"))
