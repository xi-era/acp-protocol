"""Conformance over the in-process memory transport (no network loopback)."""

from acp.client import AcpClient
from acp.transports.memory import MemoryClientTransport

from suite import ConformanceContext, make_conformance_server, run_conformance_suite


def make_memory_client(server):
    client = AcpClient(transport=MemoryClientTransport(server=server), timeout_ms=5000)
    client.connect()
    return client


class TestMemoryConformance:
    def test_conformance_suite(self):
        server = make_conformance_server()
        client = make_memory_client(server)
        run_conformance_suite(
            ConformanceContext(
                client=client,
                emit=lambda component, data: server.emit(component=component, data=data),
            )
        )
        client.close()

    def test_events_supported(self):
        server = make_conformance_server()
        transport = MemoryClientTransport(server=server)
        assert transport.events_supported() is True
