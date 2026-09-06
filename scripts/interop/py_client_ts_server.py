#!/usr/bin/env python3
"""Interop: Python AcpClient against a TypeScript ACP server.

Boots scripts/interop/ts-server.mjs (node), then asserts discover / call /
stream / $ping over HTTP from the Python SDK. Exit code 0 = pass.
"""
from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PORT = 8611


def wait_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"port {port} never became ready")


def main() -> int:
    proc = subprocess.Popen(
        ["node", str(REPO / "scripts/interop/ts-server.mjs"), str(PORT)],
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_port(PORT)
        from acp import AcpClient

        client = AcpClient(f"http://127.0.0.1:{PORT}/acp", timeout_ms=10_000)
        client.connect()

        comps = client.discover()
        ids = {c.id for c in comps}
        assert "interop.echo" in ids, f"missing interop.echo in {ids}"

        result = client.call("interop.echo", {"msg": "hola"})
        assert result == {"msg": "hola"}, f"unexpected result {result}"

        chunks = list(client.call_stream("interop.counter", {"n": 3}))
        seqs = [c.seq for c in chunks]
        assert seqs == [0, 1, 2, 3], f"unexpected seq {seqs}"
        assert chunks[-1].end is True

        reply = client.request({"op": "$ping", "input": {"ts": 42}})
        assert reply["ok"] is True and reply["result"]["ts"] == 42, f"bad ping {reply}"
        assert reply["acp"] == "0.2", f"expected acp echo, got {reply.get('acp')}"

        client.close()
        print("py-client <-> ts-server: PASS")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"py-client <-> ts-server: FAIL — {exc}")
        return 1
    finally:
        proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
