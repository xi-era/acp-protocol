#!/usr/bin/env python3
"""Interop: TypeScript AcpClient against a Python ACP server.

Boots a Python AcpServer (examples-style), then runs scripts/interop/ts-client.mjs
(node + @xi-era/acp-sdk dist) against it. Exit code 0 = pass.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PORT = 8612


def wait_port(port: int, timeout: float = 15.0) -> None:
    import socket

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"port {port} never became ready")


def main() -> int:
    env = dict(os.environ, ACP_PORT=str(PORT))
    proc = subprocess.Popen(
        [sys.executable, str(REPO / "scripts/interop/py-server.py")],
        cwd=REPO,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_port(PORT)
        result = subprocess.run(
            ["node", str(REPO / "scripts/interop/ts-client.mjs"), str(PORT)],
            cwd=REPO,
            text=True,
        )
        if result.returncode == 0:
            print("ts-client <-> py-server: PASS")
            return 0
        print("ts-client <-> py-server: FAIL")
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ts-client <-> py-server: FAIL — {exc}")
        return 1
    finally:
        proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
