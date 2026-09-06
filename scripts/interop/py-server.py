#!/usr/bin/env python3
"""Python ACP server for interop tests (ACP_PORT env, default 8612)."""
from __future__ import annotations

import os
from typing import Any, AsyncIterator

from acp import AcpServer, ComponentDef


def echo_handle(inp: Any, ctx) -> dict[str, str]:  # noqa: ANN001
    return {"msg": inp["msg"]}


async def counter_handle(inp: dict[str, Any], ctx) -> AsyncIterator[dict[str, int]]:  # noqa: ANN001
    for i in range(int(inp["n"])):
        yield {"i": i}


server = AcpServer(name="interop-py-node", version="1.0.0")
server.register(
    ComponentDef(
        id="interop.echo",
        name="Echo",
        description="Echoes msg",
        input_schema={"type": "object", "properties": {"msg": {"type": "string"}}, "required": ["msg"]},
        tags=["interop"],
        handle=echo_handle,
    )
)
server.register(
    ComponentDef(
        id="interop.counter",
        name="Counter",
        description="Streams n items",
        stream=True,
        tags=["interop", "stream"],
        handle=counter_handle,
    )
)

if __name__ == "__main__":
    port = server.listen(port=int(os.environ.get("ACP_PORT", "8612")))
    print(f"ready:{port}", flush=True)
    server.serve_forever()
