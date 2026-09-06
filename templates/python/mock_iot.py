"""模板:模拟 IoT 硬件 → ACP 元件(Python 版)。

展示硬件场景三件套:一次性读取、流式读数、$event 事件推送。
启动:python templates/python/mock_iot.py
"""
from __future__ import annotations

import asyncio
import math
import os
from typing import Any, AsyncIterator

from acp import AcpServer
from acp.component import ComponentDef

_state = {"reads": 0}


def reading(base: float) -> float:
    _state["reads"] += 1
    return round(base + math.sin(_state["reads"] / 7) * 5 + (_state["reads"] % 3), 1)


def build_server() -> AcpServer:
    server = AcpServer(name="mock-iot-node", version="1.0.0")

    def handle_temp(inp: Any, ctx) -> dict[str, float]:  # noqa: ANN001
        return {"celsius": reading(21)}

    async def handle_watch(inp: dict[str, Any], ctx) -> AsyncIterator[dict[str, Any]]:  # noqa: ANN001
        for i in range(int(inp.get("n", 1))):
            celsius = reading(21)
            ctx.emit(data={"seq": i, "celsius": celsius})  # $event 推送(spec v0.2 §6.2)
            yield {"seq": i, "celsius": celsius}
            await asyncio.sleep(0.1)

    server.register(
        ComponentDef(
            id="sensor.iot.temperature",
            name="Temperature",
            description="Reads the current temperature (°C)",
            tags=["iot", "template", "sensor"],
            handle=handle_temp,
        )
    )
    server.register(
        ComponentDef(
            id="sensor.iot.watch",
            name="Sensor Watch",
            description="Streams n live readings and pushes $event to subscribers",
            stream=True,
            input_schema={
                "type": "object",
                "properties": {"n": {"type": "integer", "minimum": 1, "maximum": 1000}},
                "required": ["n"],
            },
            tags=["iot", "template", "stream"],
            handle=handle_watch,
        )
    )
    return server


if __name__ == "__main__":
    server = build_server()
    ws_port = server.listen(port=int(os.environ.get("ACP_PORT", "8093")))
    print(f"HTTP ACP endpoint: http://localhost:{server.http_port}/acp  (WS: ws://localhost:{ws_port}/acp)")
    server.serve_forever()
