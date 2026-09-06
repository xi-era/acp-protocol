"""Virtual sensor ACP server example (Python SDK, spec v0.2).

Run::

    python examples/server.py [--port 8100]

Endpoints:
    ws://127.0.0.1:<port>/acp        WebSocket transport
    http://127.0.0.1:<port+1>/acp    HTTP transport (POST /acp, GET /acp/discover)

(HTTP sits on the adjacent port because Python's websockets HTTP parser only
accepts GET — see acp/server.py.)
"""

import argparse
import random

from acp import AcpServer

server = AcpServer(name="edge-node-01", version="1.0.0")


@server.component(
    id="sensor.temperature",
    name="Temperature Sensor",
    description="Reads current temperature from a virtual sensor",
    input_schema={
        "type": "object",
        "properties": {"unit": {"enum": ["C", "F"]}},
        "required": [],
    },
    tags=["iot", "sensor"],
)
def read_temperature(input, ctx):
    celsius = round(random.uniform(18.0, 30.0), 1)
    unit = (input or {}).get("unit", "C")
    if unit == "F":
        return {"celsius": celsius, "fahrenheit": round(celsius * 9 / 5 + 32, 1)}
    return {"celsius": celsius}


@server.component(
    id="sensor.readings",
    name="Sensor Readings",
    description="Streams n virtual sensor readings",
    stream=True,
    input_schema={
        "type": "object",
        "properties": {"n": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": [],
    },
    tags=["iot", "sensor", "stream"],
)
def readings(input, ctx):
    for i in range((input or {}).get("n", 3)):
        if ctx.emit is not None:
            ctx.emit(data={"i": i, "celsius": round(random.uniform(18.0, 30.0), 1)})
        yield {"i": i}


def main() -> None:
    parser = argparse.ArgumentParser(description="ACP v0.2 virtual sensor server")
    parser.add_argument("--port", type=int, default=8100, help="WebSocket port (HTTP uses port+1)")
    args = parser.parse_args()

    port = server.listen(port=args.port)
    print("ACP server listening:")
    print("  ws   ws://127.0.0.1:{}/acp".format(port))
    print("  http http://127.0.0.1:{}/acp".format(server.http_port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
