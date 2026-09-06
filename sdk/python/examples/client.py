"""ACP client example (Python SDK, spec v0.2).

Run (with the example server on its default port)::

    python examples/client.py [--port 8100]

Demonstrates discover, single call, streamed call, $ping keepalive and an
event subscription over the WebSocket transport.
"""

import argparse
import time

from acp import AcpClient


def main() -> None:
    parser = argparse.ArgumentParser(description="ACP v0.2 example client")
    parser.add_argument("--port", type=int, default=8100, help="WebSocket port of the example server")
    args = parser.parse_args()

    with AcpClient("ws://127.0.0.1:{}/acp".format(args.port), timeout_ms=5000) as client:
        # discover
        print("components:")
        for descriptor in client.discover():
            print("  {} - {}".format(descriptor.id, descriptor.description))

        # single call
        print("call:", client.call("sensor.temperature", {"unit": "F"}))

        # streamed call
        print("stream:")
        for chunk in client.call_stream("sensor.readings", {"n": 3}):
            print("  seq={} end={} data={}".format(chunk.seq, chunk.end, chunk.data))

        # $ping roundtrip (spec v0.2 §4.3)
        reply = client.request({"op": "$ping", "input": {"ts": int(time.time() * 1000)}})
        print("ping:", reply["result"])

        # events (spec v0.2 §4.4): the streaming component emits on each chunk
        events = []
        sub = client.subscribe(component="sensor.readings", handler=events.append)
        for _ in client.call_stream("sensor.readings", {"n": 2}):
            pass
        time.sleep(0.2)  # event delivery is asynchronous
        print("events:", events)
        sub.unsubscribe()


if __name__ == "__main__":
    main()
