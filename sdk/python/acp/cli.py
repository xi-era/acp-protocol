"""Minimal ACP CLI: ``acp discover <url>`` and ``acp call <url> <component>``.

Examples::

    acp discover http://127.0.0.1:8101/acp
    acp call ws://127.0.0.1:8100/acp sensor.temperature --input '{"unit": "C"}'
"""

import argparse
import json
import sys
from typing import List, Optional

from .client import AcpClient


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="acp", description="ACP v0.2 command-line client")
    sub = parser.add_subparsers(dest="command", required=True)

    p_discover = sub.add_parser("discover", help="list components of a server")
    p_discover.add_argument("url", help="server endpoint (http(s):// or ws(s)://)")
    p_discover.add_argument("component", nargs="?", help="optional single component id")

    p_call = sub.add_parser("call", help="call a component")
    p_call.add_argument("url", help="server endpoint (http(s):// or ws(s)://)")
    p_call.add_argument("component", help="component id")
    p_call.add_argument("--input", default=None, help="JSON input (default: none)")
    p_call.add_argument("--stream", action="store_true", help="request streamed chunks")

    args = parser.parse_args(argv)

    client = AcpClient(args.url)
    client.connect()
    try:
        if args.command == "discover":
            components = client.discover(args.component)
            print(json.dumps([_descriptor_dict(c) for c in components], indent=2, ensure_ascii=False))
            return 0

        input_value = json.loads(args.input) if args.input else None
        if getattr(args, "stream", False):
            for chunk in client.call_stream(args.component, input_value):
                print(json.dumps({"seq": chunk.seq, "end": chunk.end, "data": chunk.data}, ensure_ascii=False))
        else:
            result = client.call(args.component, input_value)
            print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except Exception as e:
        print("error: {}".format(e), file=sys.stderr)
        return 1


def _descriptor_dict(descriptor) -> dict:
    from dataclasses import asdict

    d = asdict(descriptor)
    d["inputSchema"] = d.pop("input_schema")
    d["outputSchema"] = d.pop("output_schema")
    return d


if __name__ == "__main__":
    raise SystemExit(main())
