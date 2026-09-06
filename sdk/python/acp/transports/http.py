"""HTTP transport (spec §9): POST /acp is the single MUST endpoint;
streaming = NDJSON.

Server side uses stdlib ``http.server.ThreadingHTTPServer``; client side uses
``urllib.request``. Note: this SDK serves HTTP on a port adjacent to the
WebSocket port (see AcpServer.listen) because websockets' built-in HTTP
parser only accepts GET requests and cannot serve POST bodies.
"""

import gzip
import io
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, List, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from ..codec import PROTOCOL_VERSION, error_envelope
from ..errors import (
    AcpErrorCode,
    acp_code_to_http_status,
    CODE_INTERNAL_ERROR,
    CODE_METHOD_NOT_ALLOWED,
    CODE_PARSE_ERROR,
)
from .base import Connection, ServerDispatch, TransportLifecycle


# ---------------------------------------------------------------------------
# Server side
# ---------------------------------------------------------------------------

class HttpServerTransport:
    """Server-side HTTP transport backed by ThreadingHTTPServer (spec §9)."""

    def __init__(self, port: int = 0, host: str = "127.0.0.1") -> None:
        self._port = port
        self._host = host
        self._dispatch: Optional[ServerDispatch] = None
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def port(self) -> int:
        return self._server.server_address[1] if self._server is not None else 0

    def start(self, dispatch: ServerDispatch, lifecycle: Optional[TransportLifecycle] = None) -> None:
        # HTTP is connectionless: lifecycle hooks are never called (spec v0.2 §4.4).
        dispatch = self._dispatch = dispatch
        outer = self

        class _AcpHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # silence request logging
                pass

            def do_POST(self) -> None:
                path = self.path.split("?", 1)[0]
                if path != "/acp":
                    outer._send_json(self, 404, {"ok": False})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length > 0 else b""
                try:
                    raw = json.loads(body.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    outer._send_error(self, None, CODE_PARSE_ERROR, "request body is not valid JSON")
                    return
                streaming = isinstance(raw, dict) and raw.get("stream") is True
                frames: List[dict] = []
                conn = Connection(meta={"transport": "http"}, send=frames.append, close=lambda: None)
                dispatch(raw, conn)
                if not frames:
                    frames.append(error_envelope(None, CODE_INTERNAL_ERROR, "no reply from dispatcher"))
                if streaming:
                    payload = "".join(json.dumps(f) + "\n" for f in frames)
                    outer._send_raw(self, 200, payload.encode("utf-8"), "application/x-ndjson")
                    return
                msg = frames[0]
                status = (
                    acp_code_to_http_status(msg["error"]["code"])
                    if msg.get("ok") is False
                    else 200
                )
                outer._send_raw(self, status, json.dumps(msg).encode("utf-8"), "application/json")

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0]
                if path == "/acp/discover":
                    outer._send_discover(self)
                elif path == "/acp/health":
                    outer._send_json(self, 200, {"ok": True})
                else:
                    outer._send_json(self, 404, {"ok": False})

            def do_PUT(self) -> None:
                self.do_POST()

            def do_DELETE(self) -> None:
                self.do_POST()

        self._server = ThreadingHTTPServer((self._host, self._port), _AcpHandler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="acp-http", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    # -- helpers ------------------------------------------------------------

    def _send_discover(self, handler: BaseHTTPRequestHandler) -> None:
        frames: List[dict] = []
        conn = Connection(meta={"transport": "http"}, send=frames.append, close=lambda: None)
        self._dispatch(
            {"acp": PROTOCOL_VERSION, "id": "get-{}".format(int(time.time() * 1000)), "op": "discover"},
            conn,
        )
        reply = frames[0] if frames else error_envelope(None, CODE_INTERNAL_ERROR, "no reply")
        if reply.get("ok") is False:
            self._send_raw(
                handler,
                acp_code_to_http_status(reply["error"]["code"]),
                json.dumps(reply).encode("utf-8"),
                "application/json",
            )
            return
        self._send_json(handler, 200, reply.get("result"))

    @staticmethod
    def _send_json(handler: BaseHTTPRequestHandler, status: int, body: Any) -> None:
        HttpServerTransport._send_raw(
            handler, status, json.dumps(body).encode("utf-8"), "application/json"
        )

    @staticmethod
    def _send_error(handler: BaseHTTPRequestHandler, id: Optional[str], code: int, message: str) -> None:
        body = error_envelope(id, code, message)
        HttpServerTransport._send_raw(
            handler, acp_code_to_http_status(code), json.dumps(body).encode("utf-8"), "application/json"
        )

    @staticmethod
    def _send_raw(handler: BaseHTTPRequestHandler, status: int, body: bytes, content_type: str) -> None:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Vary", "Accept-Encoding")
        handler.end_headers()
        handler.wfile.write(body)


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------

class HttpClientTransport:
    """Client-side HTTP transport backed by urllib (spec §9).

    Connectionless: ``events_supported()`` is False. Streaming responses are
    parsed as line-delimited NDJSON.
    """

    def __init__(self, url: str, headers: Optional[dict] = None) -> None:
        self._url = url
        self._headers = dict(headers or {})

    def connect(self) -> None:  # connectionless
        pass

    def request(self, req: dict, timeout: Optional[float] = None) -> dict:
        text = self._post(req, timeout)
        content_type = self._last_content_type or ""
        if "x-ndjson" in content_type:
            lines = [line for line in text.split("\n") if line.strip()]
            if not lines:
                raise ValueError("empty NDJSON response")
            return json.loads(lines[0])
        try:
            return json.loads(text)
        except ValueError:
            raise ValueError(
                "invalid ACP response: {}".format(text[:120])
            ) from None

    def request_stream(self, req: dict, timeout: Optional[float] = None):
        resp = self._open(json.dumps(req).encode("utf-8"), timeout)
        with resp:
            text = self._read_all(resp)
        for line in text.split("\n"):
            line = line.strip()
            if line:
                yield json.loads(line)

    def close(self) -> None:  # connectionless
        pass

    def events_supported(self) -> bool:
        return False

    def on_event(self, handler: Callable[[dict], None]) -> Callable[[], None]:
        raise AcpErrorCode.EVENT_UNSUPPORTED  # pragma: no cover - guarded by client

    # -- helpers ------------------------------------------------------------

    _last_content_type: str = ""

    def _post(self, req: dict, timeout: Optional[float]) -> str:
        resp = self._open(json.dumps(req).encode("utf-8"), timeout)
        with resp:
            return self._read_all(resp)

    def _open(self, body: bytes, timeout: Optional[float]):
        r = urllib_request.Request(
            self._url,
            data=body,
            headers={"Content-Type": "application/json", **self._headers},
            method="POST",
        )
        try:
            resp = urllib_request.urlopen(r, timeout=timeout)
        except urllib_error.HTTPError as e:
            resp = e  # ACP error envelopes arrive with non-200 status (spec §9.3)
        self._last_content_type = resp.headers.get("Content-Type", "") or ""
        encoding = (resp.headers.get("Content-Encoding") or "").lower()
        if "gzip" in encoding:
            resp = io.BytesIO(gzip.decompress(resp.read()))
            self._last_content_type = ""
        return resp

    @staticmethod
    def _read_all(resp: Any) -> str:
        chunks = []
        if hasattr(resp, "readline"):  # streaming-friendly: iterate lines
            for raw_line in resp:
                chunks.append(raw_line if isinstance(raw_line, bytes) else raw_line.encode("utf-8"))
        else:
            chunks.append(resp.read())
        return b"".join(chunks).decode("utf-8")
