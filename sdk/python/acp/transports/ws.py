"""WebSocket transport (spec §10): one text frame = one envelope; no handshake
packet; per-message version negotiation; concurrent calls multiplexed by id.

Uses the **threaded** implementation of ``websockets`` (``websockets.sync``):
one thread per connection, thread-safe sends. v0.2 additions: application-layer
``$ping`` keepalive, automatic re-subscription after reconnect, and server
``$event`` routing (spec v0.2 §4.3, §4.4, §10.3).
"""

import json
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from websockets.datastructures import Headers
from websockets.http11 import Response
from websockets.sync.client import connect as ws_connect
from websockets.sync.server import serve as ws_serve

from ..codec import PROTOCOL_VERSION, error_envelope
from ..errors import (
    CODE_INTERNAL_ERROR,
    CODE_INVALID_ENVELOPE,
    CODE_PARSE_ERROR,
    CODE_TIMEOUT,
    CODE_UNKNOWN_OP,
    AcpError,
)
from .base import Connection, ServerDispatch, TransportLifecycle


def _make_response(status: int, body: str, content_type: str) -> Response:
    from http import HTTPStatus

    return Response(
        status,
        HTTPStatus(status).phrase,
        Headers({"Content-Type": content_type}),
        body.encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# Server side
# ---------------------------------------------------------------------------

class WsServerTransport:
    """Server-side WebSocket transport backed by ``websockets.sync.server``."""

    def __init__(self, port: int = 0, host: str = "127.0.0.1") -> None:
        self._port = port
        self._host = host
        self._dispatch: Optional[ServerDispatch] = None
        self._lifecycle: Optional[TransportLifecycle] = None
        self._server = None

    @property
    def port(self) -> int:
        return self._server.socket.getsockname()[1] if self._server is not None else 0

    def start(self, dispatch: ServerDispatch, lifecycle: Optional[TransportLifecycle] = None) -> None:
        self._dispatch = dispatch
        self._lifecycle = lifecycle
        self._server = ws_serve(
            self._handle_connection,
            self._host,
            self._port,
            process_request=self._process_request,
        )

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server = None

    def _handle_connection(self, ws: Any) -> None:
        assert self._dispatch is not None
        remote = ws.remote_address or ("?", 0)
        conn = Connection(
            meta={"transport": "ws", "ip": remote[0]},
            send=lambda msg: ws.send(json.dumps(msg)),
            close=ws.close,
        )
        if self._lifecycle is not None and self._lifecycle.on_connection is not None:
            self._lifecycle.on_connection(conn)
        try:
            for raw in ws:
                if isinstance(raw, bytes):
                    conn.send(error_envelope(None, CODE_INVALID_ENVELOPE, "binary frames are not defined"))
                    continue
                try:
                    parsed = json.loads(raw)
                except ValueError:
                    conn.send(error_envelope(None, CODE_PARSE_ERROR, "frame is not valid JSON"))
                    continue
                self._dispatch(parsed, conn)
        finally:
            if self._lifecycle is not None and self._lifecycle.on_disconnect is not None:
                self._lifecycle.on_disconnect(conn)

    def _process_request(self, connection: Any, request: Any) -> Optional[Response]:
        """Serves plain-HTTP GET probes on the WS port (spec §9.1 SHOULD)."""
        path = request.path.split("?", 1)[0]
        headers = request.headers
        if (headers.get("Upgrade") or "").lower() == "websocket":
            return None  # proceed with the WebSocket opening handshake
        if path == "/acp/discover":
            frames: List[dict] = []
            conn = Connection(meta={"transport": "ws"}, send=frames.append, close=lambda: None)
            self._dispatch_call(
                {"acp": PROTOCOL_VERSION, "id": "get-{}".format(int(time.time() * 1000)), "op": "discover"},
                conn,
            )
            reply = frames[0] if frames else error_envelope(None, CODE_INTERNAL_ERROR, "no reply")
            if reply.get("ok") is False:
                return _make_response(
                    _status_of(reply), json.dumps(reply), "application/json"
                )
            return _make_response(200, json.dumps(reply.get("result")), "application/json")
        if path == "/acp/health":
            return _make_response(200, json.dumps({"ok": True}), "application/json")
        if path == "/acp":
            # Non-POST /acp without a WebSocket upgrade (POST is not routable
            # through websockets' HTTP parser; see module docstring).
            frame = error_envelope(None, 40500, "method {} not allowed on /acp".format(request.method))
            return _make_response(_status_of(frame), json.dumps(frame), "application/json")
        return _make_response(404, json.dumps({"ok": False}), "application/json")

    def _dispatch_call(self, raw: Any, conn: Connection) -> None:
        assert self._dispatch is not None
        self._dispatch(raw, conn)


def _status_of(error_frame: dict) -> int:
    from ..errors import acp_code_to_http_status

    return acp_code_to_http_status(error_frame["error"]["code"])


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------

def _timeout_error(req_id: str, timeout: Optional[float]) -> AcpError:
    seconds = timeout if timeout is not None else 0
    return AcpError(CODE_TIMEOUT, "call {} timed out after {}ms".format(req_id, int(seconds * 1000)))


class _Pending:
    __slots__ = ("frames",)

    def __init__(self) -> None:
        import queue

        self.frames: "queue.Queue[dict]" = queue.Queue()


class WsClientTransport:
    """Client-side WebSocket transport backed by ``websockets.sync.client``.

    - Multiplexes concurrent calls by request id (spec §10.3).
    - Routes ``$event`` frames to registered handlers.
    - Auto-answers server-initiated ``$ping`` frames (spec v0.2 §4.3).
    - Sends idle ``$ping`` keepalives; a 40002 reply permanently disables
      keepalive (0.1 server); a pong timeout closes the connection.
    - Tracks ``$subscribe`` filters and re-subscribes after reconnect.
    """

    def __init__(
        self,
        url: str,
        headers: Optional[dict] = None,
        keep_alive_ms: int = 30000,
        pong_timeout_ms: int = 10000,
    ) -> None:
        self._url = url
        self._headers = dict(headers or {})
        self._keep_alive_ms = keep_alive_ms
        self._pong_timeout_ms = pong_timeout_ms
        self._ws = None
        self._lock = threading.Lock()
        self._pending: Dict[str, _Pending] = {}
        self._event_handlers: List[Callable[[dict], None]] = []
        self._subscriptions: Dict[str, dict] = {}
        self._closed_by_user = False
        self._keepalive_supported = True
        self._ka_seq = 0
        self._last_activity = time.monotonic()
        self._stop_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._ka_thread: Optional[threading.Thread] = None

    # -- ClientTransport API -------------------------------------------------

    def connect(self) -> None:
        if self._ws is not None:
            return
        self._closed_by_user = False
        self._stop_event.clear()
        ws = ws_connect(self._url, additional_headers=self._headers or None)
        self._ws = ws
        self._reader_thread = threading.Thread(target=self._read_loop, args=(ws,), daemon=True, name="acp-ws-reader")
        self._reader_thread.start()
        # Auto-resubscribe after reconnect (spec v0.2 §4.4: SDK responsibility).
        for filter_ in list(self._subscriptions.values()):
            self._ka_seq += 1
            try:
                self.request(
                    {"acp": PROTOCOL_VERSION, "id": "resub-{}".format(self._ka_seq), "op": "$subscribe", "input": filter_}
                )
            except AcpError:
                pass
        if self._ka_thread is None and self._keep_alive_ms > 0:
            self._ka_thread = threading.Thread(target=self._keepalive_loop, daemon=True, name="acp-ws-keepalive")
            self._ka_thread.start()

    def request(self, req: dict, timeout: Optional[float] = None) -> dict:
        ws = self._assert_connected()
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        self._track_subscription(req)
        self._send_frame(ws, req)
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                try:
                    msg = self._get_frame(pending, deadline, req["id"])
                except queue.Empty:
                    raise _timeout_error(req["id"], timeout) from None
                if "ok" in msg or ("chunk" in msg and msg["chunk"].get("end") is True):
                    return msg
                # non-terminal chunk on a request() call: keep waiting
        finally:
            with self._lock:
                self._pending.pop(req["id"], None)

    def request_stream(self, req: dict, timeout: Optional[float] = None):
        ws = self._assert_connected()
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        self._send_frame(ws, req)
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                try:
                    msg = self._get_frame(pending, deadline, req["id"])
                except queue.Empty:
                    raise _timeout_error(req["id"], timeout) from None
                yield msg
                if "ok" in msg or ("chunk" in msg and msg["chunk"].get("end") is True):
                    return
        finally:
            with self._lock:
                self._pending.pop(req["id"], None)

    @staticmethod
    def _get_frame(pending: "_Pending", deadline: Optional[float], req_id: str) -> dict:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise queue.Empty()
        return pending.frames.get(timeout=remaining)

    def close(self) -> None:
        self._closed_by_user = True
        self._stop_event.set()
        self._fail_all("client closed")
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def events_supported(self) -> bool:
        return True

    def on_event(self, handler: Callable[[dict], None]) -> Callable[[], None]:
        self._event_handlers.append(handler)
        return lambda: self._event_handlers.remove(handler)

    # -- internals -----------------------------------------------------------

    def _assert_connected(self):
        ws = self._ws
        if ws is None:
            raise AcpError(CODE_INTERNAL_ERROR, "ws transport not connected — call connect() first")
        return ws

    def _send_frame(self, ws: Any, frame: dict) -> None:
        ws.send(json.dumps(frame))
        self._last_activity = time.monotonic()

    def _track_subscription(self, req: dict) -> None:
        if req.get("op") == "$subscribe":
            input_ = req.get("input") or {}
            self._subscriptions[json.dumps(input_, sort_keys=True)] = input_
        elif req.get("op") == "$unsubscribe":
            input_ = req.get("input")
            if input_ is None:
                self._subscriptions.clear()
            else:
                self._subscriptions.pop(json.dumps(input_, sort_keys=True), None)

    def _read_loop(self, ws: Any) -> None:
        try:
            for raw in ws:
                if isinstance(raw, bytes):
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                self._on_message(ws, msg)
        except Exception:
            pass
        finally:
            if self._ws is ws:
                self._ws = None
            self._fail_all("connection closed")
            if not self._closed_by_user:
                # Basic reconnect retry (spec v0.2 §4.4: re-subscribe after reconnect).
                timer = threading.Timer(1.0, self._try_reconnect)
                timer.daemon = True
                timer.start()

    def _try_reconnect(self) -> None:
        if self._closed_by_user or self._ws is not None:
            return
        try:
            self.connect()
        except Exception:
            timer = threading.Timer(1.0, self._try_reconnect)
            timer.daemon = True
            timer.start()

    def _on_message(self, ws: Any, msg: dict) -> None:
        if "event" in msg:
            for handler in list(self._event_handlers):
                handler(msg["event"])
            return
        if msg.get("op") == "$ping":
            # Server-initiated keepalive: MUST answer (spec v0.2 §4.3).
            input_ = msg.get("input") or {}
            result: dict = {"pong": int(time.time() * 1000)}
            if isinstance(input_, dict) and isinstance(input_.get("ts"), (int, float)):
                result["ts"] = input_["ts"]
            self._send_frame(ws, {"acp": msg.get("acp", PROTOCOL_VERSION), "id": msg.get("id"), "ok": True, "result": result})
            return
        pending = self._pending.get(msg.get("id") or "")
        if pending is None:
            return
        pending.frames.put(msg)

    def _fail_all(self, reason: str) -> None:
        with self._lock:
            pendings = list(self._pending.values())
            self._pending.clear()
        for pending in pendings:
            pending.frames.put(
                {"acp": PROTOCOL_VERSION, "id": None, "ok": False, "error": {"code": CODE_INTERNAL_ERROR, "message": reason}}
            )

    def _keepalive_loop(self) -> None:
        # Idle-time $ping keepalive (spec v0.2 §4.3).
        while not self._stop_event.wait(max(self._keep_alive_ms, 1) / 1000.0):
            if not self._keepalive_supported or self._closed_by_user:
                return
            if time.monotonic() - self._last_activity < self._keep_alive_ms / 1000.0:
                continue  # connection not idle
            ws = self._ws
            if ws is None:
                continue
            self._ka_seq += 1
            try:
                self.request(
                    {
                        "acp": PROTOCOL_VERSION,
                        "id": "ka-{}".format(self._ka_seq),
                        "op": "$ping",
                        "input": {"ts": int(time.time() * 1000)},
                    },
                    timeout=self._pong_timeout_ms / 1000.0,
                )
            except AcpError as e:
                if e.code == CODE_UNKNOWN_OP:
                    # 0.1 server: permanently disable keepalive on this connection.
                    self._keepalive_supported = False
                    return
                self._close_socket(ws)
            except Exception:
                self._close_socket(ws)

    def _close_socket(self, ws: Any) -> None:
        try:
            ws.close()
        except Exception:
            pass
