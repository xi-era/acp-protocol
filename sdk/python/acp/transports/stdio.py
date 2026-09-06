"""Stdio transport (spec §11): one envelope per line, stderr left for logs.

v0.2: the connection lives for the lifetime of the process pair; ``$ping`` and
``$event`` are available. The server transport is typically wired to an
``AcpServer`` via ``server.connection_lifecycle``.
"""

import asyncio
import json
import queue
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from ..codec import PROTOCOL_VERSION, error_envelope
from ..errors import (
    AcpError,
    CODE_INTERNAL_ERROR,
    CODE_PARSE_ERROR,
    CODE_TIMEOUT,
)
from .base import Connection, ServerDispatch, TransportLifecycle


def _timeout_error(req_id: str, timeout: Optional[float]) -> AcpError:
    seconds = timeout if timeout is not None else 0
    return AcpError(CODE_TIMEOUT, "call {} timed out after {}ms".format(req_id, int(seconds * 1000)))


# ---------------------------------------------------------------------------
# Server side
# ---------------------------------------------------------------------------

class StdioServerTransport:
    """Server-side stdio transport: reads request lines from ``input`` and
    writes reply lines to ``output`` (spec §11).

    Runs the (possibly async) dispatch on a private event-loop thread so it
    can serve handlers of any shape.
    """

    def __init__(self, input: Any = None, output: Any = None) -> None:
        self._input = input if input is not None else sys.stdin.buffer
        self._output = output if output is not None else sys.stdout.buffer
        self._write_lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self, dispatch: ServerDispatch, lifecycle: Optional[TransportLifecycle] = None) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name="acp-stdio-loop")
        self._thread.start()
        conn = Connection(
            meta={"transport": "stdio"},
            send=self._send,
            close=lambda: None,
        )
        if lifecycle is not None and lifecycle.on_connection is not None:
            lifecycle.on_connection(conn)
        reader = threading.Thread(
            target=self._read_loop, args=(dispatch, conn, lifecycle), daemon=True, name="acp-stdio-reader"
        )
        reader.start()

    def stop(self) -> None:
        # The process pair owns the streams; nothing to close here.
        pass

    def _send(self, msg: dict) -> None:
        with self._write_lock:
            self._output.write((json.dumps(msg) + "\n").encode("utf-8"))
            self._output.flush()

    def _read_loop(self, dispatch: ServerDispatch, conn: Connection, lifecycle: Optional[TransportLifecycle]) -> None:
        while True:
            line = self._input.readline()
            if not line:
                break  # EOF: the other end closed the pipe
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except ValueError:
                self._send(error_envelope(None, CODE_PARSE_ERROR, "frame is not valid JSON"))
                continue
            loop = self._loop
            assert loop is not None
            asyncio.run_coroutine_threadsafe(_dispatch_async(dispatch, parsed, conn), loop).result()
        if lifecycle is not None and lifecycle.on_disconnect is not None:
            lifecycle.on_disconnect(conn)


async def _dispatch_async(dispatch: ServerDispatch, raw: Any, conn: Connection) -> None:
    result = dispatch(raw, conn)
    if asyncio.iscoroutine(result):
        await result


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------

class _Pending:
    __slots__ = ("frames",)

    def __init__(self) -> None:
        self.frames: "queue.Queue[dict]" = queue.Queue()


class StdioClientTransport:
    """Client-side stdio transport: writes request lines to ``output`` and
    reads reply/event lines from ``input``.

    Routes ``$event`` frames to handlers and auto-answers server-initiated
    ``$ping`` frames (spec v0.2 §4.3, §4.4).
    """

    def __init__(self, input: Any, output: Any) -> None:
        self._input = input  # readable: server -> client
        self._output = output  # writable: client -> server
        self._lock = threading.Lock()
        self._pending: Dict[str, _Pending] = {}
        self._event_handlers: List[Callable[[dict], None]] = []
        self._connected = False
        self._closed = False

    def connect(self) -> None:
        if self._connected:
            return
        self._connected = True
        self._closed = False
        reader = threading.Thread(target=self._read_loop, daemon=True, name="acp-stdio-client")
        reader.start()

    def request(self, req: dict, timeout: Optional[float] = None) -> dict:
        self._assert_connected()
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        try:
            self._write(req)
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                try:
                    msg = self._get_frame(pending, deadline)
                except queue.Empty:
                    raise _timeout_error(req["id"], timeout) from None
                if "ok" in msg or ("chunk" in msg and msg["chunk"].get("end") is True):
                    return msg
        finally:
            with self._lock:
                self._pending.pop(req["id"], None)

    def request_stream(self, req: dict, timeout: Optional[float] = None):
        self._assert_connected()
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        try:
            self._write(req)
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                try:
                    msg = self._get_frame(pending, deadline)
                except queue.Empty:
                    raise _timeout_error(req["id"], timeout) from None
                yield msg
                if "ok" in msg or ("chunk" in msg and msg["chunk"].get("end") is True):
                    return
        finally:
            with self._lock:
                self._pending.pop(req["id"], None)

    def close(self) -> None:
        self._closed = True
        self._connected = False
        self._fail_all("client closed")

    def events_supported(self) -> bool:
        return True

    def on_event(self, handler: Callable[[dict], None]) -> Callable[[], None]:
        self._event_handlers.append(handler)
        return lambda: self._event_handlers.remove(handler)

    # -- internals -----------------------------------------------------------

    def _assert_connected(self) -> None:
        if not self._connected:
            raise AcpError(CODE_INTERNAL_ERROR, "stdio transport not connected — call connect() first")

    @staticmethod
    def _get_frame(pending: _Pending, deadline: Optional[float]) -> dict:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise queue.Empty()
        return pending.frames.get(timeout=remaining)

    def _write(self, frame: dict) -> None:
        self._output.write((json.dumps(frame) + "\n").encode("utf-8"))
        self._output.flush()

    def _read_loop(self) -> None:
        while not self._closed:
            try:
                line = self._input.readline()
            except Exception:
                break
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if "event" in msg:
                for handler in list(self._event_handlers):
                    handler(msg["event"])
                continue
            if msg.get("op") == "$ping":
                # Server-initiated keepalive: MUST answer (spec v0.2 §4.3).
                input_ = msg.get("input") or {}
                result: dict = {"pong": int(time.time() * 1000)}
                if isinstance(input_, dict) and isinstance(input_.get("ts"), (int, float)):
                    result["ts"] = input_["ts"]
                try:
                    self._write(
                        {"acp": msg.get("acp", PROTOCOL_VERSION), "id": msg.get("id"), "ok": True, "result": result}
                    )
                except Exception:
                    break
                continue
            pending = self._pending.get(msg.get("id") or "")
            if pending is not None:
                pending.frames.put(msg)

    def _fail_all(self, reason: str) -> None:
        with self._lock:
            pendings = list(self._pending.values())
            self._pending.clear()
        for pending in pendings:
            pending.frames.put(
                {"acp": PROTOCOL_VERSION, "id": None, "ok": False, "error": {"code": CODE_INTERNAL_ERROR, "message": reason}}
            )
