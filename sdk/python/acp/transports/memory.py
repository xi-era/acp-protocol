"""In-process transport: connects a ClientTransport directly to an AcpServer
dispatch without any network loopback (spec §2 "Connection"; v0.2 events).

Used by unit tests and by adapters bridging protocols inside a single process.
Mirrors packages/acp-sdk-ts/src/memory-transport.ts: the pending entry is
registered *before* dispatch (replies can arrive synchronously), and a reply
whose id doesn't match falls back to the stashed orphan frame or a synthetic
50000 error.
"""

import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from ..codec import PROTOCOL_VERSION
from ..errors import AcpError, CODE_INTERNAL_ERROR, CODE_TIMEOUT
from .base import Connection, ServerDispatch


def _timeout_error(req_id: str, timeout: Optional[float]) -> AcpError:
    seconds = timeout if timeout is not None else 0
    return AcpError(CODE_TIMEOUT, "call {} timed out after {}ms".format(req_id, int(seconds * 1000)))


class _Pending:
    __slots__ = ("frames", "terminal", "done")

    def __init__(self) -> None:
        self.frames: "queue.Queue[dict]" = queue.Queue()
        self.terminal: Optional[dict] = None
        self.done = threading.Event()


class MemoryClientTransport:
    """Client transport bound to one server instance (persistent connection)."""

    def __init__(
        self,
        server: Any = None,
        dispatch: Optional[ServerDispatch] = None,
        on_connect: Optional[Callable[[Connection], None]] = None,
        on_close: Optional[Callable[[Connection], None]] = None,
    ) -> None:
        if server is not None:
            # Attach through the server's connection lifecycle so $subscribe
            # and $event work across the in-process connection.
            self._dispatch: ServerDispatch = server.handle
            on_connect = on_connect or server.attach_connection
            on_close = on_close or server.detach_connection
        else:
            if dispatch is None:
                raise ValueError("MemoryClientTransport requires a server or a dispatch")
            self._dispatch = dispatch
        self._on_connect = on_connect
        self._on_close = on_close
        self._conn: Optional[Connection] = None
        self._lock = threading.Lock()
        self._pending: Dict[str, _Pending] = {}
        self._orphan: Optional[dict] = None
        self._event_handlers: List[Callable[[dict], None]] = []

    # -- ClientTransport API -------------------------------------------------

    def connect(self) -> None:
        if self._conn is not None:
            return
        conn = Connection(meta={"transport": "memory"}, send=self._on_message, close=lambda: None)
        self._conn = conn
        if self._on_connect is not None:
            self._on_connect(conn)

    def request(self, req: dict, timeout: Optional[float] = None) -> dict:
        conn = self._ensure_conn()
        # Register pending BEFORE dispatch: replies can arrive synchronously.
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        try:
            self._dispatch(req, conn)
        except Exception as e:
            if not pending.done.is_set():
                pending.done.set()
                pending.terminal = self._synthetic_error(req, str(e))
        if not pending.done.is_set():
            # Reply arrived with an id that didn't match (e.g. the id:null error
            # replies for malformed envelopes): fall back to the orphan frame.
            pending.done.set()
            orphan, self._orphan = self._orphan, None
            pending.terminal = orphan or self._synthetic_error(req, "no reply from server")
        with self._lock:
            self._pending.pop(req["id"], None)
        assert pending.terminal is not None
        return pending.terminal

    def request_stream(self, req: dict, timeout: Optional[float] = None):
        conn = self._ensure_conn()
        pending = _Pending()
        with self._lock:
            self._pending[req["id"]] = pending
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            self._dispatch(req, conn)
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
        if self._conn is not None:
            if self._on_close is not None:
                self._on_close(self._conn)
            self._conn = None

    def events_supported(self) -> bool:
        return True

    def on_event(self, handler: Callable[[dict], None]) -> Callable[[], None]:
        self._event_handlers.append(handler)
        return lambda: self._event_handlers.remove(handler)

    # -- internals -----------------------------------------------------------

    def _ensure_conn(self) -> Connection:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        return self._conn

    @staticmethod
    def _get_frame(pending: _Pending, deadline: Optional[float]) -> dict:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise queue.Empty()
        return pending.frames.get(timeout=remaining)

    @staticmethod
    def _synthetic_error(req: dict, message: str) -> dict:
        return {
            "acp": req.get("acp", PROTOCOL_VERSION),
            "id": req.get("id"),
            "ok": False,
            "error": {"code": CODE_INTERNAL_ERROR, "message": message},
        }

    def _on_message(self, msg: dict) -> None:
        # Called synchronously from inside the dispatch (conn.send).
        if "event" in msg:
            for handler in list(self._event_handlers):
                handler(msg["event"])
            return
        if "op" in msg:
            return  # server-initiated requests are not expected in memory
        pending = self._pending.get(msg.get("id") or "")
        if pending is None:
            self._orphan = msg
            return
        pending.frames.put(msg)
        is_terminal = "ok" in msg or ("chunk" in msg and msg["chunk"].get("end") is True)
        if is_terminal:
            pending.terminal = msg
            pending.done.set()


def create_memory_client(server: Any) -> MemoryClientTransport:
    """Convenience factory binding a transport to an AcpServer instance."""
    return MemoryClientTransport(server=server)
