"""AcpClient (spec §3-6): discover / call / callStream over any ClientTransport.

Synchronous, thread-based client. The transport is auto-selected from the URL
scheme (``http(s)://``, ``ws(s)://``, ``memory://``) or injected directly
(e.g. MemoryClientTransport for tests and adapters). Implements the v0.2
fallback ladder (spec §12.2): on 40003 with a compatible ``data.supported``
version, retry once with the highest supported version and lock it.
"""

import json
from typing import Any, Callable, Iterator, List, Optional
from uuid import uuid4

from .codec import PROTOCOL_VERSION
from .errors import AcpError, CODE_EVENT_UNSUPPORTED, CODE_INTERNAL_ERROR, CODE_INVALID_ENVELOPE, CODE_UNSUPPORTED_VERSION
from .types import AcpChunk, ComponentDescriptor


class AcpSubscription:
    """Handle returned by AcpClient.subscribe."""

    def __init__(self, unsubscribe_fn: Callable[[], None]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class AcpClient:
    """Synchronous ACP v0.2 client facade.

    Example::

        client = AcpClient("ws://127.0.0.1:8080/acp", timeout_ms=30000)
        client.connect()
        components = client.discover()
        result = client.call("sensor.temperature", {"unit": "C"})
        client.close()
    """

    def __init__(
        self,
        url: Optional[str] = None,
        timeout_ms: int = 30000,
        protocol_version: str = PROTOCOL_VERSION,
        headers: Optional[dict] = None,
        transport: Optional[Any] = None,
        keep_alive_ms: int = 30000,
        pong_timeout_ms: int = 10000,
        server: Optional[Any] = None,
    ) -> None:
        self._timeout = timeout_ms / 1000.0
        self._protocol_version = protocol_version
        self._fallback_tried = False
        if transport is not None:
            self._transport = transport
        else:
            if not url:
                raise ValueError("AcpClient requires either url or transport")
            self._transport = self._make_transport(
                url, headers, keep_alive_ms, pong_timeout_ms, server
            )

    @staticmethod
    def _make_transport(url, headers, keep_alive_ms, pong_timeout_ms, server):
        scheme = url.split(":", 1)[0].lower()
        if scheme in ("http", "https"):
            from .transports.http import HttpClientTransport

            return HttpClientTransport(url=url, headers=headers)
        if scheme in ("ws", "wss"):
            from .transports.ws import WsClientTransport

            return WsClientTransport(
                url=url,
                headers=headers,
                keep_alive_ms=keep_alive_ms,
                pong_timeout_ms=pong_timeout_ms,
            )
        if scheme == "memory":
            from .transports.memory import MemoryClientTransport

            if server is None:
                raise ValueError('memory:// URLs require the `server=` argument (e.g. AcpClient("memory://local", server=server))')
            return MemoryClientTransport(server=server)
        raise ValueError(
            "unsupported URL scheme: {} (use http(s)://, ws(s):// or memory://, or inject a transport)".format(scheme)
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> "AcpClient":
        """Establishes the connection (no-op for connectionless transports)."""
        self._transport.connect()
        return self

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "AcpClient":
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Ops (spec §4)
    # ------------------------------------------------------------------

    def discover(self, component_id: Optional[str] = None) -> List[ComponentDescriptor]:
        """Discover all components, or a single one by id (empty list when absent)."""
        req: dict = {"op": "discover"}
        if component_id is not None:
            req["component"] = component_id
        reply = self._request(req)
        result = self._assert_ok(reply).get("result") or {}
        return [ComponentDescriptor.from_dict(c) for c in result.get("components", [])]

    def call(self, component_id: str, input: Any = None) -> Any:
        """Single call; returns the bare result value (spec §5.1)."""
        req: dict = {"op": "call", "component": component_id}
        if input is not None:
            req["input"] = input
        reply = self._request(req)
        return self._assert_ok(reply).get("result")

    def call_stream(self, component_id: str, input: Any = None) -> Iterator[AcpChunk]:
        """Streamed call; yields chunk payloads in seq order and completes
        after the end frame (spec §6.1). Raises AcpError on error frames."""
        req: dict = {"op": "call", "component": component_id, "stream": True}
        if input is not None:
            req["input"] = input
        full = self._full_request(req)
        for frame in self._transport.request_stream(full, timeout=self._timeout):
            if "chunk" in frame:
                chunk = frame["chunk"]
                yield AcpChunk(
                    seq=chunk.get("seq", 0),
                    end=bool(chunk.get("end", False)),
                    data=chunk.get("data"),
                    bin=chunk.get("bin"),
                )
                if chunk.get("end"):
                    return
            elif "event" in frame:
                continue  # events are routed via on_event, not call streams
            elif frame.get("ok") is False:
                raise AcpError.from_body(frame.get("error"))
            else:
                return  # one-shot reply to a stream request: nothing to iterate

    def subscribe(self, component: Optional[str] = None, tags: Optional[List[str]] = None,
                  handler: Optional[Callable[[dict], None]] = None) -> AcpSubscription:
        """Subscribes to server events (spec v0.2 §4.4). Exactly one of
        ``component`` / ``tags`` must be set. Raises AcpError(50100) on
        transports without event support (e.g. HTTP)."""
        if (component is not None) == (tags is not None):
            raise AcpError(CODE_INVALID_ENVELOPE, "subscription filter requires exactly one of component/tags")
        if not getattr(self._transport, "events_supported", lambda: False)():
            raise AcpError(CODE_EVENT_UNSUPPORTED, "events unsupported on connectionless transport")
        filter_: dict = {"component": component} if component is not None else {"tags": tags}
        off = self._transport.on_event(handler) if handler is not None else (lambda: None)
        try:
            reply = self._request({"op": "$subscribe", "input": filter_})
            self._assert_ok(reply)
        except Exception:
            off()
            raise
        unsubscribed = False

        def unsubscribe() -> None:
            nonlocal unsubscribed
            if unsubscribed:
                return
            unsubscribed = True
            off()
            reply = self._request({"op": "$unsubscribe", "input": filter_})
            self._assert_ok(reply)

        return AcpSubscription(unsubscribe)

    def request(self, envelope: dict) -> dict:
        """Low-level escape hatch: sends a full envelope and returns the raw
        reply frame (no ok-assertion, no fallback ladder)."""
        return self._transport.request(self._full_request(envelope), timeout=self._timeout)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _request(self, req: dict) -> dict:
        full = self._full_request(req)
        reply = self._transport.request(full, timeout=self._timeout)
        if isinstance(reply, dict) and reply.get("ok") is False:
            error = reply.get("error") or {}
            # Fallback ladder step 1 (spec v0.2 §12.2): on 40003, retry once
            # with the highest server-supported version and lock it.
            if error.get("code") == CODE_UNSUPPORTED_VERSION and not self._fallback_tried:
                best = _pick_highest_supported(error.get("data"))
                if best is not None and best != self._protocol_version:
                    self._protocol_version = best
                    self._fallback_tried = True
                    return self._request(req)
            raise AcpError(
                int(error.get("code", CODE_INTERNAL_ERROR)),
                str(error.get("message", "unknown error")),
                error.get("data"),
            )
        return reply

    def _full_request(self, req: dict) -> dict:
        full = dict(req)
        full.setdefault("acp", self._protocol_version)
        if not full.get("id"):
            full["id"] = "acp-{}".format(uuid4())
        full.setdefault("op", "discover")
        return full

    @staticmethod
    def _assert_ok(reply: dict) -> dict:
        if not isinstance(reply, dict) or reply.get("ok") is not True:
            error = (reply or {}).get("error") if isinstance(reply, dict) else None
            if error is None:
                error = {"code": CODE_INTERNAL_ERROR, "message": "unexpected reply shape"}
            raise AcpError.from_body(error)
        return reply


def _pick_highest_supported(data: Any) -> Optional[str]:
    """Picks the highest version from a 40003 ``data.supported`` payload."""
    supported = (data or {}).get("supported") if isinstance(data, dict) else None
    if not isinstance(supported, list):
        return None
    versions = []
    from .codec import parse_version

    for v in supported:
        if isinstance(v, str) and parse_version(v) is not None:
            versions.append((parse_version(v), v))
    if not versions:
        return None
    versions.sort(reverse=True)
    return versions[0][1]
