"""AcpServer core (spec v0.2 §3-6, §12): transport-agnostic op routing, schema
validation, streaming, reserved ops (``$ping``/``$subscribe``/``$unsubscribe``),
and event fan-out.

Mirrors packages/acp-sdk-ts/src/server.ts. The Python server is a synchronous
facade backed by thread-based transports:

- WebSocket: ``websockets.sync.server`` on ``listen()``'s port;
- HTTP: stdlib ``http.server.ThreadingHTTPServer`` on the adjacent port
  (``server.http_port``). The TS SDK serves both on one port via Node's http
  server; Python's ``websockets`` HTTP parser only accepts GET requests, so a
  POST endpoint cannot share the WS port — hence the sanctioned fallback.
- Component handlers of any shape run on the calling thread (sync) or on a
  private event-loop thread (async handlers / async generators).
"""

import asyncio
import inspect
import json
import jsonschema
import threading
import time
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional

from websockets.datastructures import Headers
from websockets.http11 import Response
from websockets.sync.server import serve as ws_serve

from .codec import (
    PROTOCOL_VERSION,
    error_envelope,
    is_version_supported,
    validate_envelope,
)
from .component import CallContext, ComponentDef
from .errors import (
    AcpError,
    acp_code_to_http_status,
    CODE_COMPONENT_ERROR,
    CODE_COMPONENT_NOT_FOUND,
    CODE_EVENT_UNSUPPORTED,
    CODE_INTERNAL_ERROR,
    CODE_INVALID_ENVELOPE,
    CODE_INVALID_INPUT,
    CODE_INVALID_OUTPUT,
    CODE_METHOD_NOT_ALLOWED,
    CODE_PARSE_ERROR,
    CODE_STREAM_REQUIRED,
    CODE_SUBSCRIPTION_LIMIT,
    CODE_UNSUPPORTED_VERSION,
)
from .registry import Registry
from .transports.base import Connection, TransportLifecycle
from .types import AcpServerInfo


class _ConnState:
    __slots__ = ("subscriptions",)

    def __init__(self) -> None:
        self.subscriptions: Dict[str, dict] = {}


def _is_http_conn(conn: Connection) -> bool:
    return conn.meta.get("transport") == "http"


def _status_of(error_frame: dict) -> int:
    return acp_code_to_http_status(error_frame["error"]["code"])


class AcpServer:
    """Synchronous ACP v0.2 server facade.

    Example::

        server = AcpServer(name="edge-node-01", version="1.0.0")

        @server.component(id="sensor.temperature", name="Temperature Sensor",
                          tags=["iot", "sensor"])
        def read_temperature(input, ctx):
            return {"celsius": 23.5}

        port = server.listen()          # WS on `port`, HTTP on `port + 1`
        server.serve_forever()          # block
    """

    def __init__(
        self,
        name: str,
        version: str = "0.0.0",
        protocol_version: str = PROTOCOL_VERSION,
        validate_input: bool = True,
        validate_output: bool = False,
        max_subscriptions_per_conn: int = 64,
        queue_limit: int = 256,
    ) -> None:
        self._registry = Registry()
        self._name = name
        self._version = version
        self._protocol_version = protocol_version
        self._validate_input = validate_input
        self._validate_output = validate_output
        self._max_subscriptions_per_conn = max_subscriptions_per_conn
        self._queue_limit = queue_limit
        self._conns: Dict[Connection, _ConnState] = {}
        self._conn_lock = threading.RLock()
        self._sub_seq = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._loop_ready = threading.Event()
        self._ws_server = None
        self._ws_thread: Optional[threading.Thread] = None
        self._http_server = None
        self._http_port = 0
        self._shutdown_event = threading.Event()

    # ------------------------------------------------------------------
    # Registry
    # ------------------------------------------------------------------

    def register(self, def_: ComponentDef) -> "AcpServer":
        self._registry.register(def_)
        return self

    def component(
        self,
        id: str,
        name: str,
        description: str = "",
        version: str = "0.0.0",
        input_schema: Optional[dict] = None,
        output_schema: Optional[dict] = None,
        stream: bool = False,
        tags: Optional[List[str]] = None,
        meta: Optional[dict] = None,
    ) -> Callable[[Callable], ComponentDef]:
        """Decorator registering a handler as a component (spec §7)."""

        def decorator(fn: Callable) -> ComponentDef:
            def_ = ComponentDef(
                id=id,
                name=name,
                description=description,
                version=version,
                input_schema=input_schema,
                output_schema=output_schema,
                stream=stream,
                tags=tags,
                meta=meta,
                handle=fn,
            )
            self.register(def_)
            return def_

        return decorator

    @property
    def descriptors(self) -> List[dict]:
        return self._registry.descriptors()

    @property
    def server_info(self) -> AcpServerInfo:
        return AcpServerInfo(name=self._name, version=self._version, protocol=self._protocol_version)

    @property
    def protocol_version(self) -> str:
        return self._protocol_version

    @property
    def http_port(self) -> int:
        """Port of the HTTP transport (WS port + 1); see module docstring."""
        return self._http_port

    # ------------------------------------------------------------------
    # Connection registry (stateful transports only; HTTP never attaches)
    # ------------------------------------------------------------------

    def attach_connection(self, conn: Connection) -> None:
        with self._conn_lock:
            if conn not in self._conns:
                self._conns[conn] = _ConnState()

    def detach_connection(self, conn: Connection) -> None:
        with self._conn_lock:
            self._conns.pop(conn, None)

    @property
    def connection_lifecycle(self) -> TransportLifecycle:
        """Lifecycle for wiring custom transports (stdio pair tests etc.)."""
        return TransportLifecycle(on_connection=self.attach_connection, on_disconnect=self.detach_connection)

    # ------------------------------------------------------------------
    # Core dispatch (spec §3.2 validation order)
    # ------------------------------------------------------------------

    def handle(self, raw: Any, conn: Connection) -> None:
        """Routes an already-JSON-parsed request; replies (including stream
        chunks) go over ``conn``. Blocks until every frame is sent.
        Responses echo the request's ``acp`` value (spec v0.2 §5.3)."""
        validation = validate_envelope(raw)
        if not validation.ok:
            acp = raw.get("acp") if (validation.id and isinstance(raw, dict)) else None
            conn.send(
                error_envelope(validation.id, validation.code, validation.message or "invalid envelope", validation.data, acp)
            )
            return

        req = validation.request
        assert req is not None
        if not is_version_supported(req["acp"], self._protocol_version):
            conn.send(
                error_envelope(
                    req["id"],
                    CODE_UNSUPPORTED_VERSION,
                    "unsupported protocol version",
                    {"supported": [self._protocol_version]},
                    req["acp"],
                )
            )
            return

        try:
            op = req["op"]
            if op == "discover":
                self._discover(req, conn)
            elif op == "call":
                self._call(req, conn)
            elif op == "$ping":
                self._ping(req, conn)
            elif op == "$subscribe":
                self._subscribe(req, conn)
            elif op == "$unsubscribe":
                self._unsubscribe(req, conn)
        except AcpError as e:
            conn.send(error_envelope(req["id"], e.code, e.message, e.data, req["acp"]))
        except Exception as e:
            conn.send(
                error_envelope(req["id"], CODE_COMPONENT_ERROR, "component handler threw: {}".format(e), None, req["acp"])
            )

    def _ping(self, req: dict, conn: Connection) -> None:
        input_ = req.get("input")
        result: dict = {"pong": int(time.time() * 1000)}
        if isinstance(input_, dict) and isinstance(input_.get("ts"), (int, float)):
            result["ts"] = input_["ts"]
        conn.send({"acp": req["acp"], "id": req["id"], "ok": True, "result": result})

    def _subscribe(self, req: dict, conn: Connection) -> None:
        if _is_http_conn(conn):
            conn.send(
                error_envelope(
                    req["id"], CODE_EVENT_UNSUPPORTED, "events unsupported on connectionless transport", None, req["acp"]
                )
            )
            return
        with self._conn_lock:
            state = self._conns.get(conn)
            if state is None:
                conn.send(
                    error_envelope(req["id"], CODE_INTERNAL_ERROR, "connection not registered", None, req["acp"])
                )
                return
            if len(state.subscriptions) >= self._max_subscriptions_per_conn:
                conn.send(
                    error_envelope(
                        req["id"],
                        CODE_SUBSCRIPTION_LIMIT,
                        "subscription limit reached ({})".format(self._max_subscriptions_per_conn),
                        None,
                        req["acp"],
                    )
                )
                return
            input_ = req.get("input") or {}
            self._sub_seq += 1
            sub_id = "s-{:x}".format(self._sub_seq)
            sub: dict = {"id": sub_id}
            if "component" in input_:
                sub["component"] = input_["component"]
            else:
                sub["tags"] = input_["tags"]
            state.subscriptions[sub_id] = sub
        conn.send({"acp": req["acp"], "id": req["id"], "ok": True, "result": {"subscription": sub_id}})

    def _unsubscribe(self, req: dict, conn: Connection) -> None:
        if _is_http_conn(conn):
            conn.send(
                error_envelope(
                    req["id"], CODE_EVENT_UNSUPPORTED, "events unsupported on connectionless transport", None, req["acp"]
                )
            )
            return
        with self._conn_lock:
            state = self._conns.get(conn)
            if state is not None:
                input_ = req.get("input")
                if not input_:
                    state.subscriptions.clear()
                else:
                    for sub_id, sub in list(state.subscriptions.items()):
                        if "component" in input_:
                            matched = sub.get("component") == input_["component"]
                        else:
                            sub_tags = sub.get("tags")
                            matched = sub_tags is not None and all(t in sub_tags for t in input_.get("tags", []))
                        if matched:
                            state.subscriptions.pop(sub_id, None)
        conn.send({"acp": req["acp"], "id": req["id"], "ok": True, "result": None})

    def emit(self, component: Optional[str] = None, tags: Optional[List[str]] = None, data: Any = None, ts: Optional[int] = None) -> None:
        """Pushes an ``$event`` to every matching subscription on every stateful
        connection (spec v0.2 §6.2). Best-effort, at-most-once; bounded queues.

        Matches by component equality, or by tags subset (subscription tags
        ⊆ event tags). Event tags default to the component's descriptor tags.
        """
        if tags is None and component is not None:
            def_ = self._registry.get(component)
            tags = def_.tags if def_ is not None else None
        if component is None and not tags:
            return
        event: dict = {}
        if component is not None:
            event["component"] = component
        if tags is not None:
            event["tags"] = list(tags)
        event["data"] = data
        if ts is not None:
            event["ts"] = ts
        frame = {"acp": self._protocol_version, "id": None, "event": event}

        with self._conn_lock:
            entries = list(self._conns.items())
        for conn, state in entries:
            with self._conn_lock:
                subs = list(state.subscriptions.values())
            if not subs:
                continue
            matched = False
            for sub in subs:
                if sub.get("component") is not None:
                    if sub["component"] == component:
                        matched = True
                        break
                elif sub.get("tags") is not None and tags is not None:
                    if all(t in tags for t in sub["tags"]):
                        matched = True
                        break
            if not matched:
                continue
            backlog = conn.event_backlog() if conn.event_backlog is not None else 0
            if backlog >= self._queue_limit:
                continue  # drop new events when backlogged
            conn.send(frame)

    # ------------------------------------------------------------------
    # discover / call
    # ------------------------------------------------------------------

    def _discover(self, req: dict, conn: Connection) -> None:
        descriptors = self._registry.descriptors()
        component = req.get("component")
        if component is not None:
            descriptors = [d for d in descriptors if d["id"] == component]
        tags = req.get("tags")
        if tags is not None:
            descriptors = [d for d in descriptors if all(t in (d.get("tags") or []) for t in tags)]
        conn.send(
            {
                "acp": req["acp"],
                "id": req["id"],
                "ok": True,
                "result": {"server": self._server_info_dict(), "components": descriptors},
            }
        )

    def _server_info_dict(self) -> dict:
        return {"name": self._name, "version": self._version, "protocol": self._protocol_version}

    def _call(self, req: dict, conn: Connection) -> None:
        component = req.get("component")
        def_ = self._registry.get(component) if component else None
        if def_ is None:
            conn.send(
                error_envelope(req["id"], CODE_COMPONENT_NOT_FOUND, "component not found: {}".format(component), None, req["acp"])
            )
            return

        if self._validate_input and def_.input_schema is not None:
            input_value = req.get("input")
            errors = self._validate(def_.input_schema, input_value)
            if errors:
                conn.send(
                    error_envelope(req["id"], CODE_INVALID_INPUT, "input validation failed", {"errors": errors}, req["acp"])
                )
                return

        handle = def_.handle
        assert handle is not None
        if inspect.isasyncgenfunction(handle) or asyncio.iscoroutinefunction(handle):
            # Async handlers (and async generators) run on the server's loop.
            self._ensure_loop()
            assert self._loop is not None
            future = asyncio.run_coroutine_threadsafe(self._run_call_async(req, conn, def_), self._loop)
            future.result()
            return

        # Sync handlers run on the calling thread.
        ctx = self._make_context(req, conn, def_)
        result = handle(req.get("input"), ctx)
        if inspect.isawaitable(result):
            # Handler returned an awaitable without being a coroutine function.
            self._ensure_loop()
            assert self._loop is not None
            future = asyncio.run_coroutine_threadsafe(self._finish_async(req, conn, def_, result), self._loop)
            future.result()
            return
        self._finish_call(req, conn, def_, result, ctx)

    async def _run_call_async(self, req: dict, conn: Connection, def_: ComponentDef) -> None:
        ctx = self._make_context(req, conn, def_)
        result = def_.handle(req.get("input"), ctx)  # type: ignore[misc]
        if inspect.isawaitable(result):
            result = await result
        await self._finish_call_async(req, conn, def_, result)

    async def _finish_async(self, req: dict, conn: Connection, def_: ComponentDef, result: Any) -> None:
        result = await result
        await self._finish_call_async(req, conn, def_, result)

    def _finish_call(self, req: dict, conn: Connection, def_: ComponentDef, result: Any, ctx: CallContext) -> None:
        if inspect.isgenerator(result):
            if req.get("stream") is not True:
                result.close()
                self._send_stream_required(req, conn, def_)
                return
            seq = 0
            try:
                for value in result:
                    conn.send({"acp": req["acp"], "id": req["id"], "chunk": {"seq": seq, "end": False, "data": value}})
                    seq += 1
            except AcpError as e:
                conn.send(error_envelope(req["id"], e.code, e.message, e.data, req["acp"]))
                return
            except Exception as e:
                conn.send(
                    error_envelope(req["id"], CODE_COMPONENT_ERROR, "component handler threw: {}".format(e), None, req["acp"])
                )
                return
            conn.send({"acp": req["acp"], "id": req["id"], "chunk": {"seq": seq, "end": True, "data": None}})
            return
        self._send_result(req, conn, def_, result)

    async def _finish_call_async(self, req: dict, conn: Connection, def_: ComponentDef, result: Any) -> None:
        if inspect.isasyncgen(result):
            if req.get("stream") is not True:
                self._send_stream_required(req, conn, def_)
                return
            seq = 0
            try:
                async for value in result:
                    conn.send({"acp": req["acp"], "id": req["id"], "chunk": {"seq": seq, "end": False, "data": value}})
                    seq += 1
            except AcpError as e:
                conn.send(error_envelope(req["id"], e.code, e.message, e.data, req["acp"]))
                return
            except Exception as e:
                conn.send(
                    error_envelope(req["id"], CODE_COMPONENT_ERROR, "component handler threw: {}".format(e), None, req["acp"])
                )
                return
            conn.send({"acp": req["acp"], "id": req["id"], "chunk": {"seq": seq, "end": True, "data": None}})
            return
        self._send_result(req, conn, def_, result)

    def _send_stream_required(self, req: dict, conn: Connection, def_: ComponentDef) -> None:
        conn.send(
            error_envelope(
                req["id"], CODE_STREAM_REQUIRED, "component {} requires stream:true".format(def_.id), None, req["acp"]
            )
        )

    def _send_result(self, req: dict, conn: Connection, def_: ComponentDef, result: Any) -> None:
        if self._validate_output and def_.output_schema is not None:
            errors = self._validate(def_.output_schema, result)
            if errors:
                conn.send(
                    error_envelope(req["id"], CODE_INVALID_OUTPUT, "output validation failed", {"errors": errors}, req["acp"])
                )
                return
        if req.get("stream") is True:
            # Non-streaming component called with stream:true: wrap in a single
            # terminated chunk (spec §4.2 — uniform handling, no special cases).
            conn.send({"acp": req["acp"], "id": req["id"], "chunk": {"seq": 0, "end": True, "data": result}})
            return
        conn.send({"acp": req["acp"], "id": req["id"], "ok": True, "result": result})

    def _make_context(self, req: dict, conn: Connection, def_: ComponentDef) -> CallContext:
        server = self

        def emit(data: Any = None, tags: Optional[List[str]] = None, ts: Optional[int] = None) -> None:
            server.emit(component=def_.id, tags=tags, data=data, ts=ts)

        return CallContext(conn=conn, request=req, meta=req.get("meta"), signal=asyncio.Event(), emit=emit)

    def _validate(self, schema: dict, instance: Any) -> List[str]:
        """Draft-07 validation collecting all errors (spec §7: draft-07)."""
        validator = jsonschema.Draft7Validator(schema)
        formatted: List[str] = []
        for error in validator.iter_errors(instance):
            path = "input"
            for part in error.absolute_path:
                if isinstance(part, int):
                    path += "[{}]".format(part)
                else:
                    path += ".{}".format(part)
            formatted.append("{} {}".format(path, error.message))
        return formatted

    # ------------------------------------------------------------------
    # Lifecycle: WS on `port`, HTTP on `port + 1` (see module docstring)
    # ------------------------------------------------------------------

    def listen(self, port: int = 0, host: str = "127.0.0.1") -> int:
        """Starts WebSocket + HTTP transports and returns the WS port.

        WebSocket listens on ``port`` (0 = ephemeral); HTTP listens on the
        first free adjacent port (``ws_port + 1`` by default) because Python's
        ``websockets`` server cannot serve POST bodies — query
        :attr:`http_port` for the actual HTTP port. Clients connect with
        ``ws://host:<listen port>/acp`` and ``http://host:<http_port>/acp``.
        """
        if self._ws_server is not None:
            raise RuntimeError("server already listening")
        self._ws_server = ws_serve(self._handle_ws_connection, host, port, process_request=self._process_request)
        ws_port = self._ws_server.socket.getsockname()[1]
        # The threaded websockets server needs an accept loop of its own.
        self._ws_thread = threading.Thread(
            target=self._ws_server.serve_forever, daemon=True, name="acp-ws-accept"
        )
        self._ws_thread.start()
        last_error: Optional[Exception] = None
        for offset in range(1, 11):
            try:
                self._http_server = _make_http_server(self, host, ws_port + offset)
                self._http_port = ws_port + offset
                last_error = None
                break
            except OSError as e:
                last_error = e
        if self._http_server is None:
            self._ws_server.shutdown()
            self._ws_server = None
            raise RuntimeError("could not bind an HTTP port next to {}: {}".format(ws_port, last_error))
        return ws_port

    def serve_forever(self) -> None:
        """Blocks until :meth:`shutdown` is called."""
        while not self._shutdown_event.wait(timeout=1.0):
            pass

    def shutdown(self) -> None:
        """Stops every started transport."""
        self._shutdown_event.set()
        if self._http_server is not None:
            self._http_server.stop()
            self._http_server = None
            self._http_port = 0
        if self._ws_server is not None:
            self._ws_server.shutdown()
            self._ws_server = None

    # -- WebSocket server ---------------------------------------------------

    def _handle_ws_connection(self, ws: Any) -> None:
        remote = ws.remote_address or ("?", 0)
        conn = Connection(
            meta={"transport": "ws", "ip": remote[0]},
            send=lambda msg: ws.send(json.dumps(msg)),
            close=ws.close,
        )
        self.attach_connection(conn)
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
                self.handle(parsed, conn)
        finally:
            self.detach_connection(conn)

    def _process_request(self, connection: Any, request: Any) -> Optional[Response]:
        """Serves plain-HTTP GET probes on the WS port (spec §9.1)."""
        path = request.path.split("?", 1)[0]
        if (request.headers.get("Upgrade") or "").lower() == "websocket":
            return None  # proceed with the WebSocket opening handshake
        if path == "/acp/discover":
            frames: List[dict] = []
            conn = Connection(meta={"transport": "ws"}, send=frames.append, close=lambda: None)
            self.handle(
                {"acp": PROTOCOL_VERSION, "id": "get-{}".format(int(time.time() * 1000)), "op": "discover"}, conn
            )
            reply = frames[0] if frames else error_envelope(None, CODE_INTERNAL_ERROR, "no reply")
            if reply.get("ok") is False:
                return _response(_status_of(reply), json.dumps(reply), "application/json")
            return _response(200, json.dumps(reply.get("result")), "application/json")
        if path == "/acp/health":
            return _response(200, json.dumps({"ok": True}), "application/json")
        if path == "/acp":
            frame = error_envelope(None, CODE_METHOD_NOT_ALLOWED, "method {} not allowed on /acp".format(request.method))
            return _response(_status_of(frame), json.dumps(frame), "application/json")
        return _response(404, json.dumps({"ok": False}), "application/json")

    # -- background event loop (async handlers / async generators) ----------

    def _ensure_loop(self) -> None:
        if self._loop is not None:
            return
        ready = self._loop_ready

        def run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            ready.set()
            loop.run_forever()

        self._loop_thread = threading.Thread(target=run, daemon=True, name="acp-server-loop")
        self._loop_thread.start()
        ready.wait(timeout=5)


def _response(status: int, body: str, content_type: str) -> Response:
    return Response(status, HTTPStatus(status).phrase, Headers({"Content-Type": content_type}), body.encode("utf-8"))


def _make_http_server(server: "AcpServer", host: str, port: int):
    """Builds the stdlib HTTP server for POST /acp on an adjacent port."""
    from .transports.http import HttpServerTransport

    transport = HttpServerTransport(port=port, host=host)
    transport.start(server.handle, None)
    return transport
