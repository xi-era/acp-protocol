"""Transport abstraction (spec §9-11): transports move bytes, nothing else.

Op routing, validation and streaming live in the server core. Unlike the
TypeScript SDK (async transports), the Python SDK's transports are
**synchronous and thread-based**: the dispatch callable blocks until every
reply frame has been sent through the connection.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable

#: Server-side handle on one client connection.
#: ``send`` pushes a response/chunk/error/event frame (dict) to the client.
@dataclass(eq=False)
class Connection:
    #: Remote metadata (transport-specific, e.g. ``{"transport": "ws", "ip": ...}``).
    meta: Dict[str, Any] = field(default_factory=dict)
    #: Push a response/chunk/error/event frame to the client.
    send: Callable[[dict], None] = field(default=lambda msg: None)
    #: Close the underlying connection.
    close: Callable[[], Any] = field(default=lambda: None)
    #: Number of queued outgoing event frames, for bounded delivery (spec
    #: v0.2 §6.2). Optional per transport.
    event_backlog: Optional[Callable[[], int]] = None


#: Core dispatch: takes an already-JSON-parsed request object, routes it, and
#: sends every reply frame through the connection. Blocks until done.
ServerDispatch = Callable[[Any, Connection], None]


@dataclass
class TransportLifecycle:
    """Connection lifecycle hooks for stateful transports (spec v0.2 §4.4).

    HTTP never calls these — every POST is an ephemeral connection.
    """

    on_connection: Optional[Callable[[Connection], None]] = None
    on_disconnect: Optional[Callable[[Connection], None]] = None


@runtime_checkable
class ServerTransport(Protocol):
    """Server-side transport; starts listening and feeds envelopes to dispatch."""

    def start(self, dispatch: ServerDispatch, lifecycle: Optional[TransportLifecycle] = None) -> None:
        ...

    def stop(self) -> None:
        ...


@runtime_checkable
class ClientTransport(Protocol):
    """Client-side transport.

    ``request`` blocks until the terminal reply frame and returns it;
    ``request_stream`` yields chunk frames until the end frame, raising
    AcpError when an error frame terminates the stream.
    ``events_supported`` / ``on_event`` back $subscribe/$event (spec v0.2 §4.4);
    connectionless transports (HTTP) report False. Incoming server-initiated
    ``$ping`` frames are auto-answered by stateful transports.
    """

    def connect(self) -> None:
        ...

    def request(self, req: dict, timeout: Optional[float] = None) -> dict:
        ...

    def request_stream(self, req: dict, timeout: Optional[float] = None):
        ...

    def close(self) -> None:
        ...

    def events_supported(self) -> bool:
        ...

    def on_event(self, handler: Callable[[dict], None]) -> Callable[[], None]:
        ...
