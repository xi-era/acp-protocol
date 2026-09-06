"""ACP (Agent-Component-Protocol) Python SDK — spec v0.2.

Homepage: https://github.com/xi-era/acp-protocol (spec/ACP-0.2-SPEC.md).
"""

from .client import AcpClient, AcpSubscription
from .errors import AcpError, AcpErrorCode, acp_code_to_http_status
from .registry import Registry
from .server import AcpServer
from .types import AcpChunk, AcpEvent, AcpRequest, AcpServerInfo, ComponentDescriptor
from .version import PROTOCOL_VERSION, SDK_VERSION

__version__ = SDK_VERSION

__all__ = [
    "AcpServer",
    "AcpClient",
    "AcpSubscription",
    "AcpError",
    "AcpErrorCode",
    "acp_code_to_http_status",
    "Registry",
    "AcpChunk",
    "AcpEvent",
    "AcpRequest",
    "AcpServerInfo",
    "ComponentDescriptor",
    "PROTOCOL_VERSION",
    "SDK_VERSION",
    "__version__",
]
