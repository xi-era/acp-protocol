"""模板:HTTP API → ACP 元件(通用代理,Python 版)。

在 ENDPOINTS 里声明「元件 id → 上游 URL/方法」即可,零业务代码。
启动:python templates/python/http_proxy.py
"""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

from acp import AcpError, AcpErrorCode, AcpServer
from acp.component import ComponentDef

ENDPOINTS: list[dict[str, Any]] = [
    {
        "id": "http.jsonplaceholder.user",
        "description": "Fetches a user from JSONPlaceholder by id",
        "url": "https://jsonplaceholder.typicode.com/users/{id}",
        "method": "GET",
        "input_schema": {"type": "object", "properties": {"id": {"type": "integer", "minimum": 1}}, "required": ["id"]},
    },
    {
        "id": "http.jsonplaceholder.post",
        "description": "Creates a post on JSONPlaceholder",
        "url": "https://jsonplaceholder.typicode.com/posts",
        "method": "POST",
        "input_schema": {
            "type": "object",
            "properties": {"title": {"type": "string"}, "body": {"type": "string"}},
            "required": ["title"],
        },
    },
]


def _fill_url(url: str, inp: dict[str, Any]) -> str:
    for key, value in inp.items():
        url = url.replace("{" + key + "}", urllib.request.quote(str(value)))
    return url


def build_components() -> list[ComponentDef]:
    defs: list[ComponentDef] = []
    for ep in ENDPOINTS:
        def make_handle(ep: dict[str, Any]):
            def handle(inp: Any, ctx) -> Any:  # noqa: ANN001
                body = json.dumps(inp or {}).encode()
                req = urllib.request.Request(
                    _fill_url(ep["url"], inp or {}),
                    data=body if ep["method"] == "POST" else None,
                    headers={"content-type": "application/json"},
                    method=ep["method"],
                )
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        return json.loads(resp.read().decode())
                except Exception as exc:  # 上游失败 → ACP 语义(spec §8:50002)
                    raise AcpError(AcpErrorCode.UPSTREAM_ERROR, f"upstream error: {exc}") from exc
            return handle

        defs.append(
            ComponentDef(
                id=ep["id"],
                name=ep["id"],
                description=ep["description"],
                input_schema=ep.get("input_schema"),
                tags=["http-proxy"],
                handle=make_handle(ep),
            )
        )
    return defs


if __name__ == "__main__":
    server = AcpServer(name="http-proxy-node", version="1.0.0")
    for comp in build_components():
        server.register(comp)
    ws_port = server.listen(port=int(os.environ.get("ACP_PORT", "8091")))
    print(f"HTTP ACP endpoint: http://localhost:{server.http_port}/acp  (WS: ws://localhost:{ws_port}/acp)")
    server.serve_forever()
