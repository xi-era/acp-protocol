"""模板:SQLite 数据库 → ACP 元件(只读查询,Python 版)。

用标准库 sqlite3(零依赖)。内置只读防护。
启动:python templates/python/sqlite_query.py
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any

from acp import AcpServer
from acp.component import ComponentDef

DB_PATH = os.environ.get("ACP_DB_PATH", ":memory:")
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT, year INTEGER)")
db.execute("INSERT OR IGNORE INTO books (title, year) VALUES ('Designing Data-Intensive Applications', 2017)")
db.execute("INSERT OR IGNORE INTO books (title, year) VALUES ('The Pragmatic Programmer', 1999)")
db.commit()


def assert_read_only(sql: str) -> None:
    trimmed = sql.strip().rstrip(";").strip()
    if ";" in trimmed:
        raise ValueError("multiple statements are not allowed")
    if not trimmed.lower().startswith(("select", "with")):
        raise ValueError("only SELECT / WITH queries are allowed")


def handle_query(inp: Any, ctx) -> dict[str, Any]:  # noqa: ANN001
    sql = (inp or {}).get("sql", "")
    assert_read_only(sql)
    rows = [dict(zip([c[0] for c in cur.description], row)) for cur in [db.execute(sql)] for row in cur.fetchall()]
    return {"rows": rows, "count": len(rows)}


sqlite_query = ComponentDef(
    id="db.query",
    name="SQLite Query",
    description="Runs a read-only SQL query against the template SQLite database",
    input_schema={"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]},
    tags=["db", "template"],
    handle=handle_query,
)

if __name__ == "__main__":
    server = AcpServer(name="sqlite-node", version="1.0.0")
    server.register(sqlite_query)
    ws_port = server.listen(port=int(os.environ.get("ACP_PORT", "8092")))
    print(f"HTTP ACP endpoint: http://localhost:{server.http_port}/acp  (WS: ws://localhost:{ws_port}/acp)")
    server.serve_forever()
