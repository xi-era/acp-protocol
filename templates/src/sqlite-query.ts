/**
 * 模板:SQLite 数据库 → ACP 元件(只读查询)
 *
 * 用 Node 22 内建的 node:sqlite(零依赖)把一个 SQLite 库暴露成 db.query 元件。
 * 内置只读防护:仅允许 SELECT / WITH 开头的单条语句,杜绝写操作与多语句注入。
 *
 * 启动:pnpm start:sqlite-query(或 node --experimental-strip-types src/sqlite-query.ts)
 */
import { createRequire } from "node:module";
import type Sqlite from "node:sqlite";
import { AcpServer, defineComponent } from "@xi-era/acp-sdk/server";

// node:sqlite 在 Node 22.5+ 可用;经 createRequire 引入以兼容打包/测试工具的解析器
const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof Sqlite;

// ★ 改这里:你的数据库文件(默认用内存库 + 示例表)
const DB_PATH = process.env["ACP_DB_PATH"] ?? ":memory:";
const db = new DatabaseSync(DB_PATH);
db.exec("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT, year INTEGER)");
if (db.prepare("SELECT COUNT(*) AS n FROM books").get() !== undefined) {
  const { n } = db.prepare("SELECT COUNT(*) AS n FROM books").get() as { n: number };
  if (n === 0) {
    db.prepare("INSERT INTO books (title, year) VALUES (?, ?)").run("Designing Data-Intensive Applications", 2017);
    db.prepare("INSERT INTO books (title, year) VALUES (?, ?)").run("The Pragmatic Programmer", 1999);
  }
}

/** 只读白名单:拒绝写操作、多语句、PRAGMA 等危险前缀 */
function assertReadOnly(sql: string): void {
  const trimmed = sql.trim().replace(/;+\s*$/, "");
  if (/;/.test(trimmed)) throw new Error("multiple statements are not allowed");
  if (!/^(select|with)\b/i.test(trimmed)) throw new Error("only SELECT / WITH queries are allowed");
}

export const sqliteQuery = defineComponent({
  id: "db.query",
  name: "SQLite Query",
  description: "Runs a read-only SQL query against the template SQLite database",
  version: "1.0.0",
  inputSchema: { type: "object", properties: { sql: { type: "string" } }, required: ["sql"] },
  tags: ["db", "template"],
  async handle(input: { sql: string }) {
    assertReadOnly(input.sql);
    const rows = db.prepare(input.sql).all();
    return { rows, count: rows.length };
  },
});

const isMain =
  process.argv[1]?.replace(/\\/g, "/").endsWith("sqlite-query.ts") ||
  process.argv[1]?.replace(/\\/g, "/").endsWith("sqlite-query.js");
if (isMain) {
  const server = new AcpServer({ name: "sqlite-node", version: "1.0.0" });
  server.register(sqliteQuery);
  server
    .listen({ port: Number(process.env["ACP_PORT"] ?? 8092) })
    .then(({ port }) => console.log(`SQLite ACP endpoint: http://localhost:${port}/acp`));
}
