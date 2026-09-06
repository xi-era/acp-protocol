# Changelog

All notable changes to ACP will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-06

Protocol evolution per [roadmap v0.2](docs/roadmap.md): heartbeat, compression, multi-language SDKs, ecosystem.

### Added — Protocol (spec/ACP-0.2-SPEC.md)

- **`$ping` reserved op** — bidirectional application-layer keepalive (works on all transports incl. stateless HTTP); recommended params `keepAliveMs=30000` / `pongTimeoutMs=10000`; 0.1 servers answer `40002` and clients permanently disable keepalive
- **`$subscribe` / `$unsubscribe` / `$event`** — connection-scoped event subscriptions with best-effort at-most-once delivery; stateful transports only (WS / Stdio / Memory)
- **Compression** — standard mechanisms only: HTTP `Content-Encoding: gzip` (threshold ≥1024 bytes, uncompressed path always available) and WS `permessage-deflate`; zero envelope semantic changes
- **Error codes** `42902 SUBSCRIPTION_LIMIT`, `50100 EVENT_UNSUPPORTED` (maps to HTTP 501)
- **Compatibility matrix + client fallback ladder**: declare "0.2" → on 40003 retry once with the highest supported version; `$ping` → 40002 disables keepalive permanently
- Spec clarification: responses MUST echo the request's `acp` value

### Added — SDKs

- **`@xi-era/acp-sdk` 0.2.0** (TypeScript): server `emit()` + per-connection subscription registry with `TransportLifecycle` hooks; client `subscribe()` with auto-resubscribe on reconnect, keepalive timers, gzip request/response on the HTTP transport, permessage-deflate on WS, version fallback; `acp ping` CLI command
- **`acp-protocol-sdk` 0.2.0** (Python, import `acp`): sync-first API mirroring the TS SDK (`@server.component` decorator, discover/call/call_stream/subscribe), HTTP+WS on one port via `websockets` (sole runtime dependency), stdio/memory transports, `$ping` keepalive, event subscriptions; extras: `[aio]` (aiohttp async client), `[langchain]` (ACP components → LangChain StructuredTool with a draft-07 → pydantic args-schema converter)
- **`github.com/xi-era/acp-protocol/go` 0.2.0** (Go, `go/acp`): context-first API (`Server.Handler()` serves HTTP+WS on one port, `iter.Seq2` streaming, `Emit`, `Subscribe`, keepalive), `coder/websocket` with built-in permessage-deflate, cross-transport conformance tests

### Added — Ecosystem

- **Component template library** (`templates/`): copy-paste sources for HTTP-proxy, SQLite-query, and mock-IoT components in TypeScript and Python, plus `$event` demo in the IoT example
- **Cursor bridge guide** (`docs/guides/cursor-mcp-bridge.md`): mount remote ACP components in Cursor / Claude Desktop via `acp-mcp`
- **Cross-SDK interop suite** (`scripts/interop/`): Python↔TypeScript and Go↔TypeScript conformance runs in CI

## [0.1.0] - 2026-09-05

First public milestone: protocol specification + TypeScript implementation.

### Added

- **`spec/ACP-0.1-SPEC.md`** — the protocol specification (the project's core asset):
  - Single-envelope dual-semantics JSON (no JSON-RPC), `ok` field for success/failure
  - `discover` (list / single / tags variants) and `call` ops
  - Streaming chunk frames (`seq` / `end` / `data` / base64 `bin`)
  - Component model: `component_id` grammar, draft-07 JSON Schema descriptors
  - 5-digit error-code system aligned with HTTP semantic classes (private range 59000–59999)
  - Transport mappings: HTTP (POST /acp, NDJSON streaming), WebSocket (per-message
    version negotiation, id multiplexing), Stdio
  - Security reservation (`meta`: auth / scopes / traceId / timeoutMs — ignored in v0.1)
  - Appendices: MCP bidirectional mapping, OpenAI Tool-Call mapping

- **`@xi-era/acp-sdk`** — TypeScript SDK:
  - Server: `defineComponent` / `AcpServer.register` / `listen()` (HTTP+WS on one
    port) / `serveStdio()`; ajv input validation, dev-time output validation,
    async-generator streaming with automatic seq numbering
  - Client: `discover` / `call` / `callStream` / `request` over HTTP, WebSocket,
    Stdio or in-process Memory transports; 50400 timeout errors
  - Cross-transport conformance suite (the executable annotation of the spec)

- **`@xi-era/acp-cli`** — `acp discover / describe / call / info / serve` with
  `--trace`, `--raw`, `--stream`, tag filters and a `--watch` dev server.

- **`@xi-era/acp-adapter-openai`** — ACP → OpenAI Tool-Call: descriptor → function
  tool conversion and a tool_calls execution handler.

- **`@xi-era/acp-adapter-mcp`** — ACP ↔ MCP bidirectional bridge:
  `serveMcpFromAcp` (mount ACP in Claude Desktop / Cursor over MCP stdio),
  `mcpToolsToComponents` (wrap MCP tools as ACP components), `acp-mcp` CLI bin.

- **Examples** — HTTP backend wrapped as components, virtual IoT sensor with
  streamed readings, and a client demo using all three integration paths.
