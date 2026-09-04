# Changelog

All notable changes to ACP will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-09-05

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
