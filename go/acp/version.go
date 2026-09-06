// Package acp implements the ACP (Agent-Component-Protocol) v0.2 specification
// (spec/ACP-0.2-SPEC.md): a minimal JSON envelope protocol for remotely calling
// components over HTTP, WebSocket, stdio or in-process connections.
//
// The package mirrors the TypeScript reference implementation in
// packages/acp-sdk-ts/src and passes the same conformance suite semantics.
package acp

// ProtocolVersion is the ACP protocol version served and declared by this SDK
// (spec v0.2).
const ProtocolVersion = "0.2"
