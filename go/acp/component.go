package acp

import (
	"context"
	"encoding/json"
)

// HandleFunc is a component handler. `input` is the decoded call input;
// `send` is non-nil only when the request carried stream:true — streaming
// components deliver shards through it. A component that requires streaming
// but receives a nil Sender returns ErrStreamRequired (the server replies
// 40005).
//
// The returned value is the call result: in non-stream mode it becomes the
// bare `result`; in stream mode, if at least one chunk was sent, the return
// value is ignored and the server appends the {seq:n, end:true} terminator,
// otherwise the output is wrapped in a single terminated chunk.
type HandleFunc func(ctx context.Context, input any, send Sender) (any, error)

// Sender sends one streaming chunk (spec §6.1). Chunks are framed with
// strictly increasing seq numbers and end:false; the server appends the
// mandatory end:true terminator after the handler returns.
type Sender func(chunk any) error

// BinaryChunk wraps a base64 string so that passing it to a Sender emits a
// `bin: true` chunk (spec §6.1).
type BinaryChunk struct {
	Bin  bool   `json:"bin"`
	Data string `json:"data"`
}

// NewBinaryChunk builds a BinaryChunk for Sender(binChunk(b64)).
func NewBinaryChunk(base64 string) BinaryChunk {
	return BinaryChunk{Bin: true, Data: base64}
}

// Component is a named callable unit (spec §7). Schemas are JSON Schema
// draft-07 documents; InputSchema nil (or empty) means "any input".
type Component struct {
	ID          string
	Name        string
	Description string
	// Version is the component's own semantic version, independent of the
	// protocol version; defaults to "0.0.0" in descriptors.
	Version string
	// InputSchema / OutputSchema are draft-07 JSON Schemas as raw JSON.
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
	Stream       bool
	Tags         []string
	Meta         map[string]any
	Handle       HandleFunc
}

// descriptor projects the component into its discover descriptor.
func (c Component) descriptor() ComponentDescriptor {
	v := c.Version
	if v == "" {
		v = "0.0.0"
	}
	return ComponentDescriptor{
		ID:           c.ID,
		Name:         c.Name,
		Description:  c.Description,
		Version:      v,
		InputSchema:  c.InputSchema,
		OutputSchema: c.OutputSchema,
		Stream:       c.Stream,
		Tags:         c.Tags,
		Meta:         c.Meta,
	}
}
