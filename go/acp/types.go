package acp

import (
	"encoding/json"
	"time"
)

// Envelope is the single JSON object that is both request and response
// (spec §3). Client-side it is built and marshaled to the wire; server-side it
// is the typed view produced by ValidateEnvelope from a decoded request.
//
// Flexible fields: Input is `any` (may hold map[string]any, []any, string,
// float64, bool, nil) and Meta is the reserved security area whose content
// servers MUST ignore (spec §13).
type Envelope struct {
	// ACP is the protocol identifier + version, e.g. "0.2". Responses MUST
	// echo the request's value (spec v0.2 §5.3).
	ACP string `json:"acp"`
	// ID is the request correlation id; every response frame echoes it.
	// Event frames carry a nil id.
	ID string `json:"id"`
	// Op routes the operation: "discover", "call", "$ping", "$subscribe",
	// "$unsubscribe".
	Op string `json:"op,omitempty"`
	// Component is the target component id (required for "call", optional
	// filter for "discover").
	Component string `json:"component,omitempty"`
	// Tags is the discover tag filter (intersection semantics).
	Tags []string `json:"tags,omitempty"`
	// Input is the call input; must satisfy the component's inputSchema.
	Input any `json:"input,omitempty"`
	// Stream requests streamed chunk replies.
	Stream bool `json:"stream,omitempty"`
	// Meta is the security reservation area; ignored by servers (v0.1+).
	Meta map[string]any `json:"meta,omitempty"`
}

// Chunk is one streamed shard (spec §6.1). The frame with End:true is the
// mandatory terminator; Seq starts at 0 and increases strictly.
type Chunk struct {
	Seq  int  `json:"seq"`
	End  bool `json:"end"`
	Data any  `json:"data"`
	// Bin marks Data as a base64 string (spec §6.1 "bin:true").
	Bin bool `json:"bin,omitempty"`
}

// Event is a server-pushed event payload (spec v0.2 §6.2). At least one of
// Component / Tags must be present; Data may be nil.
type Event struct {
	Component string   `json:"component,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	Data      any      `json:"data"`
	// TS is the server unix-millisecond timestamp; optional.
	TS int64 `json:"ts,omitempty"`
}

// ComponentDescriptor is the discover reply unit (spec §7.2).
type ComponentDescriptor struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	Version      string          `json:"version"`
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Stream       bool            `json:"stream"`
	Tags         []string        `json:"tags,omitempty"`
	Meta         map[string]any  `json:"meta,omitempty"`
}

// ServerInfo is the server self-description included in every discover result.
type ServerInfo struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Protocol string `json:"protocol"`
}

// DiscoverResult is the fixed result shape of the discover op (spec §4.1).
type DiscoverResult struct {
	Server     ServerInfo            `json:"server"`
	Components []ComponentDescriptor `json:"components"`
}

// ErrorBody is the structured error payload of a failure envelope (spec §5.2).
type ErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// frame is a server-to-client message (reply / chunk / error / event) built as
// a generic JSON object. One of ok/error, chunk or event is set; id is a
// string for correlated frames and nil for event frames.
type frame map[string]any

func okFrame(acp, id string, result any) frame {
	return frame{"acp": acp, "id": id, "ok": true, "result": result}
}

func chunkFrame(acp, id string, c Chunk) frame {
	return frame{"acp": acp, "id": id, "chunk": map[string]any{
		"seq": c.Seq, "end": c.End, "data": c.Data, "bin": c.Bin,
	}}
}

func eventFrame(acp string, ev Event) frame {
	event := map[string]any{"data": ev.Data}
	if ev.Component != "" {
		event["component"] = ev.Component
	}
	if ev.Tags != nil {
		event["tags"] = ev.Tags
	}
	if ev.TS != 0 {
		event["ts"] = ev.TS
	}
	return frame{"acp": acp, "id": nil, "event": event}
}

// isErrFrame reports whether a decoded server frame is a failure envelope.
func isErrFrame(f map[string]any) bool {
	ok, present := f["ok"]
	if !present {
		return false
	}
	b, isBool := ok.(bool)
	return isBool && !b
}

// errBodyOf extracts the error body from a decoded failure envelope. The code
// may be float64 (JSON-decoded) or int (in-memory frames).
func errBodyOf(f map[string]any) (ErrorBody, bool) {
	raw, ok := f["error"].(map[string]any)
	if !ok {
		return ErrorBody{}, false
	}
	var body ErrorBody
	switch code := raw["code"].(type) {
	case float64:
		body.Code = int(code)
	case int:
		body.Code = code
	case int64:
		body.Code = int(code)
	}
	if msg, ok := raw["message"].(string); ok {
		body.Message = msg
	}
	body.Data = raw["data"]
	return body, true
}

// jsonDecode re-marshals and unmarshals a value into a fresh decoded `any`
// (map[string]any / []any / float64 ...), the same shape transports produce.
func jsonDecode(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

// mustJSON marshals v; a nil value marshals as the literal "null" so JSON
// Schema validation treats a missing input as null (mirrors the TS SDK).
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}

// nowMilli is the current unix time in milliseconds.
func nowMilli() int64 { return time.Now().UnixMilli() }

// timeoutFromMeta extracts meta.timeoutMs (spec §3.1 reserved key).
func timeoutFromMeta(meta map[string]any) (millis int64) {
	if meta == nil {
		return 0
	}
	if v, ok := meta["timeoutMs"].(float64); ok && v > 0 {
		return int64(v)
	}
	return 0
}
