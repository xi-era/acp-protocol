package acp

import (
	"fmt"
	"regexp"
	"strings"
)

// reservedOps are the `$`-prefixed protocol operations (spec v0.2 §4.3-4.4).
var reservedOps = map[string]struct{}{
	"$ping":        {},
	"$subscribe":   {},
	"$unsubscribe": {},
}

// IsReservedOp reports whether op is a protocol-reserved `$` op.
func IsReservedOp(op string) bool {
	_, ok := reservedOps[op]
	return ok
}

// componentIDRe implements the spec §7.1 grammar:
// ^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$
var componentIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}(\.[a-z][a-z0-9-]{0,62}){1,3}$`)

// IsValidComponentID reports whether id satisfies the component_id grammar.
func IsValidComponentID(id string) bool {
	return componentIDRe.MatchString(id)
}

// ComponentIDToToolName maps a component id to an MCP/OpenAI tool name
// ("." -> "_", lossless and reversible, spec §7.1).
func ComponentIDToToolName(id string) string {
	return strings.ReplaceAll(id, ".", "_")
}

// ToolNameToComponentID is the inverse of ComponentIDToToolName.
func ToolNameToComponentID(name string) string {
	return strings.ReplaceAll(name, "_", ".")
}

// parseVersion parses "major.minor"; ok is false when malformed.
func parseVersion(v string) (major, minor int, ok bool) {
	var dot int
	for i := 0; i < len(v); i++ {
		if v[i] == '.' {
			if dot != 0 {
				return 0, 0, false
			}
			dot = i + 1
		} else if v[i] < '0' || v[i] > '9' {
			return 0, 0, false
		}
	}
	if dot == 0 || dot == 1 || dot == len(v) {
		return 0, 0, false
	}
	return atoi(v[:dot-1]), atoi(v[dot:]), true
}

func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// IsVersionSupported reports whether the server supports the client's declared
// version: major equal AND server minor >= client minor (spec §12.1).
func IsVersionSupported(clientVersion, serverVersion string) bool {
	cm, cn, ok := parseVersion(clientVersion)
	if !ok {
		return false
	}
	sm, sn, ok := parseVersion(serverVersion)
	if !ok {
		return false
	}
	return cm == sm && sn >= cn
}

// ValidateReservedInput checks the input shape of a reserved op
// (spec v0.2 §4.3-4.4). It returns "" when valid, else the reason.
func ValidateReservedInput(op string, input any) string {
	switch op {
	case "$ping":
		// Input optional; any shape accepted (ts echoed when present).
		return ""
	case "$unsubscribe":
		// Absent / null input = unsubscribe all (valid).
		if input == nil {
			return ""
		}
	case "$subscribe":
		// Object with exactly one of component/tags required below.
	default:
		return ""
	}
	m, isObj := input.(map[string]any)
	if !isObj {
		return fmt.Sprintf("op=%s requires an input object with exactly one of component/tags", op)
	}
	_, hasComponent := m["component"]
	_, hasTags := m["tags"]
	if hasComponent == hasTags {
		return fmt.Sprintf("op=%s requires exactly one of component/tags", op)
	}
	if hasComponent {
		c, isStr := m["component"].(string)
		if !isStr || !IsValidComponentID(c) {
			return fmt.Sprintf("op=%s: invalid component id", op)
		}
	}
	if hasTags {
		arr, isArr := m["tags"].([]any)
		if !isArr || len(arr) == 0 {
			return fmt.Sprintf("op=%s: tags must be a non-empty string array", op)
		}
		for _, t := range arr {
			if _, isStr := t.(string); !isStr {
				return fmt.Sprintf("op=%s: tags must be a non-empty string array", op)
			}
		}
	}
	return ""
}

// EnvelopeError is a failed envelope validation carrying the code to report.
// ID is the request id when it could be read as a string, else nil; ACP is the
// request's declared version to echo back, or "" for the SDK default.
type EnvelopeError struct {
	ID      any
	Code    int
	Message string
	Data    any
	ACP     string
}

// Error implements the error interface.
func (e *EnvelopeError) Error() string {
	return fmt.Sprintf("acp envelope error %d: %s", e.Code, e.Message)
}

// ErrorEnvelope builds a failure envelope (spec §5.2). `id` is nil when the
// request id could not be read; `acp` echoes the request's declared version
// (spec v0.2 §5.3) and falls back to ProtocolVersion when empty.
func ErrorEnvelope(id any, code int, message string, data any, acp string) map[string]any {
	if acp == "" {
		acp = ProtocolVersion
	}
	body := map[string]any{"code": code, "message": message}
	if data != nil {
		body["data"] = data
	}
	return map[string]any{"acp": acp, "id": id, "ok": false, "error": body}
}

// ValidateEnvelope validates a decoded request following the spec §3.2 check
// order: JSON object -> acp/id/op presence & types (40001) -> version
// supported (40003, data.supported) -> op enum (40002) -> component (40004 /
// 40001) -> reserved-op input shape (40001). On success it returns the typed
// Envelope. `raw` must be the value produced by encoding/json decoding into
// `any` (or a value convertible to it via jsonDecode).
func ValidateEnvelope(raw any, serverVersion string) (*Envelope, *EnvelopeError) {
	m, isMap := raw.(map[string]any)
	if !isMap {
		// Accept the SDK's internal frame alias as well.
		if f, ok := raw.(frame); ok {
			m, isMap = f, true
		}
	}
	if !isMap {
		return nil, &EnvelopeError{Code: CodeInvalidEnvelope, Message: "request must be a JSON object"}
	}

	// The id is echoed on every error frame when it is a string (TS semantics).
	var idStr string
	hasID := false
	if id, ok := m["id"].(string); ok {
		idStr, hasID = id, true
	}
	var errEnvelopes = func(code int, msg string, data any) (*Envelope, *EnvelopeError) {
		ve := &EnvelopeError{Code: code, Message: msg, Data: data}
		if hasID {
			ve.ID = idStr
		}
		if acp, ok := m["acp"].(string); ok && hasID {
			ve.ACP = acp
		}
		return nil, ve
	}

	// Step 2: required fields present and correctly typed.
	acpVal, ok := m["acp"].(string)
	if !ok {
		return errEnvelopes(CodeInvalidEnvelope, "missing required field: acp", nil)
	}
	if !hasID {
		return errEnvelopes(CodeInvalidEnvelope, "missing required field: id", nil)
	}
	op, ok := m["op"].(string)
	if !ok {
		return errEnvelopes(CodeInvalidEnvelope, "missing required field: op", nil)
	}

	// Step 3: version supported (data.supported lists the served version).
	if !IsVersionSupported(acpVal, serverVersion) {
		return errEnvelopes(CodeUnsupportedVersion, "unsupported protocol version",
			map[string]any{"supported": []string{serverVersion}})
	}

	// Step 4: op in the enum (standard + reserved).
	if op != "discover" && op != "call" && !IsReservedOp(op) {
		return errEnvelopes(CodeUnknownOp, fmt.Sprintf("unknown op: %s", op), nil)
	}

	// Step 5: component presence and grammar.
	if compRaw, present := m["component"]; present {
		comp, isStr := compRaw.(string)
		if !isStr || !IsValidComponentID(comp) {
			return errEnvelopes(CodeInvalidComponentID, fmt.Sprintf("invalid component id: %v", compRaw), nil)
		}
	}
	if op == "call" {
		if _, present := m["component"]; !present {
			return errEnvelopes(CodeInvalidEnvelope, "op=call requires component", nil)
		}
	}

	// Step 6a: tags must be an array when present.
	if tagsRaw, present := m["tags"]; present {
		if _, isArr := tagsRaw.([]any); !isArr {
			return errEnvelopes(CodeInvalidEnvelope, "tags must be an array", nil)
		}
	}
	// Step 6b: stream must be boolean when present.
	if streamRaw, present := m["stream"]; present {
		if _, isBool := streamRaw.(bool); !isBool {
			return errEnvelopes(CodeInvalidEnvelope, "stream must be a boolean", nil)
		}
	}

	// Step 6c: reserved-op input shape.
	if IsReservedOp(op) {
		if msg := ValidateReservedInput(op, m["input"]); msg != "" {
			return errEnvelopes(CodeInvalidEnvelope, msg, nil)
		}
	}

	req := &Envelope{ACP: acpVal, ID: idStr, Op: op, Input: m["input"]}
	if comp, ok := m["component"].(string); ok {
		req.Component = comp
	}
	if tags, ok := toStringSlice(m["tags"]); ok {
		req.Tags = tags
	}
	if stream, ok := m["stream"].(bool); ok {
		req.Stream = stream
	}
	if meta, ok := m["meta"].(map[string]any); ok {
		req.Meta = meta
	}
	return req, nil
}

// toStringSlice converts a decoded JSON string array to []string.
func toStringSlice(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}
