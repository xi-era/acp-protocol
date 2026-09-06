package acp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestComponentIDGrammar(t *testing.T) {
	valid := []string{
		"sensor.temperature",
		"a.b",
		"sensor.temperature.celsius",
		"a.b.c.d",
		"edge-1.sensor-room.temperature-c",
		"abc." + strings.Repeat("x", 63) + ".d", // segment of 64 chars (1+63)
	}
	for _, id := range valid {
		if !IsValidComponentID(id) {
			t.Errorf("IsValidComponentID(%q) = false, want true", id)
		}
	}
	invalid := []string{
		"",
		"sensor",                       // single segment
		"Sensor.temp",                  // uppercase
		"1sensor.temp",                 // leading digit
		"a..b",                         // empty segment
		"a.b.c.d.e",                    // too many segments
		"-sensor.temp",                 // leading dash
		"a." + strings.Repeat("x", 64), // segment too long
	}
	for _, id := range invalid {
		if IsValidComponentID(id) {
			t.Errorf("IsValidComponentID(%q) = true, want false", id)
		}
	}
}

func TestComponentIDToolNameRoundTrip(t *testing.T) {
	id := "sensor.temperature.celsius"
	name := ComponentIDToToolName(id)
	if name != "sensor_temperature_celsius" {
		t.Fatalf("ComponentIDToToolName = %q", name)
	}
	if got := ToolNameToComponentID(name); got != id {
		t.Fatalf("ToolNameToComponentID = %q, want %q", got, id)
	}
}

func TestIsReservedOp(t *testing.T) {
	for _, op := range []string{"$ping", "$subscribe", "$unsubscribe"} {
		if !IsReservedOp(op) {
			t.Errorf("IsReservedOp(%q) = false", op)
		}
	}
	for _, op := range []string{"discover", "call", "ping", "$unknown", ""} {
		if IsReservedOp(op) {
			t.Errorf("IsReservedOp(%q) = true", op)
		}
	}
}

func TestValidateReservedInput(t *testing.T) {
	cases := []struct {
		op    string
		input any
		want  string // "" = valid
	}{
		{"$ping", nil, ""},
		{"$ping", map[string]any{"ts": float64(42)}, ""},
		{"$ping", "anything", ""},
		{"$subscribe", nil, "requires"},
		{"$subscribe", map[string]any{}, "requires"},
		{"$subscribe", map[string]any{"component": "a.b", "tags": []any{"x"}}, "requires"},
		{"$subscribe", map[string]any{"component": "a.b"}, ""},
		{"$subscribe", map[string]any{"tags": []any{"iot"}}, ""},
		{"$subscribe", map[string]any{"component": "BAD"}, "invalid component id"},
		{"$subscribe", map[string]any{"tags": []any{}}, "non-empty"},
		{"$subscribe", map[string]any{"tags": []any{1}}, "non-empty"},
		{"$subscribe", "not-an-object", "requires"},
		{"$unsubscribe", nil, ""},
		{"$unsubscribe", map[string]any{"component": "a.b"}, ""},
		{"$unsubscribe", map[string]any{"tags": []any{"iot"}}, ""},
		{"$unsubscribe", map[string]any{}, "requires"},
	}
	for _, tc := range cases {
		if got := ValidateReservedInput(tc.op, tc.input); !strings.Contains(got, tc.want) || (tc.want == "" && got != "") {
			t.Errorf("ValidateReservedInput(%q, %v) = %q, want %q", tc.op, tc.input, got, tc.want)
		}
	}
}

func TestIsVersionSupported(t *testing.T) {
	cases := []struct {
		client, server string
		want           bool
	}{
		{"0.2", "0.2", true},
		{"0.1", "0.2", true},  // server minor >= client minor
		{"0.2", "0.1", false}, // 0.1 server vs 0.2 client
		{"1.0", "0.9", false}, // major mismatch
		{"0.2", "1.0", false},
		{"bogus", "0.2", false},
		{"0.2", "bogus", false},
	}
	for _, tc := range cases {
		if got := IsVersionSupported(tc.client, tc.server); got != tc.want {
			t.Errorf("IsVersionSupported(%q, %q) = %v, want %v", tc.client, tc.server, got, tc.want)
		}
	}
}

// decoded is a helper: JSON round-trip so shapes match transport decoding.
func decoded(t *testing.T, s string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestValidateEnvelopeOrder(t *testing.T) {
	// spec §3.2 order: object -> fields (40001) -> version (40003) -> op
	// (40002) -> component (40004/40001) -> reserved input (40001).

	if _, ve := ValidateEnvelope(decoded(t, `["not","an","object"]`), "0.2"); ve == nil || ve.Code != CodeInvalidEnvelope {
		t.Errorf("array input: want 40001, got %+v", ve)
	}
	if _, ve := ValidateEnvelope(decoded(t, `"a string"`), "0.2"); ve == nil || ve.Code != CodeInvalidEnvelope {
		t.Errorf("string input: want 40001, got %+v", ve)
	}

	// Missing fields -> 40001.
	for _, raw := range []string{
		`{"id":"1","op":"discover"}`,           // no acp
		`{"acp":"0.2","op":"discover"}`,        // no id
		`{"acp":"0.2","id":"1"}`,               // no op
		`{"acp":5,"id":"1","op":"discover"}`,   // acp wrong type
		`{"acp":"0.2","id":7,"op":"discover"}`, // id wrong type
	} {
		_, ve := ValidateEnvelope(decoded(t, raw), "0.2")
		if ve == nil || ve.Code != CodeInvalidEnvelope {
			t.Errorf("%s: want 40001, got %+v", raw, ve)
		}
	}

	// Unsupported version -> 40003 with data.supported (checked before op enum).
	_, ve := ValidateEnvelope(decoded(t, `{"acp":"9.9","id":"1","op":"discover"}`), "0.2")
	if ve == nil || ve.Code != CodeUnsupportedVersion {
		t.Fatalf("9.9: want 40003, got %+v", ve)
	}
	data, _ := ve.Data.(map[string]any)
	supported, _ := data["supported"].([]string)
	if len(supported) != 1 || supported[0] != "0.2" {
		t.Errorf("data.supported = %v, want [0.2]", supported)
	}
	if ve.ID != "1" || ve.ACP != "9.9" {
		t.Errorf("error echo: id=%v acp=%v, want id=1 acp=9.9", ve.ID, ve.ACP)
	}

	// Unknown op -> 40002.
	if _, ve = ValidateEnvelope(decoded(t, `{"acp":"0.2","id":"1","op":"bogus"}`), "0.2"); ve == nil || ve.Code != CodeUnknownOp {
		t.Errorf("unknown op: want 40002, got %+v", ve)
	}

	// Invalid component id -> 40004.
	if _, ve = ValidateEnvelope(decoded(t, `{"acp":"0.2","id":"1","op":"call","component":"BAD"}`), "0.2"); ve == nil || ve.Code != CodeInvalidComponentID {
		t.Errorf("bad component: want 40004, got %+v", ve)
	}

	// call without component -> 40001.
	if _, ve = ValidateEnvelope(decoded(t, `{"acp":"0.2","id":"1","op":"call"}`), "0.2"); ve == nil || ve.Code != CodeInvalidEnvelope {
		t.Errorf("call w/o component: want 40001, got %+v", ve)
	}

	// Reserved op with bad input shape -> 40001.
	if _, ve = ValidateEnvelope(decoded(t, `{"acp":"0.2","id":"1","op":"$subscribe"}`), "0.2"); ve == nil || ve.Code != CodeInvalidEnvelope {
		t.Errorf("$subscribe w/o input: want 40001, got %+v", ve)
	}

	// Valid envelope round-trips into the typed struct.
	req, ve := ValidateEnvelope(decoded(t, `{"acp":"0.1","id":"r1","op":"call","component":"a.b","input":{"x":1},"stream":true,"meta":{"traceId":"t"}}`), "0.2")
	if ve != nil {
		t.Fatalf("valid envelope rejected: %+v", ve)
	}
	if req.ACP != "0.1" || req.ID != "r1" || req.Op != "call" || req.Component != "a.b" || !req.Stream {
		t.Errorf("parsed envelope = %+v", req)
	}
	if m, ok := req.Input.(map[string]any); !ok || m["x"] != float64(1) {
		t.Errorf("input = %#v", req.Input)
	}
	if req.Meta["traceId"] != "t" {
		t.Errorf("meta = %v", req.Meta)
	}
}

func TestErrorEnvelope(t *testing.T) {
	f := ErrorEnvelope("r1", CodeInvalidInput, "nope", map[string]any{"errors": []string{"x"}}, "0.1")
	if f["acp"] != "0.1" {
		t.Errorf("acp echo = %v, want 0.1", f["acp"])
	}
	if f["id"] != "r1" || f["ok"] != false {
		t.Errorf("frame = %v", f)
	}
	body := f["error"].(map[string]any)
	if body["code"] != CodeInvalidInput || body["data"] == nil {
		t.Errorf("error body = %v", body)
	}

	// Empty acp falls back to ProtocolVersion; nil data omits the key.
	f = ErrorEnvelope(nil, CodeParseError, "bad", nil, "")
	if f["acp"] != ProtocolVersion {
		t.Errorf("acp default = %v", f["acp"])
	}
	if _, has := f["error"].(map[string]any)["data"]; has {
		t.Errorf("nil data must be omitted: %v", f)
	}
}

func TestACPCodeToHTTPStatus(t *testing.T) {
	cases := map[int]int{
		40000: 400, 40100: 401, 40400: 404, 40500: 405, 41500: 415,
		42200: 422, 42900: 429, 42902: 429,
		50000: 500, 50001: 500, 50002: 502, // UPSTREAM_ERROR exception
		50100: 501, 50300: 503, 50400: 504,
		51000: 500, 51001: 500, // stream segment
		99999: 500, // out of range
	}
	for code, want := range cases {
		if got := ACPCodeToHTTPStatus(code); got != want {
			t.Errorf("ACPCodeToHTTPStatus(%d) = %d, want %d", code, got, want)
		}
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Component{ID: "bad id", Handle: func(ctx context.Context, input any, send Sender) (any, error) { return nil, nil }}); err == nil {
		t.Error("invalid id accepted")
	}
	if err := r.Register(Component{ID: "a.b"}); err == nil {
		t.Error("missing handle accepted")
	}
	if err := r.Register(Component{ID: "a.b", InputSchema: []byte("{invalid"), Handle: dummyHandle}); err == nil {
		t.Error("bad schema accepted")
	}
	if err := r.Register(Component{ID: "a.b", Handle: dummyHandle}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register(Component{ID: "a.b", Handle: dummyHandle}); err == nil {
		t.Error("duplicate accepted")
	}
	if _, ok := r.Get("a.b"); !ok {
		t.Error("Get after register failed")
	}
	if _, ok := r.Get("x.y"); ok {
		t.Error("Get of unknown succeeded")
	}
	descs := r.Descriptors()
	if len(descs) != 1 || descs[0].Version != "0.0.0" {
		t.Errorf("descriptors = %+v", descs)
	}
}

func dummyHandle(ctx context.Context, input any, send Sender) (any, error) { return nil, nil }
