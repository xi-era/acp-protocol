package acp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Conformance fixtures — mirror packages/acp-sdk-ts/test/conformance/suite.ts
// ---------------------------------------------------------------------------

func newConformanceServer(t *testing.T) *Server {
	t.Helper()
	srv := NewServer(ServerOptions{Name: "conf-node", Version: "1.0.0"})

	err := srv.Register(Component{
		ID:          "conf.echo",
		Name:        "Echo",
		Description: "echoes input.msg",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}},"required":["msg"]}`),
		Tags:        []string{"conf", "echo"},
		Handle: func(ctx context.Context, input any, send Sender) (any, error) {
			return input, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Register(Component{
		ID:          "conf.counter",
		Name:        "Counter",
		Description: "streams n chunks {i:0..n-1}",
		Stream:      true,
		Tags:        []string{"conf"},
		Handle: func(ctx context.Context, input any, send Sender) (any, error) {
			if send == nil {
				return nil, ErrStreamRequired
			}
			n := 0
			if m, ok := input.(map[string]any); ok {
				if v, ok := m["n"].(float64); ok {
					n = int(v)
				}
			}
			for i := 0; i < n; i++ {
				if err := send(map[string]any{"i": i}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Register(Component{
		ID:          "conf.failing",
		Name:        "Failing",
		Description: "always throws",
		Handle: func(ctx context.Context, input any, send Sender) (any, error) {
			return nil, errors.New("boom")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// confHarness bundles a conformance client with per-transport extras.
type confHarness struct {
	name    string
	srv     *Server
	client  *Client
	emit    func(component string, data any)
	sendRaw func(text string) (map[string]any, error) // nil when unsupported
	cleanup func()
}

// sendRawViaPending probes the transport's PARSE_ERROR path: replies to an
// unreadable envelope carry id null, which routes to the pending with id "".
// Each harness builds this inline against its transport's pending map.

func conformance(t *testing.T, h confHarness) {
	t.Helper()
	ctx := context.Background()

	// spec §4.1 discover: fixed result shape with server info
	reply, err := h.client.Request(ctx, Envelope{Op: "discover"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if reply["ok"] != true {
		t.Fatalf("discover reply not ok: %v", reply)
	}
	result, _ := reply["result"].(map[string]any)
	server, _ := result["server"].(map[string]any)
	if server["protocol"] != "0.2" {
		t.Errorf("server.protocol = %v, want 0.2", server["protocol"])
	}
	if server["name"] != "conf-node" {
		t.Errorf("server.name = %v", server["name"])
	}
	comps, _ := result["components"].([]any)
	ids := map[string]bool{}
	for _, c := range comps {
		m, _ := c.(map[string]any)
		id, _ := m["id"].(string)
		ids[id] = true
	}
	for _, want := range []string{"conf.echo", "conf.counter", "conf.failing"} {
		if !ids[want] {
			t.Errorf("discover components missing %q (got %v)", want, ids)
		}
	}

	// spec §4.1 single lookup: still an array
	got, err := h.client.Discover(ctx, "conf.echo")
	if err != nil || len(got) != 1 {
		t.Errorf("discover(conf.echo) = %v, %v; want 1 component", got, err)
	}
	got, err = h.client.Discover(ctx, "absent.component")
	if err != nil || len(got) != 0 {
		t.Errorf("discover(absent.component) = %v, %v; want 0", got, err)
	}

	// spec §5.1 bare result
	var out struct {
		Msg string `json:"msg"`
	}
	if err := h.client.Call(ctx, "conf.echo", map[string]any{"msg": "ping"}, &out); err != nil {
		t.Fatalf("call conf.echo: %v", err)
	}
	if out.Msg != "ping" {
		t.Errorf("call result = %+v, want msg=ping", out)
	}

	// spec §8 42200
	err = h.client.Call(ctx, "conf.echo", map[string]any{"msg": 42}, nil)
	var invalid *ACPError
	if !errors.As(err, &invalid) || invalid.Code != CodeInvalidInput {
		t.Errorf("invalid input: want ACPError 42200, got %v", err)
	}

	// spec §8 40400
	err = h.client.Call(ctx, "absent.component", map[string]any{}, nil)
	var notFound *ACPError
	if !errors.As(err, &notFound) || notFound.Code != CodeComponentNotFound {
		t.Errorf("absent component: want ACPError 40400, got %v", err)
	}

	// spec §6 streaming: seq order + terminal end frame
	seqFn, err := h.client.CallStream(ctx, "conf.counter", map[string]any{"n": 3})
	if err != nil {
		t.Fatalf("callStream: %v", err)
	}
	var chunks []Chunk
	for chunk, err := range seqFn {
		if err != nil {
			t.Fatalf("stream chunk error: %v", err)
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 4 {
		t.Fatalf("stream chunks = %d, want 4", len(chunks))
	}
	for i, c := range chunks {
		if c.Seq != i {
			t.Errorf("chunk[%d].seq = %d", i, c.Seq)
		}
	}
	if !chunks[3].End {
		t.Errorf("last chunk not terminal")
	}
	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(chunks[i].Data)
		if string(b) != fmt.Sprintf(`{"i":%d}`, i) {
			t.Errorf("chunk[%d].data = %s, want {\"i\":%d}", i, b, i)
		}
	}

	// spec §4.2 40005 STREAM_REQUIRED
	err = h.client.Call(ctx, "conf.counter", map[string]any{"n": 1}, nil)
	var streamReq *ACPError
	if !errors.As(err, &streamReq) || streamReq.Code != CodeStreamRequired {
		t.Errorf("non-stream call on streaming component: want 40005, got %v", err)
	}

	// spec §8 50001 handler exception
	err = h.client.Call(ctx, "conf.failing", map[string]any{}, nil)
	var compErr *ACPError
	if !errors.As(err, &compErr) || compErr.Code != CodeComponentError {
		t.Errorf("failing component: want 50001, got %v", err)
	}

	// spec §12 40003 version negotiation (client's own version is "0.2", so
	// the fallback ladder does not retry: highest supported == declared).
	badReply, err := h.client.Request(ctx, Envelope{ACP: "9.9", Op: "discover"})
	if err != nil {
		t.Fatalf("9.9 request: %v", err)
	}
	if badReply["ok"] != false {
		t.Errorf("9.9 reply ok = %v", badReply["ok"])
	}
	body, _ := errBodyOf(badReply)
	if body.Code != CodeUnsupportedVersion {
		t.Errorf("9.9 code = %d, want 40003", body.Code)
	}
	supported, _ := body.Data.(map[string]any)["supported"].([]any)
	found := false
	for _, v := range supported {
		if v == "0.2" {
			found = true
		}
	}
	if !found {
		t.Errorf("supported = %v, want to contain 0.2", supported)
	}

	// spec v0.2 §5.3: responses echo the request's acp value
	echoReply, err := h.client.Request(ctx, Envelope{Op: "discover"})
	if err != nil {
		t.Fatal(err)
	}
	if echoReply["acp"] != "0.2" {
		t.Errorf("response acp = %v, want 0.2", echoReply["acp"])
	}

	// spec v0.2 §4.3: $ping roundtrip
	pingReply, err := h.client.Request(ctx, Envelope{Op: "$ping", Input: map[string]any{"ts": 42}})
	if err != nil {
		t.Fatalf("$ping: %v", err)
	}
	if pingReply["ok"] != true {
		t.Fatalf("$ping not ok: %v", pingReply)
	}
	pong, _ := pingReply["result"].(map[string]any)
	if ts, ok := pong["ts"].(float64); !ok || ts != 42 {
		t.Errorf("$ping ts = %v, want 42", pong["ts"])
	}
	if _, ok := pong["pong"].(float64); !ok {
		t.Errorf("$ping pong = %v, want number", pong["pong"])
	}

	// spec v0.2 §4.4/§6.2: subscribe -> emit -> event -> unsubscribe
	if h.emit != nil {
		const component = "conf.echo"
		var mu sync.Mutex
		var received []any
		sub, err := h.client.Subscribe(ctx, SubscriptionFilter{Component: component}, func(ev Event) {
			mu.Lock()
			received = append(received, ev.Data)
			mu.Unlock()
		})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		h.emit(component, "evt-1")
		h.emit(component, "evt-2")
		waitFor(t, 2*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(received) >= 2
		})
		mu.Lock()
		if len(received) != 2 || received[0] != "evt-1" || received[1] != "evt-2" {
			t.Errorf("events = %v, want [evt-1 evt-2]", received)
		}
		mu.Unlock()
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatalf("unsubscribe: %v", err)
		}
		h.emit(component, "evt-3")
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if len(received) != 2 {
			t.Errorf("events after unsubscribe = %v, want no delivery", received)
		}
		mu.Unlock()
	}

	// spec §13 meta ignored
	metaReply, err := h.client.Request(ctx, Envelope{
		Op:        "call",
		Component: "conf.echo",
		Input:     map[string]any{"msg": "meta"},
		Meta:      map[string]any{"auth": "bearer x", "traceId": "tr-1", "vendor": "ignored"},
	})
	if err != nil || metaReply["ok"] != true {
		t.Errorf("call with meta: reply=%v err=%v, want ok", metaReply, err)
	}

	// spec §3.2 40000 parse error (transport-dependent hook)
	if h.sendRaw != nil {
		parseReply, err := h.sendRaw("this is not json")
		if err != nil {
			t.Fatalf("sendRaw: %v", err)
		}
		if parseReply["ok"] != false {
			t.Errorf("raw text: ok = %v, want false", parseReply["ok"])
		}
		body, _ := errBodyOf(parseReply)
		if body.Code != CodeParseError {
			t.Errorf("raw text: code = %d, want 40000", body.Code)
		}
	}
}

// waitFor polls cond until true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// ---------------------------------------------------------------------------
// Transport harnesses
// ---------------------------------------------------------------------------

func TestConformance(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) confHarness
	}{
		{"http", buildHTTPHarness},
		{"ws", buildWSHarness},
		{"stdio", buildStdioHarness},
		{"memory", buildMemoryHarness},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.build(t)
			defer h.cleanup()
			conformance(t, h)
		})
	}
}

func clientOpts() ClientOptions {
	return ClientOptions{Timeout: 10 * time.Second, KeepAlive: -1}
}

func buildHTTPHarness(t *testing.T) confHarness {
	t.Helper()
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	cli, err := NewClient(hs.URL+"/acp", clientOpts())
	if err != nil {
		t.Fatal(err)
	}
	url := hs.URL + "/acp"
	return confHarness{
		name:   "http",
		srv:    srv,
		client: cli,
		emit:   nil, // HTTP: no event delivery; subscription asserted in client_test
		sendRaw: func(text string) (map[string]any, error) {
			resp, err := http.Post(url, "application/json", strings.NewReader(text))
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			var f map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&f); err != nil {
				return nil, err
			}
			return f, nil
		},
		cleanup: func() { _ = cli.Close(); hs.Close() },
	}
}

func buildWSHarness(t *testing.T) confHarness {
	t.Helper()
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http") + "/acp"
	cli, err := NewClient(wsURL, clientOpts())
	if err != nil {
		t.Fatal(err)
	}
	return confHarness{
		name:   "ws",
		srv:    srv,
		client: cli,
		emit: func(component string, data any) {
			srv.Emit(Event{Component: component, Data: data})
		},
		sendRaw: func(text string) (map[string]any, error) {
			tr := cli.transportInstance().(*wsTransport)
			p := newPending()
			tr.pending.set("", p)
			defer tr.pending.remove("")
			if err := tr.sendText(text); err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return p.waitTerminal(ctx)
		},
		cleanup: func() { _ = cli.Close(); hs.Close() },
	}
}

func buildStdioHarness(t *testing.T) confHarness {
	t.Helper()
	srv := newConformanceServer(t)
	srvInR, srvInW := io.Pipe()   // client requests -> server stdin
	srvOutR, srvOutW := io.Pipe() // server stdout -> client replies
	go func() { _ = srv.ServeStdio(srvInR, srvOutW) }()
	tr := NewStdioClientTransport(srvOutR, srvInW)
	cli, err := NewClient("", ClientOptions{Transport: tr, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return confHarness{
		name:   "stdio",
		srv:    srv,
		client: cli,
		emit: func(component string, data any) {
			srv.Emit(Event{Component: component, Data: data})
		},
		sendRaw: func(text string) (map[string]any, error) {
			p := newPending()
			tr.pending.set("", p)
			defer tr.pending.remove("")
			if err := tr.sendRawLine(text); err != nil {
				return nil, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return p.waitTerminal(ctx)
		},
		cleanup: func() {
			_ = cli.Close()
			_ = srvInW.Close()
			_ = srvOutW.Close()
		},
	}
}

func buildMemoryHarness(t *testing.T) confHarness {
	t.Helper()
	srv := newConformanceServer(t)
	tr := NewMemoryTransport(srv)
	cli, err := NewClient("", ClientOptions{Transport: tr, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return confHarness{
		name:   "memory",
		srv:    srv,
		client: cli,
		emit: func(component string, data any) {
			srv.Emit(Event{Component: component, Data: data})
		},
		cleanup: func() { _ = cli.Close() },
	}
}

// ---------------------------------------------------------------------------
// HTTP transport specifics (spec §9)
// ---------------------------------------------------------------------------

func TestHTTPTransportEndpoints(t *testing.T) {
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	base := hs.URL

	// GET /acp/health
	resp, err := http.Get(base + "/acp/health")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(bytes.TrimSpace(body)) != `{"ok":true}` {
		t.Errorf("health = %d %s", resp.StatusCode, body)
	}

	// GET /acp/discover returns the bare result
	resp, err = http.Get(base + "/acp/discover")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("discover status = %d", resp.StatusCode)
	}
	srvInfo, _ := result["server"].(map[string]any)
	if srvInfo["protocol"] != "0.2" {
		t.Errorf("GET discover server = %v", result)
	}

	// GET /acp -> 40500 METHOD_NOT_ALLOWED with HTTP 405
	resp, err = http.Get(base + "/acp")
	if err != nil {
		t.Fatal(err)
	}
	var methodFrame map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&methodFrame)
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("GET /acp status = %d, want 405", resp.StatusCode)
	}
	if body, _ := errBodyOf(methodFrame); body.Code != CodeMethodNotAllowed {
		t.Errorf("GET /acp code = %v", methodFrame)
	}

	// POST with wrong content type -> 41500
	resp, err = http.Post(base+"/acp", "text/plain", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var mtFrame map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&mtFrame)
	resp.Body.Close()
	if resp.StatusCode != 415 {
		t.Errorf("bad content-type status = %d, want 415", resp.StatusCode)
	}
	if body, _ := errBodyOf(mtFrame); body.Code != CodeUnsupportedMediaType {
		t.Errorf("bad content-type code = %v", mtFrame)
	}

	// POST invalid JSON -> 40000 PARSE_ERROR with HTTP 400
	resp, err = http.Post(base+"/acp", "application/json", strings.NewReader("not json"))
	if err != nil {
		t.Fatal(err)
	}
	var peFrame map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&peFrame)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("parse error status = %d, want 400", resp.StatusCode)
	}
	if body, _ := errBodyOf(peFrame); body.Code != CodeParseError {
		t.Errorf("parse error code = %v", peFrame)
	}

	// POST gzip request body (spec v0.2 §9.4) is decompressed transparently.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(`{"acp":"0.2","id":"gz-1","op":"discover"}`))
	_ = gz.Close()
	req, _ := http.NewRequest(http.MethodPost, base+"/acp", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var gzFrame map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&gzFrame)
	resp.Body.Close()
	if gzFrame["ok"] != true {
		t.Errorf("gzip request reply = %v", gzFrame)
	}

	// Unsupported Content-Encoding -> 41500
	req, _ = http.NewRequest(http.MethodPost, base+"/acp", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "br")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var brFrame map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&brFrame)
	resp.Body.Close()
	if body, _ := errBodyOf(brFrame); body.Code != CodeUnsupportedMediaType {
		t.Errorf("br content-encoding = %v, want 41500", brFrame)
	}

	// Large buffered responses are gzipped when the client accepts it and
	// carry Vary: Accept-Encoding (spec v0.2 §9.4).
	big := NewServer(ServerOptions{Name: "big"})
	_ = big.Register(Component{
		ID:          "big.component",
		Name:        strings.Repeat("x", 2048),
		Description: strings.Repeat("y", 2048),
		Handle:      func(ctx context.Context, input any, send Sender) (any, error) { return nil, nil },
	})
	bigSrv := httptest.NewServer(big.Handler())
	defer bigSrv.Close()
	req, _ = http.NewRequest(http.MethodGet, bigSrv.URL+"/acp/discover", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	if resp.Header.Get("Vary") != "Accept-Encoding" {
		t.Errorf("Vary = %q", resp.Header.Get("Vary"))
	}
	gzResp, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var discResult map[string]any
	if err := json.NewDecoder(gzResp).Decode(&discResult); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(discResult["components"].([]any)) != 1 {
		t.Errorf("gzip discover body = %v", discResult)
	}

	// Uncompressed path stays available (spec §9.4 principle).
	resp, err = http.Get(bigSrv.URL + "/acp/discover")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("unexpected compression without Accept-Encoding")
	}
	resp.Body.Close()

	// Unknown path -> 404 {"ok":false}
	resp, err = http.Get(base + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("unknown path status = %d", resp.StatusCode)
	}
}

// Binary WS frames are rejected with 40001 (spec §10.1).
func TestWSBinaryFrameRejected(t *testing.T) {
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()

	cli, err := NewClient("ws"+strings.TrimPrefix(hs.URL, "http")+"/acp", clientOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	tr := cli.transportInstance().(*wsTransport)
	if err := tr.connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	p := newPending()
	tr.pending.set("", p)
	defer tr.pending.remove("")
	if err := tr.sendBinary([]byte{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f, err := p.waitTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if body, _ := errBodyOf(f); body.Code != CodeInvalidEnvelope {
		t.Errorf("binary frame reply = %v, want 40001", f)
	}
}
