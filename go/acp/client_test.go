package acp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Spec v0.2 §12.2 fallback ladder: a client declaring "0.2" against a "0.1"
// server receives 40003, retries once with the highest supported version and
// locks it for subsequent calls.
func TestClientFallbackLadder(t *testing.T) {
	srv := NewServer(ServerOptions{Name: "legacy", ProtocolVersion: "0.1"})
	if err := srv.Register(Component{
		ID:     "legacy.component",
		Name:   "Legacy",
		Handle: func(ctx context.Context, input any, send Sender) (any, error) { return map[string]any{"ok": true}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	tr := NewMemoryTransport(srv)
	cli, err := NewClient("", ClientOptions{Transport: tr, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	comps, err := cli.Discover(ctx, "legacy.component")
	if err != nil {
		t.Fatalf("discover after fallback: %v", err)
	}
	if len(comps) != 1 || comps[0].ID != "legacy.component" {
		t.Errorf("discover = %+v", comps)
	}
	if got := cli.version; got != "0.1" {
		t.Errorf("locked version = %q, want 0.1", got)
	}
	// The locked version is used for later requests: a second discover must
	// not re-trigger the ladder.
	if _, err := cli.Request(ctx, Envelope{Op: "discover"}); err != nil {
		t.Fatalf("second discover: %v", err)
	}
	if cli.version != "0.1" || !cli.fallbacked {
		t.Errorf("fallback state: version=%q fallbacked=%v", cli.version, cli.fallbacked)
	}
}

// A declared version equal to the highest supported version must NOT retry
// (the "9.9" probe in the conformance suite relies on this).
func TestClientFallbackNoRetryWhenEqual(t *testing.T) {
	srv := newConformanceServer(t)
	tr := NewMemoryTransport(srv)
	cli, _ := NewClient("", ClientOptions{Transport: tr, Timeout: 5 * time.Second})
	ctx := context.Background()
	reply, err := cli.Request(ctx, Envelope{ACP: "9.9", Op: "discover"})
	if err != nil {
		t.Fatal(err)
	}
	if reply["ok"] != false {
		t.Errorf("reply = %v, want the 40003 envelope", reply)
	}
	if cli.version != ProtocolVersion {
		t.Errorf("version changed to %q", cli.version)
	}
}

// Client-side timeout maps to ACPError 50400 TIMEOUT.
func TestClientTimeout50400(t *testing.T) {
	srv := NewServer(ServerOptions{Name: "slow"})
	if err := srv.Register(Component{
		ID:   "slow.component",
		Name: "Slow",
		Handle: func(ctx context.Context, input any, send Sender) (any, error) {
			time.Sleep(300 * time.Millisecond)
			return nil, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	tr := NewMemoryTransport(srv)
	cli, _ := NewClient("", ClientOptions{Transport: tr, Timeout: 50 * time.Millisecond})
	err := cli.Call(context.Background(), "slow.component", nil, nil)
	var ae *ACPError
	if !errors.As(err, &ae) || ae.Code != CodeTimeout {
		t.Fatalf("want ACPError 50400, got %v", err)
	}
}

// Server-side handler context honors meta.timeoutMs (spec §3.1): the handler
// observes a deadline and can abort.
func TestServerMetaTimeoutMs(t *testing.T) {
	srv := NewServer(ServerOptions{Name: "meta"})
	if err := srv.Register(Component{
		ID:   "sleep.component",
		Name: "Sleep",
		Handle: func(ctx context.Context, input any, send Sender) (any, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(300 * time.Millisecond):
				return map[string]any{"done": true}, nil
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	tr := NewMemoryTransport(srv)
	cli, _ := NewClient("", ClientOptions{Transport: tr, Timeout: 5 * time.Second})

	// meta.timeoutMs=50 aborts the handler -> 50001 with context error.
	reply, err := cli.Request(context.Background(), Envelope{
		Op:        "call",
		Component: "sleep.component",
		Meta:      map[string]any{"timeoutMs": 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply["ok"] != false {
		t.Fatalf("reply = %v, want failure", reply)
	}
	body, _ := errBodyOf(reply)
	if body.Code != CodeComponentError {
		t.Errorf("code = %d, want 50001", body.Code)
	}
	if !strings.Contains(body.Message, "context deadline exceeded") {
		t.Errorf("message = %q", body.Message)
	}

	// Without meta.timeoutMs the handler completes.
	var out struct {
		Done bool `json:"done"`
	}
	if err := cli.Call(context.Background(), "sleep.component", nil, &out); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !out.Done {
		t.Errorf("out = %+v", out)
	}
}

// HTTP connections have no event support: subscribe fails with 50100 and
// events never arrive (spec v0.2 §4.4).
func TestHTTPSubscribeUnsupported(t *testing.T) {
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	cli, err := NewClient(hs.URL+"/acp", ClientOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	_, err = cli.Subscribe(context.Background(), SubscriptionFilter{Component: "conf.echo"}, func(Event) {})
	var ae *ACPError
	if !errors.As(err, &ae) || ae.Code != CodeEventUnsupported {
		t.Fatalf("want ACPError 50100, got %v", err)
	}
}

// Subscription filters require exactly one of component/tags (spec §4.4).
func TestSubscribeFilterValidation(t *testing.T) {
	srv := newConformanceServer(t)
	tr := NewMemoryTransport(srv)
	cli, _ := NewClient("", ClientOptions{Transport: tr, Timeout: 5 * time.Second})
	ctx := context.Background()
	for _, bad := range []SubscriptionFilter{
		{}, // neither
		{Component: "a.b", Tags: []string{"iot"}}, // both
	} {
		if _, err := cli.Subscribe(ctx, bad, func(Event) {}); err == nil {
			t.Errorf("filter %+v accepted", bad)
		}
	}
}

// WS client keepalive: with aggressive intervals the connection survives
// because the server answers $ping (spec v0.2 §4.3).
func TestWSKeepaliveSurvives(t *testing.T) {
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	cli, err := NewClient("ws"+strings.TrimPrefix(hs.URL, "http")+"/acp",
		ClientOptions{Timeout: 5 * time.Second, KeepAlive: 80 * time.Millisecond, PongTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	tr := cli.transportInstance().(*wsTransport)
	if err := tr.connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // several keepalive cycles
	var out struct {
		Msg string `json:"msg"`
	}
	if err := cli.Call(context.Background(), "conf.echo", map[string]any{"msg": "alive"}, &out); err != nil {
		t.Fatalf("call after keepalive cycles: %v", err)
	}
	if out.Msg != "alive" {
		t.Errorf("out = %+v", out)
	}
}

// WS client reconnect: when the server drops the connection the client
// redials, fails in-flight requests, and re-issues its subscriptions
// (spec v0.2 §4.4 — auto-resubscribe is SDK duty).
func TestWSReconnectAndResubscribe(t *testing.T) {
	srv := newConformanceServer(t)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	cli, err := NewClient("ws"+strings.TrimPrefix(hs.URL, "http")+"/acp",
		ClientOptions{Timeout: 5 * time.Second, KeepAlive: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()

	var mu sync.Mutex
	received := 0
	_, err = cli.Subscribe(ctx, SubscriptionFilter{Component: "conf.echo"}, func(Event) {
		mu.Lock()
		received++
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Kill the server-side connection: the client must reconnect.
	var victim *Conn
	waitFor(t, 2*time.Second, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		for c := range srv.conns {
			victim = c
			break
		}
		return victim != nil
	})
	if victim == nil {
		t.Fatal("no server connection found")
	}
	victim.Close()

	// After the reconnect the connection works again.
	waitFor(t, 8*time.Second, func() bool {
		var out struct {
			Msg string `json:"msg"`
		}
		return cli.Call(ctx, "conf.echo", map[string]any{"msg": "back"}, &out) == nil && out.Msg == "back"
	})

	// And the subscription was re-issued: a fresh event arrives.
	srv.Emit(Event{Component: "conf.echo", Data: "after-reconnect"})
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received >= 1
	})
}

// Subscription via tags filter matches events whose tags are a superset
// (spec v0.2 §6.2), and descriptor tags are the default event tags.
func TestEventTagsMatching(t *testing.T) {
	srv := newConformanceServer(t) // conf.echo has tags [conf, echo]
	tr := NewMemoryTransport(srv)
	cli, _ := NewClient("", ClientOptions{Transport: tr, Timeout: 5 * time.Second})
	ctx := context.Background()

	var got []string
	done := make(chan struct{}, 4) // buffered: memory dispatch runs inline
	_, err := cli.Subscribe(ctx, SubscriptionFilter{Tags: []string{"echo"}}, func(ev Event) {
		b, _ := json.Marshal(ev.Data)
		got = append(got, string(b))
		select {
		case done <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// Matches: sub.tags ["echo"] ⊆ event.tags ["conf","echo"].
	srv.Emit(Event{Component: "conf.echo", Data: "tagged"})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("tag-subscribed event not delivered")
	}

	// Non-matching component emits nothing for this subscription.
	srv.Emit(Event{Component: "conf.counter", Data: "other"})
	select {
	case <-done:
		t.Fatal("non-matching event delivered")
	case <-time.After(100 * time.Millisecond):
	}

	// Closing the client detaches the memory connection; further emits are
	// simply not delivered to anyone.
	if err := cli.Close(); err != nil {
		t.Fatal(err)
	}
}
