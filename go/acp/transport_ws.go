package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// Server side (spec §10): /acp upgrade; one text frame = one envelope; the
// permessage-deflate extension is negotiated by the websocket library.
// ---------------------------------------------------------------------------

// serveWS upgrades the request to a WebSocket connection and serves it until
// the peer disconnects. Events and replies multiplex over the single
// connection; each incoming envelope is dispatched on its own goroutine so
// concurrent calls are legal (spec §10.3).
func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The SDK is transport-agnostic; origin policy is the deployment's
		// concern (mirrors the TS ws server, which does not check origins).
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer c.Close(websocket.StatusInternalError, "")

	ctx := r.Context() // canceled when the connection dies / handler returns
	conn := &Conn{
		Meta: map[string]any{"transport": "ws", "ip": r.RemoteAddr},
		Ctx:  ctx,
	}
	var wmu sync.Mutex
	conn.send = func(f frame) error {
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}
		wmu.Lock()
		defer wmu.Unlock()
		return c.Write(ctx, websocket.MessageText, b)
	}
	conn.closeFn = func() { _ = c.CloseNow() }

	s.attach(conn)
	defer s.detach(conn)

	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			_ = conn.Send(ErrorEnvelope(nil, CodeInvalidEnvelope, "binary frames are not supported", nil, ProtocolVersion))
			continue
		}
		var parsed any
		if err := json.Unmarshal(data, &parsed); err != nil {
			_ = conn.Send(ErrorEnvelope(nil, CodeParseError, "frame is not valid JSON", nil, ProtocolVersion))
			continue
		}
		go s.Handle(parsed, conn)
	}
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

// wsTransport is the WebSocket client transport: id multiplexing, event
// routing, server-initiated $ping auto-answer (spec v0.2 §4.3), idle
// keepalive with pong-timeout-triggered reconnect, and automatic
// resubscription after reconnect (spec v0.2 §4.4).
type wsTransport struct {
	url         string
	headers     http.Header
	keepAlive   time.Duration
	pongTimeout time.Duration

	mu            sync.Mutex
	conn          *websocket.Conn
	wctx          context.Context
	wcancel       context.CancelFunc
	pending       pendingMap
	handlers      map[int]func(Event)
	handlerSeq    int
	subs          map[string]SubscriptionFilter
	closedByUser  bool
	keepaliveOff  bool
	reconnecting  bool
}

// newWSTransport builds the WS client transport for a ws(s):// URL.
func newWSTransport(url string, headers http.Header, keepAlive, pongTimeout time.Duration) *wsTransport {
	return &wsTransport{
		url:         url,
		headers:     headers,
		keepAlive:   keepAlive,
		pongTimeout: pongTimeout,
		handlers:    map[int]func(Event){},
		subs:        map[string]SubscriptionFilter{},
	}
}

var _ Transport = (*wsTransport)(nil)

// connect dials the endpoint and starts the read loop (idempotent).
func (t *wsTransport) connect(ctx context.Context) error {
	t.mu.Lock()
	if t.conn != nil {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{HTTPHeader: t.headers}
	c, _, err := websocket.Dial(dctx, t.url, opts)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.conn = c
	t.wctx, t.wcancel = context.WithCancel(context.Background())
	rctx := t.wctx
	t.mu.Unlock()

	go t.readLoop(c, rctx)
	if t.keepAlive > 0 {
		go t.keepaliveLoop(c, rctx)
	}

	// Auto-resubscribe after (re)connect (spec v0.2 §4.4: SDK duty).
	for _, filter := range t.snapshotSubs() {
		env := Envelope{ACP: ProtocolVersion, ID: "resub-" + filterKey(filter), Op: "$subscribe", Input: jsonDecode(filter)}
		go func() { _, _ = t.Request(rctx, env) }()
	}
	return nil
}

func (t *wsTransport) snapshotSubs() []SubscriptionFilter {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]SubscriptionFilter, 0, len(t.subs))
	for _, f := range t.subs {
		out = append(out, f)
	}
	return out
}

func filterKey(f SubscriptionFilter) string {
	b, _ := json.Marshal(jsonDecode(f))
	return string(b)
}

// readLoop pumps incoming frames into pending requests and event handlers.
func (t *wsTransport) readLoop(c *websocket.Conn, rctx context.Context) {
	for {
		typ, data, err := c.Read(rctx)
		if err != nil {
			t.onClosed(c)
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var f map[string]any
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		if _, isEvent := f["event"]; isEvent {
			var ev Event
			if b, jerr := json.Marshal(jsonDecode(f["event"])); jerr == nil {
				if jerr := json.Unmarshal(b, &ev); jerr == nil {
					t.dispatchEvent(ev)
				}
			}
			continue
		}
		if op, ok := f["op"].(string); ok && op == "$ping" {
			// Server-initiated keepalive: MUST answer (spec v0.2 §4.3).
			t.answerPing(f)
			continue
		}
		id, _ := f["id"].(string)
		if p := t.pending.get(id); p != nil {
			p.push(f)
		}
	}
}

// answerPing replies to a server-initiated $ping frame.
func (t *wsTransport) answerPing(f map[string]any) {
	result := map[string]any{"pong": float64(nowMilli())}
	if m, ok := f["input"].(map[string]any); ok {
		if ts, ok := m["ts"].(float64); ok {
			result["ts"] = ts
		}
	}
	acp, _ := f["acp"].(string)
	id, _ := f["id"].(string)
	reply := okFrame(acp, id, result)
	t.sendFrame(reply)
}

func (t *wsTransport) dispatchEvent(ev Event) {
	t.mu.Lock()
	hs := make([]func(Event), 0, len(t.handlers))
	for _, h := range t.handlers {
		hs = append(hs, h)
	}
	t.mu.Unlock()
	for _, h := range hs {
		h(ev)
	}
}

// sendFrame writes one JSON text frame; safe for concurrent use.
func (t *wsTransport) sendFrame(f map[string]any) error {
	t.mu.Lock()
	c, wctx := t.conn, t.wctx
	t.mu.Unlock()
	if c == nil {
		return &ACPError{Code: CodeInternalError, Message: "ws transport not connected"}
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return c.Write(wctx, websocket.MessageText, b)
}

func (t *wsTransport) Request(ctx context.Context, env Envelope) (map[string]any, error) {
	if err := t.connect(ctx); err != nil {
		return nil, err
	}
	t.trackSub(env)
	defer t.untrackSub(env)
	p := newPending()
	// Register pending BEFORE sending: the reply can race the write.
	t.pending.set(env.ID, p)
	if err := t.sendFrame(mustMarshalFrame(env)); err != nil {
		t.pending.remove(env.ID)
		return nil, err
	}
	f, err := p.waitTerminal(ctx)
	t.pending.remove(env.ID)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (t *wsTransport) RequestStream(ctx context.Context, env Envelope) (iter.Seq2[map[string]any, error], error) {
	if err := t.connect(ctx); err != nil {
		return nil, err
	}
	p := newPending()
	t.pending.set(env.ID, p)
	if err := t.sendFrame(mustMarshalFrame(env)); err != nil {
		t.pending.remove(env.ID)
		return nil, err
	}
	return p.iterate(ctx), nil
}

// EventsSupported reports event capability (stateful transport).
func (t *wsTransport) EventsSupported() bool { return true }

// OnEvent registers an event handler; returns the unregister func.
func (t *wsTransport) OnEvent(handler func(Event)) func() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlerSeq++
	id := t.handlerSeq
	t.handlers[id] = handler
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.handlers, id)
	}
}

// Close closes the connection; no reconnect is attempted afterwards.
func (t *wsTransport) Close() error {
	t.mu.Lock()
	t.closedByUser = true
	c, wcancel := t.conn, t.wcancel
	t.conn = nil
	t.mu.Unlock()
	if wcancel != nil {
		wcancel()
	}
	if c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Close(websocket.StatusNormalClosure, "client closed")
		_ = ctx // Close is synchronous enough; CloseNow on next write failure.
	}
	t.pending.failAll("client closed")
	return nil
}

// trackSub records subscriptions so a reconnect can re-issue $subscribe.
func (t *wsTransport) trackSub(env Envelope) {
	if env.Op == "$subscribe" {
		if m, ok := env.Input.(map[string]any); ok {
			var f SubscriptionFilter
			if c, ok := m["component"].(string); ok {
				f.Component = c
			}
			if tags, ok := toStringSlice(m["tags"]); ok {
				f.Tags = tags
			}
			t.mu.Lock()
			t.subs[filterKey(f)] = f
			t.mu.Unlock()
		}
	}
}

// untrackSub forgets subscriptions on $unsubscribe.
func (t *wsTransport) untrackSub(env Envelope) {
	if env.Op != "$unsubscribe" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if env.Input == nil {
		t.subs = map[string]SubscriptionFilter{}
		return
	}
	if m, ok := env.Input.(map[string]any); ok {
		var f SubscriptionFilter
		if c, ok := m["component"].(string); ok {
			f.Component = c
		}
		if tags, ok := toStringSlice(m["tags"]); ok {
			f.Tags = tags
		}
		delete(t.subs, filterKey(f))
	}
}

// onClosed fails in-flight requests and reconnects unless the user closed.
func (t *wsTransport) onClosed(c *websocket.Conn) {
	t.mu.Lock()
	if t.conn == c {
		t.conn = nil
	}
	if t.wcancel != nil {
		t.wcancel()
		t.wcancel = nil
	}
	userClosed := t.closedByUser
	reconnecting := t.reconnecting
	if !userClosed && !reconnecting {
		t.reconnecting = true
	}
	t.mu.Unlock()

	t.pending.failAll("connection closed")
	if userClosed || reconnecting {
		return
	}
	go t.reconnectLoop()
}

// reconnectLoop retries the dial with a fixed 1s backoff until it succeeds or
// the user closes the transport, then re-issues subscriptions.
func (t *wsTransport) reconnectLoop() {
	for {
		t.mu.Lock()
		stop := t.closedByUser
		t.mu.Unlock()
		if stop {
			t.mu.Lock()
			t.reconnecting = false
			t.mu.Unlock()
			return
		}
		time.Sleep(time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := t.connect(ctx)
		cancel()
		if err == nil {
			t.mu.Lock()
			t.reconnecting = false
			t.mu.Unlock()
			return
		}
	}
}

// keepaliveLoop sends an application-layer $ping whenever the connection has
// been idle for keepAlive (spec v0.2 §4.3). A missing reply within
// pongTimeout kills the connection, triggering reconnect. A 0.1 server's
// 40002 UNKNOWN_OP permanently disables keepalive for this transport.
func (t *wsTransport) keepaliveLoop(c *websocket.Conn, rctx context.Context) {
	for {
		select {
		case <-rctx.Done():
			return
		case <-time.After(t.keepAlive):
		}
		t.mu.Lock()
		if t.conn != c || t.keepaliveOff {
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()

		pctx, cancel := context.WithTimeout(rctx, t.pongTimeout)
		env := Envelope{ACP: ProtocolVersion, ID: fmt.Sprintf("ka-%d", nowMilli()), Op: "$ping",
			Input: map[string]any{"ts": nowMilli()}}
		_, err := t.Request(pctx, env)
		cancel()
		if err != nil {
			var ae *ACPError
			if errors.As(err, &ae) && ae.Code == CodeUnknownOp {
				t.mu.Lock()
				t.keepaliveOff = true
				t.mu.Unlock()
				return
			}
			// Timeout or dead connection: terminate; reconnect follows.
			if t.connStillActive(c) {
				_ = c.CloseNow()
			}
			return
		}
	}
}

func (t *wsTransport) connStillActive(c *websocket.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn == c
}

// mustMarshalFrame marshals an envelope into a sendable frame.
func mustMarshalFrame(env Envelope) map[string]any {
	b, err := json.Marshal(env)
	if err != nil {
		return map[string]any{"acp": env.ACP, "id": env.ID, "op": env.Op}
	}
	var f map[string]any
	if err := json.Unmarshal(b, &f); err != nil {
		return map[string]any{"acp": env.ACP, "id": env.ID, "op": env.Op}
	}
	return f
}
