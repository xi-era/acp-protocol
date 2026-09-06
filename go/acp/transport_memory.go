package acp

import (
	"context"
	"encoding/json"
	"iter"
	"sync"
)

// MemoryTransport connects a Client directly to a Server dispatch in the same
// process — no network loopback. Used by unit tests and by adapters bridging
// protocols inside a single process. It is stateful: events (v0.2) are routed
// to registered handlers.
type MemoryTransport struct {
	srv *Server

	mu       sync.Mutex
	conn     *Conn
	started  bool
	pending  pendingMap
	handlers map[int]func(Event)
	seq      int
	orphan   map[string]any
}

// NewMemoryTransport binds a client transport to a server instance. The
// persistent connection is attached to the server on first use, so
// subscriptions and $events work exactly as over WS/stdio.
func NewMemoryTransport(srv *Server) *MemoryTransport {
	return &MemoryTransport{srv: srv, handlers: map[int]func(Event){}}
}

var _ Transport = (*MemoryTransport)(nil)

// ensureConn lazily creates the persistent connection and registers it with
// the server (lifecycle onConnection). Frames are JSON round-tripped so the
// client sees exactly the decoded shape other transports deliver.
func (t *MemoryTransport) ensureConn() *Conn {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn == nil {
		t.conn = &Conn{Meta: map[string]any{"transport": "memory"}, Ctx: context.Background()}
		t.conn.send = func(f frame) error {
			b, err := json.Marshal(f)
			if err != nil {
				return err
			}
			var out map[string]any
			if err := json.Unmarshal(b, &out); err != nil {
				return err
			}
			t.route(out)
			return nil
		}
		t.srv.attach(t.conn)
	}
	return t.conn
}

// route delivers a server frame: events to handlers, everything else to the
// pending request with the matching id (orphaned frames are stashed, mirroring
// id-mismatch fallbacks in the TS memory transport).
func (t *MemoryTransport) route(f frame) {
	if _, isEvent := f["event"]; isEvent {
		var ev Event
		if b, err := json.Marshal(jsonDecode(f["event"])); err == nil {
			if err := json.Unmarshal(b, &ev); err == nil {
				t.dispatchEvent(ev)
			}
		}
		return
	}
	if _, hasOp := f["op"]; hasOp {
		return // server-initiated requests are not expected in memory
	}
	id, _ := f["id"].(string)
	if p := t.pending.get(id); p != nil {
		p.push(f)
		return
	}
	t.mu.Lock()
	t.orphan = f
	t.mu.Unlock()
}

func (t *MemoryTransport) dispatchEvent(ev Event) {
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

// Request runs the dispatch synchronously: replies (and events emitted by the
// handler) arrive via route before it returns. The pending is registered
// BEFORE dispatch — replies can arrive synchronously.
func (t *MemoryTransport) Request(ctx context.Context, env Envelope) (map[string]any, error) {
	conn := t.ensureConn()
	p := newPending()
	t.pending.set(env.ID, p)

	raw := envToRaw(env)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.srv.Handle(raw, conn)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.pending.remove(env.ID)
		return nil, ctx.Err()
	}

	if f, ok := p.pop(); ok && isTerminalFrame(f) {
		t.pending.remove(env.ID)
		return f, nil
	}
	t.pending.remove(env.ID)
	// No matching terminal frame (e.g. id:null error replies for malformed
	// envelopes): fall back to the stashed orphan frame.
	t.mu.Lock()
	orphan := t.orphan
	t.orphan = nil
	t.mu.Unlock()
	if orphan != nil {
		return orphan, nil
	}
	return ErrorEnvelope(env.ID, CodeInternalError, "no reply from server", nil, env.ACP), nil
}

// RequestStream runs the dispatch on a goroutine and iterates the chunk
// frames as the handler produces them.
func (t *MemoryTransport) RequestStream(ctx context.Context, env Envelope) (iter.Seq2[map[string]any, error], error) {
	conn := t.ensureConn()
	p := newPending()
	t.pending.set(env.ID, p)
	raw := envToRaw(env)
	go func() {
		defer func() {
			// Guarantee the pending eventually terminates even if the
			// dispatcher sends nothing.
			p.mu.Lock()
			sent := len(p.chunks) > 0 && isTerminalFrame(p.chunks[len(p.chunks)-1])
			p.mu.Unlock()
			if !sent {
				p.push(ErrorEnvelope(env.ID, CodeInternalError, "no reply from server", nil, env.ACP))
			}
			t.pending.remove(env.ID)
		}()
		t.srv.Handle(raw, conn)
	}()
	return p.iterate(ctx), nil
}

// Close detaches the persistent connection (dropping its subscriptions).
func (t *MemoryTransport) Close() error {
	t.mu.Lock()
	conn := t.conn
	t.conn = nil
	t.mu.Unlock()
	if conn != nil {
		t.srv.detach(conn)
	}
	t.pending.failAll("client closed")
	return nil
}

// EventsSupported reports event capability (stateful transport).
func (t *MemoryTransport) EventsSupported() bool { return true }

// OnEvent registers an event handler; returns the unregister func.
func (t *MemoryTransport) OnEvent(handler func(Event)) func() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	id := t.seq
	t.handlers[id] = handler
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.handlers, id)
	}
}

// envToRaw round-trips an Envelope through JSON so the server sees exactly
// the decoded shape other transports deliver (numbers as float64 etc.).
func envToRaw(env Envelope) any {
	b, err := json.Marshal(env)
	if err != nil {
		return map[string]any{}
	}
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return map[string]any{}
	}
	return raw
}
