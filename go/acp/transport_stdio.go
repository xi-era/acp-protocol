package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"iter"
	"strings"
	"sync"
)

// StdioClientTransport is the client side of the stdio transport (spec §11):
// one envelope per line. `in` carries the server's replies (its stdout),
// `out` receives the client's requests (the server's stdin). Events and
// server-initiated $ping frames are answered on the same line protocol.
//
// It is typically paired with Server.ServeStdio over an io.Pipe pair; the
// production form is a child process talking to os.Stdin/os.Stdout.
type StdioClientTransport struct {
	in  io.Reader
	out io.Writer

	mu       sync.Mutex
	pending  pendingMap
	handlers map[int]func(Event)
	seq      int
	wmu      sync.Mutex
	started  bool
}

// NewStdioClientTransport builds a stdio client transport. `in` is the
// stream the server writes to (its stdout); `out` is the stream the server
// reads from (its stdin).
func NewStdioClientTransport(in io.Reader, out io.Writer) *StdioClientTransport {
	t := &StdioClientTransport{in: in, out: out, handlers: map[int]func(Event){}}
	return t
}

var _ Transport = (*StdioClientTransport)(nil)

// start launches the read loop once.
func (t *StdioClientTransport) start() {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return
	}
	t.started = true
	t.mu.Unlock()
	go t.readLoop()
}

// readLoop scans reply lines and routes them to pending requests, event
// handlers, or the $ping auto-answer.
func (t *StdioClientTransport) readLoop() {
	sc := bufio.NewScanner(t.in)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var f map[string]any
		if err := json.Unmarshal([]byte(line), &f); err != nil {
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

func (t *StdioClientTransport) answerPing(f map[string]any) {
	result := map[string]any{"pong": float64(nowMilli())}
	if m, ok := f["input"].(map[string]any); ok {
		if ts, ok := m["ts"].(float64); ok {
			result["ts"] = ts
		}
	}
	acp, _ := f["acp"].(string)
	id, _ := f["id"].(string)
	_ = t.writeLine(okFrame(acp, id, result))
}

func (t *StdioClientTransport) dispatchEvent(ev Event) {
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

// writeLine writes one envelope line, serialized across goroutines.
func (t *StdioClientTransport) writeLine(f map[string]any) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	t.wmu.Lock()
	defer t.wmu.Unlock()
	_, err = t.out.Write(b)
	return err
}

// sendRawLine writes an arbitrary line (conformance PARSE_ERROR probing).
func (t *StdioClientTransport) sendRawLine(text string) error {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	_, err := io.WriteString(t.out, text+"\n")
	return err
}

// Request writes the envelope line and waits for the terminal reply frame.
// The pending is registered before the write: the server's reply can arrive
// synchronously (flowing-mode pipes) before Request regains control.
func (t *StdioClientTransport) Request(ctx context.Context, env Envelope) (map[string]any, error) {
	t.start()
	p := newPending()
	t.pending.set(env.ID, p)
	if err := t.writeLine(mustMarshalFrame(env)); err != nil {
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

// RequestStream writes the envelope line and iterates chunk frames.
func (t *StdioClientTransport) RequestStream(ctx context.Context, env Envelope) (iter.Seq2[map[string]any, error], error) {
	t.start()
	p := newPending()
	t.pending.set(env.ID, p)
	if err := t.writeLine(mustMarshalFrame(env)); err != nil {
		t.pending.remove(env.ID)
		return nil, err
	}
	inner := p.iterate(ctx)
	return func(yield func(map[string]any, error) bool) {
		defer t.pending.remove(env.ID)
		inner(yield)
	}, nil
}

// Close stops the read loop's source; the underlying streams are caller-owned.
func (t *StdioClientTransport) Close() error {
	t.pending.failAll("client closed")
	if closer, ok := t.in.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

// EventsSupported reports event capability (stateful transport).
func (t *StdioClientTransport) EventsSupported() bool { return true }

// OnEvent registers an event handler; returns the unregister func.
func (t *StdioClientTransport) OnEvent(handler func(Event)) func() {
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
