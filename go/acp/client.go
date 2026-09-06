package acp

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SubscriptionFilter filters event subscriptions: exactly one of Component /
// Tags must be set (spec v0.2 §4.4).
type SubscriptionFilter struct {
	Component string   `json:"component,omitempty"`
	Tags      []string `json:"tags,omitempty"`
}

// Subscription is the handle returned by Client.Subscribe.
type Subscription struct {
	filter  SubscriptionFilter
	handler func(Event)
	off     func()
	client  *Client
}

// Unsubscribe removes the subscription (spec v0.2 §4.4, $unsubscribe). Events
// already delivered are not replayed.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	return s.client.unsubscribe(ctx, s)
}

// Transport moves envelopes between a client and a server. Request resolves
// with the single decoded reply envelope (including failure envelopes);
// RequestStream iterates chunk frames until the end frame. Connectionless
// transports (HTTP) report EventsSupported false.
type Transport interface {
	Request(ctx context.Context, env Envelope) (map[string]any, error)
	RequestStream(ctx context.Context, env Envelope) (iter.Seq2[map[string]any, error], error)
	Close() error
	EventsSupported() bool
	// OnEvent registers an event handler and returns the unregister func.
	OnEvent(handler func(Event)) (off func())
}

// ClientOptions configures an ACP client.
type ClientOptions struct {
	// Timeout is the per-call timeout; default 30s. Exceeding it fails the
	// call with ACPError code 50400 TIMEOUT.
	Timeout time.Duration
	// ProtocolVersion is the version declared on requests; default "0.2".
	// On a 40003 reply the client retries once with the highest server-
	// supported version and locks it (spec v0.2 §12.2).
	ProtocolVersion string
	// Headers are sent with HTTP requests and the WS handshake.
	Headers http.Header
	// Transport injects a transport, taking precedence over URL.
	Transport Transport
	// KeepAlive is the WS idle interval for $ping pings (spec v0.2 §4.3);
	// zero means the 30s default, a negative value disables keepalive.
	KeepAlive time.Duration
	// PongTimeout fails the connection (triggering reconnect) when no $ping
	// reply arrives in time; default 10s.
	PongTimeout time.Duration
}

// Client calls ACP servers over any transport. The transport is auto-selected
// from the URL scheme (http(s) -> HTTP, ws(s) -> WebSocket) or injected via
// ClientOptions.Transport.
type Client struct {
	timeout    time.Duration
	keepAlive  time.Duration
	pongTime   time.Duration
	headers    http.Header
	transport  Transport
	url        string
	idSeq      uint64
	mu         sync.Mutex
	version    string
	fallbacked bool
}

// NewClient builds a client for a "http(s)://host:port/acp",
// "ws(s)://host:port/acp" endpoint, or with an injected transport via
// ClientOptions.Transport (e.g. memory / stdio for tests and adapters).
func NewClient(url string, opts ClientOptions) (*Client, error) {
	c := newClient(opts)
	if opts.Transport != nil {
		c.transport = opts.Transport
		return c, nil
	}
	if url == "" {
		return nil, fmt.Errorf("acp client requires either url or transport")
	}
	scheme := url
	if i := strings.IndexByte(scheme, ':'); i >= 0 {
		scheme = strings.ToLower(scheme[:i])
	}
	c.url = url
	switch scheme {
	case "http", "https":
		c.transport = newHTTPTransport(url, opts.Headers)
	case "ws", "wss":
		c.transport = newWSTransport(url, opts.Headers, c.keepAlive, c.pongTime)
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s (use http(s):// or ws(s)://, or inject a transport)", scheme)
	}
	return c, nil
}

func newClient(opts ClientOptions) *Client {
	c := &Client{timeout: 30 * time.Second, keepAlive: 30 * time.Second, pongTime: 10 * time.Second}
	if opts.Timeout > 0 {
		c.timeout = opts.Timeout
	}
	c.version = orDefault(opts.ProtocolVersion, ProtocolVersion)
	if opts.KeepAlive != 0 {
		if opts.KeepAlive < 0 {
			c.keepAlive = 0
		} else {
			c.keepAlive = opts.KeepAlive
		}
	}
	if opts.PongTimeout > 0 {
		c.pongTime = opts.PongTimeout
	}
	c.headers = opts.Headers
	return c
}

// transportInstance returns the bound transport.
func (c *Client) transportInstance() Transport { return c.transport }

// nextID generates a fresh request correlation id.
func (c *Client) nextID() string {
	var b [12]byte
	if _, err := crand.Read(b[:]); err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.idSeq++
		return fmt.Sprintf("acp-%d-%d", nowMilli(), c.idSeq)
	}
	return "acp-" + hex.EncodeToString(b[:])
}

// Discover lists components (empty id = all) or fetches a single one by id;
// a missing component yields an empty slice (spec §4.1).
func (c *Client) Discover(ctx context.Context, componentID string) ([]ComponentDescriptor, error) {
	env := Envelope{Op: "discover"}
	if componentID != "" {
		env.Component = componentID
	}
	reply, err := c.request(ctx, env)
	if err != nil {
		return nil, err
	}
	if err := asACPError(reply); err != nil {
		return nil, err
	}
	result, _ := reply["result"].(map[string]any)
	raw, _ := result["components"]
	raw = jsonDecode(raw)
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var comps []ComponentDescriptor
	if err := json.Unmarshal(b, &comps); err != nil {
		return nil, err
	}
	return comps, nil
}

// Call performs a single call and decodes the bare result into out (out may
// be nil to discard it). Server error frames surface as *ACPError.
func (c *Client) Call(ctx context.Context, componentID string, input any, out any) error {
	env := Envelope{Op: "call", Component: componentID, Input: input}
	reply, err := c.request(ctx, env)
	if err != nil {
		return err
	}
	if err := asACPError(reply); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	b, err := json.Marshal(jsonDecode(reply["result"]))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

// CallStream performs a streamed call (stream:true) and returns an iterator
// yielding chunks in seq order; iteration ends after the end frame. An error
// frame terminating the stream yields a *ACPError. The second return value is
// the immediate error (e.g. transport failure), not a per-chunk error.
func (c *Client) CallStream(ctx context.Context, componentID string, input any) (iter.Seq2[Chunk, error], error) {
	env := Envelope{Op: "call", Component: componentID, Input: input, Stream: true}
	frames, err := c.transportInstance().RequestStream(ctx, env)
	if err != nil {
		return nil, err
	}
	seq := func(yield func(Chunk, error) bool) {
		for f, ferr := range frames {
			if ferr != nil {
				yield(Chunk{}, ferr)
				return
			}
			if raw, ok := f["chunk"].(map[string]any); ok {
				chunk := chunkFromMap(raw)
				if !yield(chunk, nil) {
					return
				}
				if chunk.End {
					return
				}
				continue
			}
			if isErrFrame(f) {
				ae, _ := asACPError(f)
				yield(Chunk{}, ae)
				return
			}
			// One-shot reply to a stream request: nothing to iterate.
			return
		}
	}
	return seq, nil
}

// chunkFromMap converts a decoded chunk object into a Chunk.
func chunkFromMap(m map[string]any) Chunk {
	c := Chunk{}
	if v, ok := m["seq"].(float64); ok {
		c.Seq = int(v)
	}
	if v, ok := m["end"].(bool); ok {
		c.End = v
	}
	c.Data = m["data"]
	if v, ok := m["bin"].(bool); ok {
		c.Bin = v
	}
	return c
}

// asACPError converts a failure envelope into an *ACPError, or returns nil.
func asACPError(f map[string]any) error {
	if !isErrFrame(f) {
		return nil
	}
	body, ok := errBodyOf(f)
	if !ok {
		return &ACPError{Code: CodeInternalError, Message: "unexpected reply shape"}
	}
	return &ACPError{Code: body.Code, Message: body.Message, Data: body.Data}
}

// Request is the low-level escape hatch: it sends a full envelope and returns
// the decoded reply envelope whatever its shape (ok / error / chunk / event).
func (c *Client) Request(ctx context.Context, env Envelope) (map[string]any, error) {
	return c.request(ctx, env)
}

// request sends the envelope, applies the timeout and runs the 40003
// fallback ladder (spec v0.2 §12.2): retry once with the highest
// server-supported version and lock it for this client.
func (c *Client) request(ctx context.Context, env Envelope) (map[string]any, error) {
	if env.ACP == "" {
		env.ACP = c.version
	}
	if env.ID == "" {
		env.ID = c.nextID()
	}
	cctx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.timeout > 0 {
		cctx, cancel = context.WithTimeout(ctx, c.timeout)
	}
	t := c.transportInstance()
	reply, err := t.Request(cctx, env)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		return nil, mapClientErr(err, env.ID, c.timeout)
	}

	// Fallback ladder step 1 (spec v0.2 §12.2).
	if isErrFrame(reply) && !c.fallbacked {
		if body, ok := errBodyOf(reply); ok && body.Code == CodeUnsupportedVersion {
			if best := pickHighestSupported(body.Data); best != "" && best != c.version {
				c.version = best
				c.fallbacked = true
				return c.request(ctx, env)
			}
		}
	}
	return reply, nil
}

// mapClientErr converts transport/context failures: deadline exceeded maps to
// 50400 TIMEOUT; cancellation passes through; other errors are wrapped in an
// ACPError 50000.
func mapClientErr(err error, id string, timeout time.Duration) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &ACPError{Code: CodeTimeout,
			Message: fmt.Sprintf("call %s timed out after %s", id, timeout)}
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	var ae *ACPError
	if errors.As(err, &ae) {
		return ae
	}
	return &ACPError{Code: CodeInternalError, Message: err.Error()}
}

// Subscribe registers an event subscription (spec v0.2 §4.4). Exactly one of
// filter.Component / filter.Tags must be set. On transports without event
// support (HTTP) it fails with ACPError code 50100. The handler is registered
// before the $subscribe round-trip so no event can race the setup.
func (c *Client) Subscribe(ctx context.Context, filter SubscriptionFilter, handler func(Event)) (*Subscription, error) {
	hasComponent := filter.Component != ""
	hasTags := len(filter.Tags) > 0
	if hasComponent == hasTags {
		return nil, &ACPError{Code: CodeInvalidEnvelope,
			Message: "subscription filter requires exactly one of component/tags"}
	}
	t := c.transportInstance()
	if !t.EventsSupported() {
		return nil, &ACPError{Code: CodeEventUnsupported,
			Message: "events unsupported on connectionless transport"}
	}
	off := t.OnEvent(handler)
	sub := &Subscription{filter: filter, handler: handler, off: off, client: c}
	env := Envelope{Op: "$subscribe", Input: jsonDecode(filter)}
	reply, err := c.request(ctx, env)
	if err != nil {
		off()
		return nil, err
	}
	if aerr := asACPError(reply); aerr != nil {
		off()
		return nil, aerr
	}
	return sub, nil
}

// unsubscribe sends $unsubscribe for the subscription's filter and removes
// the local event handler.
func (c *Client) unsubscribe(ctx context.Context, sub *Subscription) error {
	env := Envelope{Op: "$unsubscribe", Input: jsonDecode(sub.filter)}
	reply, err := c.request(ctx, env)
	if sub.off != nil {
		sub.off()
	}
	if err != nil {
		return err
	}
	return asACPError(reply)
}

// Close releases the transport (closing WS connections, draining HTTP).
func (c *Client) Close() error {
	return c.transportInstance().Close()
}

// pickHighestSupported picks the highest version from a 40003
// `data.supported` payload (spec v0.2 §12.2).
func pickHighestSupported(data any) string {
	m, ok := data.(map[string]any)
	if !ok {
		return ""
	}
	arr, ok := m["supported"].([]any)
	if !ok {
		return ""
	}
	best := ""
	var bestMajor, bestMinor int
	for _, item := range arr {
		v, ok := item.(string)
		if !ok {
			continue
		}
		major, minor, valid := parseVersion(v)
		if !valid {
			continue
		}
		if best == "" || major > bestMajor || (major == bestMajor && minor > bestMinor) {
			best, bestMajor, bestMinor = v, major, minor
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Shared pending state for stateful client transports (ws / stdio / memory).
// Replies can arrive synchronously with the send (memory dispatch, stdio
// pipes), so the pending MUST be registered before the request is written.
// ---------------------------------------------------------------------------

type pendingState struct {
	mu     sync.Mutex
	chunks []map[string]any
	done   bool
	notify chan struct{}
	term   chan map[string]any
}

func newPending() *pendingState {
	return &pendingState{notify: make(chan struct{}, 1), term: make(chan map[string]any, 1)}
}

// isTerminalFrame reports whether a decoded frame ends a request's stream.
func isTerminalFrame(f map[string]any) bool {
	if _, ok := f["ok"]; ok {
		return true
	}
	if raw, ok := f["chunk"].(map[string]any); ok {
		end, _ := raw["end"].(bool)
		return end
	}
	return false
}

func (p *pendingState) push(f map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chunks = append(p.chunks, f)
	if isTerminalFrame(f) {
		p.done = true
		select {
		case p.term <- f:
		default:
		}
	}
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *pendingState) pop() (map[string]any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.chunks) == 0 {
		return nil, false
	}
	f := p.chunks[0]
	p.chunks = p.chunks[1:]
	return f, true
}

func (p *pendingState) finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done && len(p.chunks) == 0
}

// waitTerminal resolves the request's single reply (first terminal frame).
func (p *pendingState) waitTerminal(ctx context.Context) (map[string]any, error) {
	select {
	case f := <-p.term:
		return f, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// iterate drains chunk frames in arrival order until the terminal frame.
func (p *pendingState) iterate(ctx context.Context) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for {
			if f, ok := p.pop(); ok {
				if !yield(f, nil) {
					return
				}
				if isTerminalFrame(f) {
					return
				}
				continue
			}
			if p.finished() {
				return
			}
			select {
			case <-p.notify:
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			}
		}
	}
}

// pendingMap tracks in-flight requests by correlation id.
type pendingMap struct {
	mu      sync.Mutex
	pending map[string]*pendingState
}

func (m *pendingMap) get(id string) *pendingState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[id]
}

func (m *pendingMap) set(id string, p *pendingState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == nil {
		m.pending = map[string]*pendingState{}
	}
	m.pending[id] = p
}

func (m *pendingMap) remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, id)
}

// failAll resolves every pending request with a synthetic internal error
// frame (used when the connection dies).
func (m *pendingMap) failAll(message string) {
	m.mu.Lock()
	all := make([]*pendingState, 0, len(m.pending))
	for _, p := range m.pending {
		all = append(all, p)
	}
	m.pending = map[string]*pendingState{}
	m.mu.Unlock()
	for _, p := range all {
		p.push(ErrorEnvelope(nil, CodeInternalError, message, nil, ProtocolVersion))
	}
}
