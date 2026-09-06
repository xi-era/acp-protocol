package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xeipuuv/gojsonschema"
)

// ErrStreamRequired is returned by a streaming component's HandleFunc when it
// is invoked without a Sender (i.e. the request lacked stream:true). The
// server translates it into a 40005 STREAM_REQUIRED error envelope.
var ErrStreamRequired = errors.New("stream required")

// ServerOptions configures an ACP server (spec v0.2 §3-6, §12).
type ServerOptions struct {
	// Name is the server self-description reported in discover results.
	Name string
	// Version is the server's own semantic version (independent of the
	// protocol version); defaults to "0.0.0".
	Version string
	// ProtocolVersion is the ACP version served; defaults to "0.2".
	ProtocolVersion string
	// MaxSubscriptionsPerConn bounds event subscriptions per stateful
	// connection (spec v0.2 §4.4); default 64.
	MaxSubscriptionsPerConn int
	// QueueLimit bounds queued events per connection before new events are
	// dropped (spec v0.2 §6.2); default 256.
	QueueLimit int
	// ValidateOutput checks handler output against outputSchema (dev-time
	// self-check, spec §8 42201); default false.
	ValidateOutput bool
}

// subscription is one connection-scoped event registration.
type subscription struct {
	id        string
	component string
	tags      []string
}

// Conn is the server-side handle on one client connection. Transports create
// it and route frames through Send; the server core routes ops per connection.
type Conn struct {
	// Meta carries transport metadata; the "transport" key is one of
	// "http", "ws", "stdio", "memory".
	Meta map[string]any
	// Ctx is canceled when the underlying connection drops. Call handlers
	// derive their context from it (HTTP also applies meta.timeoutMs).
	Ctx context.Context

	send    func(f frame) error
	closeFn func()

	// backlog counts in-flight event frames for bounded delivery (§6.2).
	backlog atomic.Int64

	subMu sync.Mutex
	subs  map[string]*subscription
}

// Send pushes one response/chunk/error/event frame to the client.
func (c *Conn) Send(f frame) error {
	if c.send == nil {
		return nil
	}
	return c.send(f)
}

// Close tears down the underlying connection (transport-specific).
func (c *Conn) Close() {
	if c.closeFn != nil {
		c.closeFn()
	}
}

// Transport returns the value of Meta["transport"].
func (c *Conn) Transport() string {
	t, _ := c.Meta["transport"].(string)
	return t
}

func (c *Conn) addSub(sub *subscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if c.subs == nil {
		c.subs = map[string]*subscription{}
	}
	c.subs[sub.id] = sub
}

func (c *Conn) removeSub(id string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	delete(c.subs, id)
}

func (c *Conn) clearSubs() {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.subs = map[string]*subscription{}
}

func (c *Conn) subCount() int {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	return len(c.subs)
}

func (c *Conn) snapshotSubs() []*subscription {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	out := make([]*subscription, 0, len(c.subs))
	for _, s := range c.subs {
		out = append(out, s)
	}
	return out
}

// Server is the transport-agnostic ACP server core: envelope validation,
// op routing, schema validation, streaming, reserved ops ($ping/$subscribe/
// $unsubscribe) and event fan-out. Transports feed decoded envelopes to Handle.
type Server struct {
	opts struct {
		name            string
		version         string
		protocolVersion string
		maxSubs         int
		queueLimit      int
		validateOutput  bool
	}

	reg     *Registry
	mu      sync.Mutex
	conns   map[*Conn]struct{}
	subSeq  uint64
	stopped bool
}

// NewServer builds a Server with defaults applied.
func NewServer(opts ServerOptions) *Server {
	s := &Server{reg: NewRegistry(), conns: map[*Conn]struct{}{}}
	s.opts.name = opts.Name
	s.opts.version = orDefault(opts.Version, "0.0.0")
	s.opts.protocolVersion = orDefault(opts.ProtocolVersion, ProtocolVersion)
	s.opts.maxSubs = orDefaultInt(opts.MaxSubscriptionsPerConn, 64)
	s.opts.queueLimit = orDefaultInt(opts.QueueLimit, 256)
	s.opts.validateOutput = opts.ValidateOutput
	return s
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orDefaultInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// Register adds a component; errors mirror the TS registry (invalid id,
// duplicate, missing handle, bad schema).
func (s *Server) Register(c Component) error { return s.reg.Register(c) }

// ServerInfo returns the server self-description (discover result shape).
func (s *Server) ServerInfo() ServerInfo {
	return ServerInfo{Name: s.opts.name, Version: s.opts.version, Protocol: s.opts.protocolVersion}
}

// Descriptors returns all component descriptors.
func (s *Server) Descriptors() []ComponentDescriptor { return s.reg.Descriptors() }

// attach registers a stateful connection (HTTP conns are never attached —
// every POST is an ephemeral connection).
func (s *Server) attach(conn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.conns[conn] = struct{}{}
	}
}

// detach removes a connection and, by losing its subscriptions, ends its
// event delivery (spec v0.2 §6.2: 断线即失去订阅).
func (s *Server) detach(conn *Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	conn.clearSubs()
}

// Handle routes an already-JSON-decoded request for the given connection
// (spec §3.2 validation order). Replies — including stream chunks and error
// frames — are sent over conn. Responses echo the request's `acp` value
// (spec v0.2 §5.3). Safe for concurrent use.
func (s *Server) Handle(raw any, conn *Conn) {
	defer func() {
		if r := recover(); r != nil {
			_ = conn.Send(ErrorEnvelope(nil, CodeInternalError,
				fmt.Sprintf("internal error: %v", r), nil, ProtocolVersion))
		}
	}()

	req, verr := ValidateEnvelope(raw, s.opts.protocolVersion)
	if verr != nil {
		_ = conn.Send(ErrorEnvelope(verr.ID, verr.Code, verr.Message, verr.Data, verr.ACP))
		return
	}

	switch req.Op {
	case "discover":
		s.handleDiscover(req, conn)
	case "call":
		s.handleCall(req, conn)
	case "$ping":
		s.handlePing(req, conn)
	case "$subscribe":
		s.handleSubscribe(req, conn)
	case "$unsubscribe":
		s.handleUnsubscribe(req, conn)
	}
}

// handlePing answers an application-layer heartbeat (spec v0.2 §4.3):
// result.pong is mandatory, result.ts echoes the sender's ts when present.
func (s *Server) handlePing(req *Envelope, conn *Conn) {
	result := map[string]any{"pong": nowMilli()}
	if m, ok := req.Input.(map[string]any); ok {
		if ts, ok := m["ts"].(float64); ok {
			result["ts"] = ts
		}
	}
	_ = conn.Send(okFrame(req.ACP, req.ID, result))
}

// handleSubscribe registers a connection-scoped event subscription
// (spec v0.2 §4.4). HTTP connections answer 50100 EVENT_UNSUPPORTED; the
// per-connection cap answers 42902 SUBSCRIPTION_LIMIT.
func (s *Server) handleSubscribe(req *Envelope, conn *Conn) {
	if conn.Transport() == "http" {
		_ = conn.Send(ErrorEnvelope(req.ID, CodeEventUnsupported,
			"events unsupported on connectionless transport", nil, req.ACP))
		return
	}
	if conn.subCount() >= s.opts.maxSubs {
		_ = conn.Send(ErrorEnvelope(req.ID, CodeSubscriptionLimit,
			fmt.Sprintf("subscription limit reached (%d)", s.opts.maxSubs), nil, req.ACP))
		return
	}
	input, _ := req.Input.(map[string]any)
	s.mu.Lock()
	s.subSeq++
	id := fmt.Sprintf("s-%x", s.subSeq)
	s.mu.Unlock()
	sub := &subscription{id: id}
	if c, ok := input["component"].(string); ok {
		sub.component = c
	} else if tags, ok := toStringSlice(input["tags"]); ok {
		sub.tags = tags
	}
	conn.addSub(sub)
	_ = conn.Send(okFrame(req.ACP, req.ID, map[string]any{"subscription": sub.id}))
}

// handleUnsubscribe removes subscriptions. A nil input clears all of the
// connection's subscriptions; otherwise component (equality) or tags
// (superset) matching applies. The result is always null (spec §4.4).
func (s *Server) handleUnsubscribe(req *Envelope, conn *Conn) {
	if conn.Transport() == "http" {
		_ = conn.Send(ErrorEnvelope(req.ID, CodeEventUnsupported,
			"events unsupported on connectionless transport", nil, req.ACP))
		return
	}
	input, _ := req.Input.(map[string]any)
	if req.Input == nil {
		conn.clearSubs()
	} else if c, ok := input["component"].(string); ok {
		for _, sub := range conn.snapshotSubs() {
			if sub.component == c {
				conn.removeSub(sub.id)
			}
		}
	} else if tags, ok := toStringSlice(input["tags"]); ok {
		for _, sub := range conn.snapshotSubs() {
			if sub.tags == nil {
				continue
			}
			match := true
			for _, t := range tags {
				if !containsString(sub.tags, t) {
					match = false
					break
				}
			}
			if match {
				conn.removeSub(sub.id)
			}
		}
	}
	_ = conn.Send(okFrame(req.ACP, req.ID, nil))
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

// handleDiscover lists components, optionally filtered by exact component id
// and/or tag intersection (spec §4.1).
func (s *Server) handleDiscover(req *Envelope, conn *Conn) {
	descriptors := s.reg.Descriptors()
	if req.Component != "" {
		filtered := descriptors[:0]
		for _, d := range descriptors {
			if d.ID == req.Component {
				filtered = append(filtered, d)
			}
		}
		descriptors = filtered
	}
	if len(req.Tags) > 0 {
		filtered := descriptors[:0]
		for _, d := range descriptors {
			match := true
			for _, t := range req.Tags {
				if !containsString(d.Tags, t) {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, d)
			}
		}
		descriptors = filtered
	}
	_ = conn.Send(okFrame(req.ACP, req.ID, DiscoverResult{Server: s.ServerInfo(), Components: descriptors}))
}

// handleCall validates input, invokes the handler and frames the reply
// (spec §4.2, §6.1): bare result, 42200 on schema failure, 40005 when a
// streaming component is called without stream:true, single terminated chunk
// wrapping for non-streaming components called with stream:true.
func (s *Server) handleCall(req *Envelope, conn *Conn) {
	reg, ok := s.reg.lookup(req.Component)
	if !ok {
		_ = conn.Send(ErrorEnvelope(req.ID, CodeComponentNotFound,
			fmt.Sprintf("component not found: %s", req.Component), nil, req.ACP))
		return
	}

	if reg.inputSchema != nil {
		result, err := reg.inputSchema.Validate(gojsonschema.NewBytesLoader(mustJSON(req.Input)))
		if err == nil && !result.Valid() {
			msgs := make([]string, 0, len(result.Errors()))
			for _, e := range result.Errors() {
				msgs = append(msgs, e.String())
			}
			_ = conn.Send(ErrorEnvelope(req.ID, CodeInvalidInput, "input validation failed",
				map[string]any{"errors": msgs}, req.ACP))
			return
		}
	}

	// Handler context: connection context (HTTP: request context) plus
	// meta.timeoutMs when present (spec §3.1).
	ctx := conn.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if ms := timeoutFromMeta(req.Meta); ms > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	}

	cc := &CallContext{Conn: conn, Request: *req, Meta: req.Meta}
	cc.emit = func(ev Event) {
		tags := ev.Tags
		if tags == nil {
			tags = reg.comp.Tags
		}
		component := ev.Component
		if component == "" {
			component = reg.comp.ID
		}
		s.Emit(Event{Component: component, Tags: tags, Data: ev.Data, TS: ev.TS})
	}
	ctx = context.WithValue(ctx, ctxKey{}, cc)

	// Sender is handed to the handler only for stream:true requests.
	var seq int
	var sentChunks int
	var send Sender
	if req.Stream {
		send = func(chunk any) error {
			c := Chunk{Seq: seq, End: false}
			if bc, isBin := chunk.(BinaryChunk); isBin {
				c.Bin = true
				c.Data = bc.Data
			} else {
				c.Data = chunk
			}
			seq++
			sentChunks++
			return conn.Send(chunkFrame(req.ACP, req.ID, c))
		}
	}

	output, err := reg.comp.Handle(ctx, req.Input, send)
	if err != nil {
		s.sendHandleError(req, conn, err)
		return
	}

	// Dev-time output self-check (spec §8 42201).
	if s.opts.validateOutput && reg.outputSchema != nil {
		result, verr := reg.outputSchema.Validate(gojsonschema.NewBytesLoader(mustJSON(output)))
		if verr == nil && !result.Valid() {
			msgs := make([]string, 0, len(result.Errors()))
			for _, e := range result.Errors() {
				msgs = append(msgs, e.String())
			}
			_ = conn.Send(ErrorEnvelope(req.ID, CodeInvalidOutput, "output validation failed",
				map[string]any{"errors": msgs}, req.ACP))
			return
		}
	}

	if req.Stream {
		if sentChunks > 0 {
			// Streaming handler: append the mandatory terminator (spec §6.1).
			_ = conn.Send(chunkFrame(req.ACP, req.ID, Chunk{Seq: seq, End: true, Data: nil}))
		} else {
			// Non-streaming component + stream:true: single terminated chunk
			// wrapping the output (spec §4.2).
			_ = conn.Send(chunkFrame(req.ACP, req.ID, Chunk{Seq: 0, End: true, Data: output}))
		}
		return
	}
	_ = conn.Send(okFrame(req.ACP, req.ID, output))
}

// sendHandleError converts a handler error into a failure envelope: a
// handler that returned *ACPError passes its code through; anything else is
// a 50001 COMPONENT_ERROR.
func (s *Server) sendHandleError(req *Envelope, conn *Conn, err error) {
	if errors.Is(err, ErrStreamRequired) {
		_ = conn.Send(ErrorEnvelope(req.ID, CodeStreamRequired,
			fmt.Sprintf("component %s requires stream:true", req.Component), nil, req.ACP))
		return
	}
	var ae *ACPError
	if errors.As(err, &ae) {
		_ = conn.Send(ErrorEnvelope(req.ID, ae.Code, ae.Message, ae.Data, req.ACP))
		return
	}
	_ = conn.Send(ErrorEnvelope(req.ID, CodeComponentError,
		fmt.Sprintf("component handler threw: %v", err), nil, req.ACP))
}

// Emit pushes an $event to every matching subscription on every stateful
// connection (spec v0.2 §6.2). Delivery is best-effort, at-most-once with
// bounded per-connection queues: events are dropped when the queue limit is
// reached. Tags default to the source component's descriptor tags.
func (s *Server) Emit(ev Event) {
	tags := ev.Tags
	if tags == nil && ev.Component != "" {
		if c, ok := s.reg.Get(ev.Component); ok {
			tags = c.Tags
		}
	}
	if ev.Component == "" && len(tags) == 0 {
		return
	}
	f := eventFrame(s.opts.protocolVersion, Event{Component: ev.Component, Tags: tags, Data: ev.Data, TS: ev.TS})

	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, conn := range conns {
		if !connMatches(conn, ev.Component, tags) {
			continue
		}
		// Bounded queue: count in-flight event frames, drop new ones when
		// the limit is reached (spec v0.2 §6.2).
		if conn.backlog.Add(1) > int64(s.opts.queueLimit) {
			conn.backlog.Add(-1)
			continue
		}
		_ = conn.Send(f)
		conn.backlog.Add(-1)
	}
}

// connMatches reports whether any of the connection's subscriptions matches
// the event (spec v0.2 §6.2): component subscriptions compare the event's
// component id; tag subscriptions require sub.tags ⊆ event tags.
func connMatches(conn *Conn, component string, tags []string) bool {
	for _, sub := range conn.snapshotSubs() {
		if sub.component != "" {
			if sub.component == component {
				return true
			}
			continue
		}
		if sub.tags == nil || len(tags) == 0 {
			continue
		}
		match := true
		for _, t := range sub.tags {
			if !containsString(tags, t) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ServeStdio serves the registry over a line-delimited JSON stream
// (spec §11). One line is one envelope on `in`; replies are written to `out`
// one per line (stderr stays free for logs). The persistent connection lives
// until `in` reaches EOF. ServeStdio blocks; run it in a goroutine when the
// caller also needs to drive the server.
func (s *Server) ServeStdio(in io.Reader, out io.Writer) error {
	conn := &Conn{Meta: map[string]any{"transport": "stdio"}, Ctx: context.Background()}
	var wmu sync.Mutex
	conn.send = func(f frame) error {
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}
		b = append(b, '\n')
		wmu.Lock()
		defer wmu.Unlock()
		_, err = out.Write(b)
		return err
	}
	s.attach(conn)
	defer s.detach(conn)

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var parsed any
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			_ = conn.Send(ErrorEnvelope(nil, CodeParseError, "line is not valid JSON", nil, ProtocolVersion))
			continue
		}
		go s.Handle(parsed, conn)
	}
	return nil
}

// Shutdown closes every attached connection (ending their subscriptions and
// event delivery) and stops accepting new ones.
func (s *Server) Shutdown() {
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = map[*Conn]struct{}{}
	s.stopped = true
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}
