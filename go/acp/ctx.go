package acp

import "context"

// ctxKey is the unexported context key carrying the CallContext.
type ctxKey struct{}

// CallContext is the per-call context handed to component handlers, reachable
// from the standard context.Context via FromContext. It exposes the
// connection the request arrived on, the raw request envelope, the reserved
// meta area, and event emission (spec v0.2 §6.2).
type CallContext struct {
	// Conn is the connection the request arrived on.
	Conn *Conn
	// Request is the raw request envelope (meta included; servers ignore
	// meta semantics).
	Request Envelope
	// Meta is shorthand for Request.Meta.
	Meta map[string]any

	emit func(Event)
}

// Emit pushes an $event to all subscribers of the emitting component
// (spec v0.2 §6.2). Component defaults to the emitting component's id and
// tags default to the descriptor tags.
func (cc *CallContext) Emit(ev Event) {
	if cc != nil && cc.emit != nil {
		cc.emit(ev)
	}
}

// FromContext returns the CallContext stored in ctx by the server, or nil
// when ctx was not produced by an ACP call dispatch.
func FromContext(ctx context.Context) *CallContext {
	if ctx == nil {
		return nil
	}
	cc, _ := ctx.Value(ctxKey{}).(*CallContext)
	return cc
}
