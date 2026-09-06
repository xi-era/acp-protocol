package acp

import (
	"fmt"
	"sync"

	"github.com/xeipuuv/gojsonschema"
)

// registered pairs a component with its precompiled draft-07 validators.
type registered struct {
	comp         Component
	inputSchema  *gojsonschema.Schema
	outputSchema *gojsonschema.Schema
}

// Registry maps component id -> Component, with descriptor projection.
type Registry struct {
	mu    sync.RWMutex
	comps map[string]*registered
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{comps: map[string]*registered{}}
}

// compileSchema compiles a draft-07 schema; empty input yields nil ("any").
func compileSchema(raw []byte) (*gojsonschema.Schema, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	return gojsonschema.NewSchema(gojsonschema.NewBytesLoader(raw))
}

// Register validates and stores a component. Duplicate ids, invalid
// component_id grammar (spec §7.1), missing handlers and malformed schemas
// are rejected.
func (r *Registry) Register(c Component) error {
	if !IsValidComponentID(c.ID) {
		return fmt.Errorf("invalid component id: %q (spec §7.1)", c.ID)
	}
	if c.Handle == nil {
		return fmt.Errorf("component %s: handle must be a function", c.ID)
	}
	inputSchema, err := compileSchema(c.InputSchema)
	if err != nil {
		return fmt.Errorf("component %s: invalid inputSchema: %w", c.ID, err)
	}
	outputSchema, err := compileSchema(c.OutputSchema)
	if err != nil {
		return fmt.Errorf("component %s: invalid outputSchema: %w", c.ID, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.comps == nil {
		r.comps = map[string]*registered{}
	}
	if _, exists := r.comps[c.ID]; exists {
		return fmt.Errorf("component already registered: %s", c.ID)
	}
	r.comps[c.ID] = &registered{comp: c, inputSchema: inputSchema, outputSchema: outputSchema}
	return nil
}

// Get returns the registered component by id.
func (r *Registry) Get(id string) (Component, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.comps[id]
	if !ok {
		return Component{}, false
	}
	return reg.comp, true
}

// Has reports whether a component with the id is registered.
func (r *Registry) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.comps[id]
	return ok
}

// Descriptors projects every registered component into its descriptor.
func (r *Registry) Descriptors() []ComponentDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ComponentDescriptor, 0, len(r.comps))
	for _, reg := range r.comps {
		out = append(out, reg.comp.descriptor())
	}
	return out
}
