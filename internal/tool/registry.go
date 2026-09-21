package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"safemarket/agent-runtime/internal/schema"
)

var (
	ErrAlreadyRegistered = errors.New("tool already registered")
	ErrNotFound          = errors.New("tool not found")
	ErrInvalidArguments  = errors.New("invalid tool arguments")
)

type Definition struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	ReadOnly    bool
}

type Result struct {
	Content string
}

type Handler interface {
	Definition() Definition
	Execute(context.Context, json.RawMessage) (Result, error)
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(handler Handler) error {
	definition := handler.Definition()
	if definition.Name == "" {
		return fmt.Errorf("%w: invalid definition", ErrInvalidArguments)
	}
	if err := schema.Check(definition.InputSchema); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[definition.Name]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, definition.Name)
	}
	r.handlers[definition.Name] = handler
	return nil
}

func (r *Registry) Definitions() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	definitions := make([]Definition, 0, len(r.handlers))
	for _, handler := range r.handlers {
		definitions = append(definitions, handler.Definition())
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions
}

func (r *Registry) Definition(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[name]
	if !exists {
		return Definition{}, false
	}
	return handler.Definition(), true
}

func (r *Registry) Execute(ctx context.Context, name string, arguments json.RawMessage) (Result, error) {
	r.mu.RLock()
	handler, exists := r.handlers[name]
	r.mu.RUnlock()
	if !exists {
		return Result{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if err := schema.Validate(handler.Definition().InputSchema, arguments); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidArguments, err)
	}
	return handler.Execute(ctx, arguments)
}

type Function struct {
	Def Definition
	Run func(context.Context, json.RawMessage) (Result, error)
}

func (f Function) Definition() Definition { return f.Def }

func (f Function) Execute(ctx context.Context, arguments json.RawMessage) (Result, error) {
	if f.Run == nil {
		return Result{}, errors.New("tool handler is nil")
	}
	return f.Run(ctx, arguments)
}
