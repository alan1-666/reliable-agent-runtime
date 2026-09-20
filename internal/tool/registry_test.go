package tool

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRegistryRejectsDuplicateName(t *testing.T) {
	registry := NewRegistry()
	handler := Function{
		Def: Definition{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)},
		Run: func(context.Context, json.RawMessage) (Result, error) {
			return Result{}, nil
		},
	}
	if err := registry.Register(handler); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(handler); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("error = %v, want ErrAlreadyRegistered", err)
	}
}

func TestRegistryRejectsInvalidArguments(t *testing.T) {
	registry := NewRegistry()
	handler := Function{
		Def: Definition{
			Name:        "echo",
			InputSchema: json.RawMessage(`{"type":"object","required":["message"],"properties":{"message":{"type":"string"}},"additionalProperties":false}`),
		},
		Run: func(context.Context, json.RawMessage) (Result, error) {
			return Result{}, nil
		},
	}
	if err := registry.Register(handler); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Execute(context.Background(), "echo", json.RawMessage(`{"message":4}`)); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("error = %v, want ErrInvalidArguments", err)
	}
}
