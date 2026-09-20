package schema

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateObject(t *testing.T) {
	definition := json.RawMessage(`{
		"type":"object",
		"properties":{"service":{"type":"string"},"limit":{"type":"integer"}},
		"required":["service"],
		"additionalProperties":false
	}`)

	if err := Validate(definition, json.RawMessage(`{"service":"order","limit":3}`)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []json.RawMessage{
		json.RawMessage(`{"limit":3}`),
		json.RawMessage(`{"service":4}`),
		json.RawMessage(`{"service":"order","unknown":true}`),
	} {
		if err := Validate(definition, invalid); !errors.Is(err, ErrValidation) {
			t.Fatalf("value %s: error = %v, want validation error", invalid, err)
		}
	}
}

func TestCheckRejectsUnsupportedType(t *testing.T) {
	err := Check(json.RawMessage(`{"type":"date"}`))
	if !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("error = %v, want invalid schema", err)
	}
}
