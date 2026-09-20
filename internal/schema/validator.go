package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrInvalidSchema = errors.New("invalid JSON schema")
	ErrValidation    = errors.New("JSON schema validation failed")
)

// Validate implements the deliberately small JSON Schema subset used by the
// first runtime version: type, required, properties, additionalProperties,
// items and enum. Unsupported keywords are ignored until a full validator is
// introduced behind the same package boundary.
func Validate(schemaJSON, valueJSON json.RawMessage) error {
	var definition map[string]any
	if err := decode(schemaJSON, &definition); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	if err := checkDefinition(definition, "$schema"); err != nil {
		return err
	}

	var value any
	if err := decode(valueJSON, &value); err != nil {
		return fmt.Errorf("%w: invalid JSON: %v", ErrValidation, err)
	}
	return validateValue(definition, value, "$")
}

func Check(schemaJSON json.RawMessage) error {
	var definition map[string]any
	if err := decode(schemaJSON, &definition); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	return checkDefinition(definition, "$schema")
}

func decode(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func checkDefinition(definition map[string]any, path string) error {
	if rawType, exists := definition["type"]; exists {
		typeName, ok := rawType.(string)
		if !ok || !supportedType(typeName) {
			return fmt.Errorf("%w: %s.type", ErrInvalidSchema, path)
		}
	}
	if rawProperties, exists := definition["properties"]; exists {
		properties, ok := rawProperties.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: %s.properties", ErrInvalidSchema, path)
		}
		for name, rawChild := range properties {
			child, ok := rawChild.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: %s.properties.%s", ErrInvalidSchema, path, name)
			}
			if err := checkDefinition(child, path+".properties."+name); err != nil {
				return err
			}
		}
	}
	if rawItems, exists := definition["items"]; exists {
		items, ok := rawItems.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: %s.items", ErrInvalidSchema, path)
		}
		return checkDefinition(items, path+".items")
	}
	return nil
}

func validateValue(definition map[string]any, value any, path string) error {
	if rawType, exists := definition["type"]; exists {
		if err := validateType(rawType.(string), value, path); err != nil {
			return err
		}
	}

	if rawEnum, exists := definition["enum"]; exists {
		values, ok := rawEnum.([]any)
		if !ok {
			return fmt.Errorf("%w: invalid enum at %s", ErrInvalidSchema, path)
		}
		matched := false
		for _, candidate := range values {
			if reflect.DeepEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: %s is not in enum", ErrValidation, path)
		}
	}

	object, isObject := value.(map[string]any)
	if isObject {
		if rawRequired, exists := definition["required"]; exists {
			required, ok := rawRequired.([]any)
			if !ok {
				return fmt.Errorf("%w: invalid required at %s", ErrInvalidSchema, path)
			}
			for _, rawName := range required {
				name, ok := rawName.(string)
				if !ok {
					return fmt.Errorf("%w: invalid required name at %s", ErrInvalidSchema, path)
				}
				if _, exists := object[name]; !exists {
					return fmt.Errorf("%w: missing %s.%s", ErrValidation, path, name)
				}
			}
		}

		properties := map[string]any{}
		if rawProperties, exists := definition["properties"]; exists {
			properties = rawProperties.(map[string]any)
		}
		allowAdditional := true
		if rawAdditional, exists := definition["additionalProperties"]; exists {
			if allowed, ok := rawAdditional.(bool); ok {
				allowAdditional = allowed
			}
		}
		for name, childValue := range object {
			rawChild, declared := properties[name]
			if !declared {
				if !allowAdditional {
					return fmt.Errorf("%w: unexpected %s.%s", ErrValidation, path, name)
				}
				continue
			}
			if err := validateValue(rawChild.(map[string]any), childValue, path+"."+name); err != nil {
				return err
			}
		}
	}

	if array, ok := value.([]any); ok {
		if rawItems, exists := definition["items"]; exists {
			items := rawItems.(map[string]any)
			for i, item := range array {
				if err := validateValue(items, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateType(typeName string, value any, path string) error {
	valid := false
	switch typeName {
	case "object":
		_, valid = value.(map[string]any)
	case "array":
		_, valid = value.([]any)
	case "string":
		_, valid = value.(string)
	case "boolean":
		_, valid = value.(bool)
	case "number":
		_, valid = value.(json.Number)
	case "integer":
		if number, ok := value.(json.Number); ok {
			_, err := number.Int64()
			valid = err == nil
		}
	case "null":
		valid = value == nil
	}
	if !valid {
		return fmt.Errorf("%w: %s must be %s", ErrValidation, path, typeName)
	}
	return nil
}

func supportedType(typeName string) bool {
	switch typeName {
	case "object", "array", "string", "boolean", "number", "integer", "null":
		return true
	default:
		return false
	}
}
