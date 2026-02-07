package dsc

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

// UnmarshalInput parses JSON input into the specified type.
func UnmarshalInput[T any](input json.RawMessage) (T, error) {
	var req T
	if err := json.Unmarshal(input, &req); err != nil {
		return req, fmt.Errorf("failed to parse input: %w", err)
	}
	return req, nil
}

// RequiredField represents a field that must have a value.
type RequiredField struct {
	Name  string
	Value string
}

// ValidateRequired checks that all required fields have values.
func ValidateRequired(fields ...RequiredField) error {
	for _, f := range fields {
		if f.Value == "" {
			return fmt.Errorf("%s is required", f.Name)
		}
	}
	return nil
}

// ValidateAtLeastOne checks that at least one of the values is non-empty.
func ValidateAtLeastOne(description string, values ...string) error {
	for _, v := range values {
		if v != "" {
			return nil
		}
	}
	return fmt.Errorf("either %s is required", description)
}

// PropertyDescriptions maps property names to their descriptions.
type PropertyDescriptions map[string]string

// MetadataConfig contains configuration for building resource metadata.
type MetadataConfig struct {
	Descriptions      PropertyDescriptions
	SchemaType        reflect.Type
	ResourceType      string
	Version           string
	Description       string
	SchemaDescription string
	ResourceName      string
	Tags              []string
}

// DefaultExitCodes returns the standard exit codes for DSC resources.
func DefaultExitCodes() map[string]string {
	return map[string]string{
		"0": "Success",
		"1": "Error",
		"2": "Resource error",
		"3": "JSON serialization error",
		"4": "Invalid input",
		"5": "Schema validation error",
		"6": "Resource not found",
	}
}

// BuildMetadata creates ResourceMetadata from a configuration.
func BuildMetadata(cfg MetadataConfig) ResourceMetadata {
	schema, _ := GenerateSchemaWithOptions(cfg.SchemaType, SchemaOptions{
		Descriptions:      cfg.Descriptions,
		SchemaDescription: cfg.SchemaDescription,
		ResourceName:      cfg.ResourceName,
	})

	version := cfg.Version
	if version == "" {
		version = "0.1.0"
	}

	return ResourceMetadata{
		Type:        cfg.ResourceType,
		Version:     version,
		Description: cfg.Description,
		Tags:        cfg.Tags,
		ExitCodes:   DefaultExitCodes(),
		Schema: ResourceSchema{
			Embedded: schema,
		},
	}
}

// NotFoundError creates a not found error for a resource.
func NotFoundError(resourceType string, identifiers ...string) error {
	return fmt.Errorf("%s not found: %v", resourceType, identifiers)
}

// renderJSON marshals a value to JSON and writes it to stdout.
func renderJSON(_ ResourceContext, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}
	_, err = fmt.Fprintln(os.Stdout, string(data))
	return err
}

// withInDesiredState merges the _inDesiredState canonical property into a state object.
// DSC v3 expects the test output to include _inDesiredState in the actual state JSON.
func withInDesiredState(state any, inDesiredState bool) (map[string]any, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m["_inDesiredState"] = inDesiredState
	return m, nil
}

// CompareStates compares two states (as JSON-serializable structs) and returns
// the list of property names that differ. This is used by the Test operation
// to report differing properties.
func CompareStates(desired, actual any) []string {
	desiredJSON, err := json.Marshal(desired)
	if err != nil {
		return nil
	}
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		return nil
	}

	var desiredMap map[string]json.RawMessage
	var actualMap map[string]json.RawMessage

	if err := json.Unmarshal(desiredJSON, &desiredMap); err != nil {
		return nil
	}
	if err := json.Unmarshal(actualJSON, &actualMap); err != nil {
		return nil
	}

	var differing []string
	for key, desiredVal := range desiredMap {
		actualVal, ok := actualMap[key]
		if !ok {
			differing = append(differing, key)
			continue
		}
		if string(desiredVal) != string(actualVal) {
			differing = append(differing, key)
		}
	}

	return differing
}
