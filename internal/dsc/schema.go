package dsc

import (
	"reflect"
	"strings"
)

// SchemaOptions contains options for schema generation.
type SchemaOptions struct {
	Descriptions      PropertyDescriptions
	SchemaDescription string
	ResourceName      string
}

// GenerateSchema generates a JSON schema from a reflect.Type.
func GenerateSchema(t reflect.Type) (any, error) {
	return GenerateSchemaWithOptions(t, SchemaOptions{})
}

// GenerateSchemaWithDescriptions generates a JSON schema with custom descriptions.
func GenerateSchemaWithDescriptions(t reflect.Type, descriptions PropertyDescriptions) (any, error) {
	return GenerateSchemaWithOptions(t, SchemaOptions{Descriptions: descriptions})
}

// GenerateSchemaWithOptions generates a JSON schema with the specified options.
func GenerateSchemaWithOptions(t reflect.Type, opts SchemaOptions) (any, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	result := generateSchemaForType(t, opts.Descriptions)
	result["$schema"] = "https://json-schema.org/draft/2020-12/schema"

	// Add top-level description if provided
	if opts.SchemaDescription != "" {
		result["description"] = opts.SchemaDescription
	}

	// Add _exist property to all schemas
	existDesc := "Indicates whether the resource should exist."
	if opts.ResourceName != "" {
		existDesc = "Indicates whether the " + opts.ResourceName + " should exist."
	}
	props, ok := result["properties"].(map[string]any)
	if !ok {
		props = make(map[string]any)
		result["properties"] = props
	}
	props["_exist"] = map[string]any{
		"type":        "boolean",
		"description": existDesc,
		"default":     true,
	}

	return result, nil
}

// GenerateSchemaFromType generates a JSON schema from a value's type.
func GenerateSchemaFromType(v any) (any, error) {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return GenerateSchema(t)
}

func generateSchemaForType(t reflect.Type, descriptions PropertyDescriptions) map[string]any {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	schema := make(map[string]any)

	switch t.Kind() {
	case reflect.Struct:
		schema["type"] = "object"
		props := make(map[string]any)
		var required []string

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}

			jsonTag := field.Tag.Get("json")
			if jsonTag == "-" {
				continue
			}

			fieldName := getJSONFieldName(field, jsonTag)
			propSchema := generateSchemaForType(field.Type, nil)

			// Apply custom description if provided
			if descriptions != nil {
				if desc, ok := descriptions[fieldName]; ok {
					propSchema["description"] = desc
				}
			}

			props[fieldName] = propSchema

			// Check if field is required (no omitempty in json tag)
			if !strings.Contains(jsonTag, "omitempty") && jsonTag != "" {
				required = append(required, fieldName)
			}
		}

		if len(props) > 0 {
			schema["properties"] = props
		}
		if len(required) > 0 {
			schema["required"] = required
		}

	case reflect.Slice, reflect.Array:
		schema["type"] = "array"
		schema["items"] = generateSchemaForType(t.Elem(), nil)

	case reflect.Map:
		schema["type"] = "object"
		schema["additionalProperties"] = generateSchemaForType(t.Elem(), nil)

	case reflect.String:
		schema["type"] = "string"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		schema["type"] = "integer"

	case reflect.Float32, reflect.Float64:
		schema["type"] = "number"

	case reflect.Bool:
		schema["type"] = "boolean"

	case reflect.Interface:
		// For interface{}/any, allow any type
		// Don't set a type constraint

	default:
		schema["type"] = "string"
	}

	return schema
}

func getJSONFieldName(field reflect.StructField, jsonTag string) string {
	if jsonTag == "" {
		return field.Name
	}

	parts := strings.Split(jsonTag, ",")
	if parts[0] != "" {
		return parts[0]
	}
	return field.Name
}
