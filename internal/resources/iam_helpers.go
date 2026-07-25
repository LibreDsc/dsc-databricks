package resources

import (
	"github.com/databricks/databricks-sdk-go/service/iam"
)

// UserComplexValue represents a SCIM complex value (used for emails, entitlements, roles).
type UserComplexValue struct {
	Value   string `json:"value" description:"The value of the entry."`
	Display string `json:"display,omitempty" description:"The display name of the entry."`
	Type    string `json:"type,omitempty" description:"The type of the entry."`
	Primary bool   `json:"primary,omitempty" description:"Indicates if this is the primary entry."`
}

// toIamComplexValues converts UserComplexValue slices to iam.ComplexValue slices.
func toIamComplexValues(vals []UserComplexValue) []iam.ComplexValue {
	if len(vals) == 0 {
		return nil
	}
	out := make([]iam.ComplexValue, len(vals))
	for i, v := range vals {
		out[i] = iam.ComplexValue{
			Value:   v.Value,
			Display: v.Display,
			Type:    v.Type,
			Primary: v.Primary,
		}
	}
	return out
}

// fromIamComplexValues converts iam.ComplexValue slices to UserComplexValue slices.
func fromIamComplexValues(vals []iam.ComplexValue) []UserComplexValue {
	if len(vals) == 0 {
		return nil
	}
	out := make([]UserComplexValue, len(vals))
	for i, v := range vals {
		out[i] = UserComplexValue{
			Value:   v.Value,
			Display: v.Display,
			Type:    v.Type,
			Primary: v.Primary,
		}
	}
	return out
}

// setNestedEnum sets an enum constraint on a value property inside an array-of-objects schema property.
func setNestedEnum(props map[string]any, arrayProp, fieldName string, enumValues []string) {
	arraySchema, ok := props[arrayProp].(map[string]any)
	if !ok {
		return
	}
	items, ok := arraySchema["items"].(map[string]any)
	if !ok {
		return
	}
	itemProps, ok := items["properties"].(map[string]any)
	if !ok {
		return
	}
	fieldSchema, ok := itemProps[fieldName].(map[string]any)
	if !ok {
		return
	}
	fieldSchema["enum"] = enumValues
}
