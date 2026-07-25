package resources

import (
	"reflect"
	"slices"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/iam"
)

func TestComplexValueRoundTrip(t *testing.T) {
	in := []UserComplexValue{
		{Value: "workspace-access", Display: "Workspace", Type: "entitlement", Primary: true},
		{Value: "databricks-sql-access"},
	}

	converted := toIamComplexValues(in)
	if len(converted) != len(in) {
		t.Fatalf("toIamComplexValues length = %d, want %d", len(converted), len(in))
	}

	back := fromIamComplexValues(converted)
	if !reflect.DeepEqual(back, in) {
		t.Errorf("round trip mismatch: got %+v, want %+v", back, in)
	}
}

func TestComplexValueEmptyToNil(t *testing.T) {
	if got := toIamComplexValues(nil); got != nil {
		t.Errorf("toIamComplexValues(nil) = %v, want nil", got)
	}
	if got := toIamComplexValues([]UserComplexValue{}); got != nil {
		t.Errorf("toIamComplexValues(empty) = %v, want nil", got)
	}
	if got := fromIamComplexValues(nil); got != nil {
		t.Errorf("fromIamComplexValues(nil) = %v, want nil", got)
	}
	if got := fromIamComplexValues([]iam.ComplexValue{}); got != nil {
		t.Errorf("fromIamComplexValues(empty) = %v, want nil", got)
	}
}

func TestUserToUpdateRequest(t *testing.T) {
	user := &iam.User{
		Id:          "123",
		UserName:    "user@example.com",
		DisplayName: "Test User",
		Active:      false,
		ExternalId:  "ext-1",
		Schemas:     []iam.UserSchema{"urn:ietf:params:scim:schemas:core:2.0:User"},
		Emails:      []iam.ComplexValue{{Value: "user@example.com", Primary: true}},
	}

	req := userToUpdateRequest(user)

	if req.Id != user.Id || req.UserName != user.UserName || req.DisplayName != user.DisplayName {
		t.Errorf("identity fields not copied: %+v", req)
	}
	if req.ExternalId != user.ExternalId {
		t.Errorf("ExternalId = %q, want %q", req.ExternalId, user.ExternalId)
	}
	if !reflect.DeepEqual(req.Emails, user.Emails) {
		t.Errorf("Emails not copied")
	}
	// Active=false must survive JSON serialization on the SCIM PUT.
	if !slices.Contains(req.ForceSendFields, "Active") {
		t.Errorf("ForceSendFields = %v, want to contain Active", req.ForceSendFields)
	}
}

func TestAccountUserToUpdateRequest(t *testing.T) {
	user := &iam.AccountUser{
		Id:          "456",
		UserName:    "acct@example.com",
		DisplayName: "Account User",
		Active:      false,
	}

	req := accountUserToUpdateRequest(user)

	if req.Id != user.Id || req.UserName != user.UserName || req.DisplayName != user.DisplayName {
		t.Errorf("identity fields not copied: %+v", req)
	}
	if !slices.Contains(req.ForceSendFields, "Active") {
		t.Errorf("ForceSendFields = %v, want to contain Active", req.ForceSendFields)
	}
}

func TestSetNestedEnum(t *testing.T) {
	props := map[string]any{
		"entitlements": map[string]any{
			"items": map[string]any{
				"properties": map[string]any{
					"value": map[string]any{"type": "string"},
				},
			},
		},
	}

	setNestedEnum(props, "entitlements", "value", []string{"a", "b"})

	value := props["entitlements"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)["value"].(map[string]any)
	enum, ok := value["enum"].([]string)
	if !ok || !slices.Equal(enum, []string{"a", "b"}) {
		t.Errorf("enum = %v, want [a b]", value["enum"])
	}

	// Missing property paths must be a no-op, not a panic.
	setNestedEnum(props, "missing", "value", []string{"a"})
	setNestedEnum(props, "entitlements", "missing", []string{"a"})
}
