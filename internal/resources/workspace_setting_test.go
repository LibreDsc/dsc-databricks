package resources

import (
	"encoding/json"
	"testing"
)

// TestSettingDefaultState guards the never-written-setting contract: the
// setting exists at its server-side default, so the state must serialize
// with _exist true and an explicit empty value (the Pester suite asserts
// value is not null), while etag stays omitted.
func TestSettingDefaultState(t *testing.T) {
	state := settingDefaultState("default_namespace")

	if state.SettingName != "default_namespace" {
		t.Errorf("SettingName = %q, want %q", state.SettingName, "default_namespace")
	}
	if !state.ShouldExist() {
		t.Error("ShouldExist() = false, want true")
	}

	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if exist, ok := m["_exist"]; !ok || exist != true {
		t.Errorf("_exist = %v (present=%v), want true", exist, ok)
	}
	if value, ok := m["value"]; !ok || value != "" {
		t.Errorf("value = %v (present=%v), want explicit empty string", value, ok)
	}
	if _, ok := m["etag"]; ok {
		t.Error("etag should be omitted for a never-written setting")
	}
}
