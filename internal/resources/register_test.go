package resources

import (
	"encoding/json"
	"slices"
	"testing"

	dsc "github.com/LibreDsc/dsc-go-rdk"
)

// testableResources are the resource types that implement a custom Testable;
// every other resource relies on the DSC engine's synthetic test and must NOT
// advertise test in its manifest.
var testableResources = []string{
	"LibreDsc.Databricks/User",
	"LibreDsc.Databricks/AccountUser",
	"LibreDsc.Databricks/Group",
	"LibreDsc.Databricks/ServicePrincipal",
	"LibreDsc.Databricks/Catalog",
	"LibreDsc.Databricks/SecretScope",
	"LibreDsc.Databricks/SecretAcl",
}

// expectedRequired pins the schema required lists so tag changes that would
// alter the public schema contract fail loudly.
var expectedRequired = map[string][]string{
	"LibreDsc.Databricks/User":                   {"user_name"},
	"LibreDsc.Databricks/AccountUser":            {"user_name"},
	"LibreDsc.Databricks/Group":                  {"display_name"},
	"LibreDsc.Databricks/ServicePrincipal":       {"display_name"},
	"LibreDsc.Databricks/Catalog":                {"name"},
	"LibreDsc.Databricks/Cluster":                {},
	"LibreDsc.Databricks/ClusterPolicy":          {},
	"LibreDsc.Databricks/Repo":                   {"path"},
	"LibreDsc.Databricks/Secret":                 {"key", "scope"},
	"LibreDsc.Databricks/SecretScope":            {"scope"},
	"LibreDsc.Databricks/SecretAcl":              {"principal", "scope"},
	"LibreDsc.Databricks/SqlWarehouse":           {},
	"LibreDsc.Databricks/SqlWarehousePermission": {},
	"LibreDsc.Databricks/WorkspaceConf":          {"key"},
	"LibreDsc.Databricks/WorkspaceSetting":       {"setting_name"},
}

// schemaDoc is the subset of the generated JSON Schema the tests assert on.
type schemaDoc struct {
	Properties           map[string]json.RawMessage `json:"properties"`
	Required             []string                   `json:"required"`
	AdditionalProperties *bool                      `json:"additionalProperties"`
}

func newTestManager(t *testing.T) *dsc.Manager {
	t.Helper()
	m := dsc.NewManager("dsc-databricks")
	RegisterAll(m)
	return m
}

func TestRegisterAllResourceTypes(t *testing.T) {
	m := newTestManager(t)

	types := m.ResourceTypes()
	if len(types) != len(expectedRequired) {
		t.Errorf("registered %d resource types, want %d: %v", len(types), len(expectedRequired), types)
	}
	for resourceType := range expectedRequired {
		if !slices.Contains(types, resourceType) {
			t.Errorf("resource type %s not registered", resourceType)
		}
	}
}

func TestManifestCapabilities(t *testing.T) {
	m := newTestManager(t)

	for _, manifest := range m.Manifests() {
		t.Run(manifest.Type, func(t *testing.T) {
			if manifest.Version != "0.1.0" {
				t.Errorf("version = %q, want 0.1.0", manifest.Version)
			}
			if manifest.Get == nil {
				t.Errorf("get method missing")
			}
			if manifest.Set == nil {
				t.Fatalf("set method missing")
			}
			if manifest.Set.Return != "stateAndDiff" {
				t.Errorf("set.return = %q, want stateAndDiff", manifest.Set.Return)
			}
			if manifest.Delete == nil {
				t.Errorf("delete method missing")
			}
			if manifest.Export == nil {
				t.Errorf("export method missing")
			}

			wantTest := slices.Contains(testableResources, manifest.Type)
			if wantTest && manifest.Test == nil {
				t.Errorf("test method missing but resource implements Testable")
			}
			if !wantTest && manifest.Test != nil {
				t.Errorf("test method advertised but resource relies on synthetic test")
			}
			if manifest.Test != nil && manifest.Test.Return != "state" {
				t.Errorf("test.return = %q, want state", manifest.Test.Return)
			}
		})
	}
}

func TestManifestSchemas(t *testing.T) {
	m := newTestManager(t)

	for _, manifest := range m.Manifests() {
		t.Run(manifest.Type, func(t *testing.T) {
			if manifest.Schema == nil {
				t.Fatalf("schema missing")
			}
			raw, err := json.Marshal(manifest.Schema.Embedded)
			if err != nil {
				t.Fatalf("failed to marshal schema: %v", err)
			}
			var doc schemaDoc
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("failed to unmarshal schema: %v", err)
			}

			// _exist must be a boolean property defaulting to true.
			existRaw, ok := doc.Properties["_exist"]
			if !ok {
				t.Fatalf("_exist property missing")
			}
			var exist struct {
				Type    string `json:"type"`
				Default *bool  `json:"default"`
			}
			if err := json.Unmarshal(existRaw, &exist); err != nil {
				t.Fatalf("failed to unmarshal _exist: %v", err)
			}
			if exist.Type != "boolean" || exist.Default == nil || !*exist.Default {
				t.Errorf("_exist = %s, want boolean with default true", existRaw)
			}

			// Schemas stay permissive: additionalProperties:false must not appear.
			if doc.AdditionalProperties != nil && !*doc.AdditionalProperties {
				t.Errorf("additionalProperties:false must not be emitted")
			}

			want, ok := expectedRequired[manifest.Type]
			if !ok {
				t.Fatalf("no expected required list for %s — add it to expectedRequired", manifest.Type)
			}
			got := slices.Clone(doc.Required)
			slices.Sort(got)
			wantSorted := slices.Clone(want)
			slices.Sort(wantSorted)
			if !slices.Equal(got, wantSorted) {
				t.Errorf("required = %v, want %v", got, wantSorted)
			}
		})
	}
}

func TestUserSchemaEntitlementsEnum(t *testing.T) {
	m := newTestManager(t)

	for _, manifest := range m.Manifests() {
		if manifest.Type != "LibreDsc.Databricks/User" {
			continue
		}
		raw, err := json.Marshal(manifest.Schema.Embedded)
		if err != nil {
			t.Fatalf("failed to marshal schema: %v", err)
		}
		var doc struct {
			Properties struct {
				Entitlements struct {
					Items struct {
						Properties struct {
							Value struct {
								Enum []string `json:"enum"`
							} `json:"value"`
						} `json:"properties"`
					} `json:"items"`
				} `json:"entitlements"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("failed to unmarshal schema: %v", err)
		}
		enum := doc.Properties.Entitlements.Items.Properties.Value.Enum
		if !slices.Contains(enum, "workspace-access") || !slices.Contains(enum, "databricks-sql-access") {
			t.Errorf("entitlements value enum = %v, want workspace entitlement values", enum)
		}
		return
	}
	t.Fatalf("User manifest not found")
}
