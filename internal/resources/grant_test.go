package resources

import (
	"slices"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
)

func TestComputeGrantChanges(t *testing.T) {
	tests := []struct {
		name       string
		desired    []string
		current    []string
		wantAdd    []string
		wantRemove []string
	}{
		{
			name:    "no changes when sets match",
			desired: []string{"SELECT", "USE_CATALOG"},
			current: []string{"SELECT", "USE_CATALOG"},
		},
		{
			name:    "all added for new principal",
			desired: []string{"USE_CATALOG"},
			wantAdd: []string{"USE_CATALOG"},
		},
		{
			name:       "adds and removes computed as set difference",
			desired:    []string{"USE_CATALOG", "CREATE_SCHEMA"},
			current:    []string{"USE_CATALOG", "SELECT"},
			wantAdd:    []string{"CREATE_SCHEMA"},
			wantRemove: []string{"SELECT"},
		},
		{
			name:       "all removed when desired empty",
			current:    []string{"SELECT", "MODIFY"},
			wantRemove: []string{"SELECT", "MODIFY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			add, remove := computeGrantChanges(tt.desired, tt.current)
			toStrings := func(ps []catalog.Privilege) []string {
				var out []string
				for _, p := range ps {
					out = append(out, string(p))
				}
				return out
			}
			if got := toStrings(add); !slices.Equal(got, tt.wantAdd) {
				t.Errorf("add = %v, want %v", got, tt.wantAdd)
			}
			if got := toStrings(remove); !slices.Equal(got, tt.wantRemove) {
				t.Errorf("remove = %v, want %v", got, tt.wantRemove)
			}
		})
	}
}

func TestSortedGrantState(t *testing.T) {
	in := GrantState{
		SecurableType: "CATALOG",
		FullName:      "main",
		Principal:     "user@example.com",
		Privileges:    []string{"USE_CATALOG", "CREATE_SCHEMA", "BROWSE"},
	}

	out := sortedGrantState(&in)

	if out.SecurableType != "catalog" {
		t.Errorf("SecurableType = %q, want lowercase normalization", out.SecurableType)
	}
	if !slices.Equal(out.Privileges, []string{"BROWSE", "CREATE_SCHEMA", "USE_CATALOG"}) {
		t.Errorf("Privileges = %v, want sorted", out.Privileges)
	}
	if !slices.Equal(in.Privileges, []string{"USE_CATALOG", "CREATE_SCHEMA", "BROWSE"}) {
		t.Errorf("input privileges mutated: %v", in.Privileges)
	}
}

func TestProjectGrantSet(t *testing.T) {
	desired := GrantState{
		SecurableType: "Catalog",
		FullName:      "main",
		Principal:     "user@example.com",
		Privileges:    []string{"USE_CATALOG", "BROWSE"},
	}

	projected := projectGrantSet(&desired)

	if projected.SecurableType != "catalog" {
		t.Errorf("SecurableType = %q, want catalog", projected.SecurableType)
	}
	if !slices.Equal(projected.Privileges, []string{"BROWSE", "USE_CATALOG"}) {
		t.Errorf("Privileges = %v, want sorted desired set", projected.Privileges)
	}
	assertExistTrue(t, projected.Exist)
}
