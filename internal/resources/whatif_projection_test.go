package resources

import (
	"testing"
)

func assertExistTrue(t *testing.T, exist *bool) {
	t.Helper()
	if exist == nil || !*exist {
		t.Errorf("_exist must be explicitly true on projected states")
	}
}

func TestProjectClusterCreate(t *testing.T) {
	desired := ClusterState{
		ClusterName:  "c",
		SparkVersion: "15.4.x-scala2.12",
		NumWorkers:   2,
	}

	projected := projectClusterCreate(&desired)

	if projected.ClusterID != "" || projected.State != "" || projected.StateMessage != "" {
		t.Errorf("server-computed fields must stay empty on create: %+v", projected)
	}
	if projected.ClusterName != "c" || projected.NumWorkers != 2 {
		t.Errorf("desired fields not carried: %+v", projected)
	}
	assertExistTrue(t, projected.Exist)
	if desired.Exist != nil {
		t.Errorf("SetWhatIf projection must not mutate desired")
	}
}

func TestProjectClusterUpdate(t *testing.T) {
	current := ClusterState{
		ClusterID:              "id-1",
		ClusterName:            "old-name",
		SparkVersion:           "14.3.x-scala2.12",
		NodeTypeID:             "Standard_D4ds_v5",
		State:                  "RUNNING",
		StateMessage:           "up",
		NumWorkers:             2,
		AutoterminationMinutes: 10,
	}

	t.Run("empty desired fields keep current values", func(t *testing.T) {
		desired := ClusterState{ClusterID: "id-1", NumWorkers: 2}
		projected := projectClusterUpdate(&desired, &current)
		if projected.ClusterName != "old-name" || projected.SparkVersion != "14.3.x-scala2.12" {
			t.Errorf("fallback to current failed: %+v", projected)
		}
		if projected.State != "RUNNING" || projected.StateMessage != "up" {
			t.Errorf("read-only fields must carry over: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("non-zero desired fields win", func(t *testing.T) {
		desired := ClusterState{
			ClusterID:              "id-1",
			ClusterName:            "new-name",
			AutoterminationMinutes: 30,
			NumWorkers:             4,
		}
		projected := projectClusterUpdate(&desired, &current)
		if projected.ClusterName != "new-name" || projected.AutoterminationMinutes != 30 || projected.NumWorkers != 4 {
			t.Errorf("desired overlay failed: %+v", projected)
		}
	})

	t.Run("num_workers zero is force-applied without autoscale", func(t *testing.T) {
		desired := ClusterState{ClusterID: "id-1", NumWorkers: 0}
		projected := projectClusterUpdate(&desired, &current)
		if projected.NumWorkers != 0 {
			t.Errorf("NumWorkers = %d, want 0 (force-sent by buildEditRequest)", projected.NumWorkers)
		}
	})

	t.Run("autoscale keeps current num_workers", func(t *testing.T) {
		desired := ClusterState{ClusterID: "id-1", AutoscaleMinWorkers: 1, AutoscaleMaxWorkers: 4}
		projected := projectClusterUpdate(&desired, &current)
		if projected.AutoscaleMinWorkers != 1 || projected.AutoscaleMaxWorkers != 4 {
			t.Errorf("autoscale not applied: %+v", projected)
		}
		if projected.NumWorkers != current.NumWorkers {
			t.Errorf("NumWorkers must stay server-managed under autoscale")
		}
	})

	t.Run("next-gen compute fields overlay when set", func(t *testing.T) {
		desired := ClusterState{
			ClusterID:         "id-1",
			NumWorkers:        2,
			Kind:              "CLASSIC_PREVIEW",
			AzureAvailability: "SPOT_WITH_FALLBACK_AZURE",
			IsSingleNode:      true,
		}
		projected := projectClusterUpdate(&desired, &current)
		if projected.Kind != "CLASSIC_PREVIEW" || projected.AzureAvailability != "SPOT_WITH_FALLBACK_AZURE" || !projected.IsSingleNode {
			t.Errorf("next-gen compute overlay failed: %+v", projected)
		}
	})
}

func TestProjectSqlWarehouseCreate(t *testing.T) {
	desired := SqlWarehouseState{Name: "wh", ClusterSize: "2X-Small", NumClusters: 3, State: "RUNNING"}

	projected := projectSqlWarehouseCreate(&desired)

	if projected.ID != "" || projected.State != "" || projected.NumClusters != 0 {
		t.Errorf("server-computed fields must stay empty on create: %+v", projected)
	}
	if projected.Name != "wh" || projected.ClusterSize != "2X-Small" {
		t.Errorf("desired fields not carried: %+v", projected)
	}
	assertExistTrue(t, projected.Exist)
}

func TestProjectSqlWarehouseUpdate(t *testing.T) {
	current := SqlWarehouseState{
		ID:           "wh-1",
		Name:         "wh",
		ClusterSize:  "Small",
		State:        "STOPPED",
		NumClusters:  1,
		AutoStopMins: 120,
	}

	t.Run("auto_stop_mins always comes from desired", func(t *testing.T) {
		desired := SqlWarehouseState{ID: "wh-1", AutoStopMins: 0}
		projected := projectSqlWarehouseUpdate(&desired, &current)
		if projected.AutoStopMins != 0 {
			t.Errorf("AutoStopMins = %d, want 0 (force-sent by buildEditWarehouseRequest)", projected.AutoStopMins)
		}
		if projected.State != "STOPPED" || projected.NumClusters != 1 || projected.ID != "wh-1" {
			t.Errorf("read-only fields must carry over: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("non-empty desired fields win", func(t *testing.T) {
		desired := SqlWarehouseState{ID: "wh-1", ClusterSize: "Medium", AutoStopMins: 30}
		projected := projectSqlWarehouseUpdate(&desired, &current)
		if projected.ClusterSize != "Medium" || projected.AutoStopMins != 30 {
			t.Errorf("desired overlay failed: %+v", projected)
		}
		if projected.Name != "wh" {
			t.Errorf("empty desired name must keep current")
		}
	})
}

func TestProjectCatalog(t *testing.T) {
	current := CatalogState{
		Name:            "cat",
		Comment:         "old",
		Owner:           "owner@example.com",
		CatalogType:     "MANAGED_CATALOG",
		MetastoreID:     "ms-1",
		StorageLocation: "abfss://loc",
	}

	t.Run("update overlays mutable fields and keeps computed ones", func(t *testing.T) {
		desired := CatalogState{Name: "cat", Comment: "new"}
		projected := projectCatalogUpdate(&desired, &current)
		if projected.Comment != "new" {
			t.Errorf("Comment = %q, want new", projected.Comment)
		}
		if projected.Owner != "owner@example.com" || projected.CatalogType != "MANAGED_CATALOG" ||
			projected.MetastoreID != "ms-1" || projected.StorageLocation != "abfss://loc" {
			t.Errorf("computed/immutable fields must carry over: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("create leaves computed fields empty", func(t *testing.T) {
		desired := CatalogState{Name: "cat", Comment: "c", Owner: "ignored"}
		projected := projectCatalogCreate(&desired)
		if projected.Owner != "" || projected.CatalogType != "" || projected.MetastoreID != "" {
			t.Errorf("computed fields must stay empty on create: %+v", projected)
		}
		if projected.Name != "cat" || projected.Comment != "c" {
			t.Errorf("create fields not carried: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})
}

func TestProjectClusterPolicy(t *testing.T) {
	current := ClusterPolicyState{
		PolicyID:   "pol-1",
		Name:       "old",
		Definition: `{"a":1}`,
	}

	t.Run("update keeps policy_id and overlays non-empty fields", func(t *testing.T) {
		desired := ClusterPolicyState{Name: "new", Description: "d"}
		projected := projectClusterPolicyUpdate(&desired, &current, "new")
		if projected.PolicyID != "pol-1" {
			t.Errorf("PolicyID must carry over")
		}
		if projected.Name != "new" || projected.Description != "d" {
			t.Errorf("overlay failed: %+v", projected)
		}
		if projected.Definition != `{"a":1}` {
			t.Errorf("empty desired definition must keep current")
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("create leaves policy_id empty and applies effective name", func(t *testing.T) {
		desired := ClusterPolicyState{PolicyID: "should-clear", Definition: `{"b":2}`}
		projected := projectClusterPolicyCreate(&desired, "eff-name")
		if projected.PolicyID != "" {
			t.Errorf("PolicyID must stay empty on create")
		}
		if projected.Name != "eff-name" || projected.Definition != `{"b":2}` {
			t.Errorf("create fields wrong: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})
}

func TestProjectRepo(t *testing.T) {
	current := RepoState{
		Path:         "/Repos/x/y",
		URL:          "https://example.com/r.git",
		Provider:     "gitHub",
		Branch:       "main",
		HeadCommitID: "abc",
		ID:           42,
	}

	t.Run("update only changes a requested different branch", func(t *testing.T) {
		desired := RepoState{Path: "/Repos/x/y", Branch: "dev"}
		projected := projectRepoUpdate(&desired, &current)
		if projected.Branch != "dev" {
			t.Errorf("Branch = %q, want dev", projected.Branch)
		}
		if projected.ID != 42 || projected.HeadCommitID != "abc" {
			t.Errorf("computed fields must carry over: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("update with same or empty branch keeps current", func(t *testing.T) {
		desired := RepoState{Path: "/Repos/x/y"}
		projected := projectRepoUpdate(&desired, &current)
		if projected.Branch != "main" {
			t.Errorf("Branch = %q, want main", projected.Branch)
		}
	})

	t.Run("create leaves computed fields zero", func(t *testing.T) {
		desired := RepoState{Path: "/Repos/x/y", URL: "u", Provider: "gitHub", Branch: "dev"}
		projected := projectRepoCreate(&desired)
		if projected.ID != 0 || projected.HeadCommitID != "" {
			t.Errorf("computed fields must stay zero on create: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})
}

func TestProjectWorkspaceSettingUpdate(t *testing.T) {
	current := WorkspaceSettingState{SettingName: "default_namespace", Value: "old", Etag: "etag-1"}
	desired := WorkspaceSettingState{SettingName: "default_namespace", Value: "new"}

	projected := projectWorkspaceSettingUpdate(&desired, &current)

	if projected.Value != "new" {
		t.Errorf("Value = %q, want new", projected.Value)
	}
	if projected.Etag != "etag-1" {
		t.Errorf("Etag must carry over from current (post-update etag unpredictable)")
	}
	assertExistTrue(t, projected.Exist)
}

func TestProjectScimUpdates(t *testing.T) {
	t.Run("user overlay", func(t *testing.T) {
		current := UserState{ID: "1", UserName: "u@example.com", DisplayName: "Old", Active: true}
		desired := UserState{UserName: "u@example.com", DisplayName: "New", Active: false}
		projected := projectUserUpdate(&desired, &current)
		if projected.ID != "1" {
			t.Errorf("ID must carry over")
		}
		if projected.DisplayName != "New" {
			t.Errorf("DisplayName overlay failed")
		}
		if projected.Active {
			t.Errorf("Active must always come from desired (force-sent)")
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("user overlay keeps current slices when desired empty", func(t *testing.T) {
		current := UserState{ID: "1", UserName: "u@example.com", Entitlements: []UserComplexValue{{Value: "workspace-access"}}}
		desired := UserState{UserName: "u@example.com", Active: true}
		projected := projectUserUpdate(&desired, &current)
		if len(projected.Entitlements) != 1 {
			t.Errorf("empty desired slices must keep current")
		}
	})

	t.Run("account user overlay", func(t *testing.T) {
		current := AccountUserState{ID: "2", UserName: "a@example.com", DisplayName: "Old", Active: false}
		desired := AccountUserState{UserName: "a@example.com", Active: true}
		projected := projectAccountUserUpdate(&desired, &current)
		if projected.ID != "2" || projected.DisplayName != "Old" {
			t.Errorf("carry-over failed: %+v", projected)
		}
		if !projected.Active {
			t.Errorf("Active must come from desired")
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("group overlay", func(t *testing.T) {
		current := GroupState{ID: "3", DisplayName: "grp", Members: []UserComplexValue{{Value: "m1"}}}
		desired := GroupState{DisplayName: "grp", ExternalID: "ext", Members: []UserComplexValue{{Value: "m2"}, {Value: "m3"}}}
		projected := projectGroupUpdate(&desired, &current)
		if projected.ID != "3" || projected.ExternalID != "ext" || len(projected.Members) != 2 {
			t.Errorf("group overlay failed: %+v", projected)
		}
		assertExistTrue(t, projected.Exist)
	})

	t.Run("service principal overlay", func(t *testing.T) {
		current := ServicePrincipalState{ID: "4", DisplayName: "sp", ApplicationID: "app-1", Active: true}
		desired := ServicePrincipalState{DisplayName: "sp", Active: false}
		projected := projectServicePrincipalUpdate(&desired, &current)
		if projected.ID != "4" || projected.ApplicationID != "app-1" {
			t.Errorf("carry-over failed: %+v", projected)
		}
		if projected.Active {
			t.Errorf("Active must come from desired")
		}
		assertExistTrue(t, projected.Exist)
	})
}
