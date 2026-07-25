package resources

import (
	"testing"

	"github.com/databricks/databricks-sdk-go/service/sql"
)

func TestPrincipalKey(t *testing.T) {
	tests := []struct {
		name      string
		state     SqlWarehousePermissionState
		wantType  string
		wantValue string
	}{
		{
			name:      "user",
			state:     SqlWarehousePermissionState{UserName: "u@example.com"},
			wantType:  "user_name",
			wantValue: "u@example.com",
		},
		{
			name:      "group",
			state:     SqlWarehousePermissionState{GroupName: "admins"},
			wantType:  "group_name",
			wantValue: "admins",
		},
		{
			name:      "service principal",
			state:     SqlWarehousePermissionState{ServicePrincipalName: "sp-app-id"},
			wantType:  "service_principal_name",
			wantValue: "sp-app-id",
		},
		{
			name:      "user wins over group",
			state:     SqlWarehousePermissionState{UserName: "u", GroupName: "g"},
			wantType:  "user_name",
			wantValue: "u",
		},
		{
			name:      "none",
			state:     SqlWarehousePermissionState{},
			wantType:  "",
			wantValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotValue := tt.state.principalKey()
			if gotType != tt.wantType || gotValue != tt.wantValue {
				t.Errorf("principalKey() = (%q, %q), want (%q, %q)",
					gotType, gotValue, tt.wantType, tt.wantValue)
			}
		})
	}
}

func TestMatchesWarehousePrincipal(t *testing.T) {
	tests := []struct {
		name  string
		entry sql.WarehouseAccessControlResponse
		req   SqlWarehousePermissionState
		want  bool
	}{
		{
			name:  "user match",
			entry: sql.WarehouseAccessControlResponse{UserName: "u@example.com"},
			req:   SqlWarehousePermissionState{UserName: "u@example.com"},
			want:  true,
		},
		{
			name:  "user mismatch",
			entry: sql.WarehouseAccessControlResponse{UserName: "other@example.com"},
			req:   SqlWarehousePermissionState{UserName: "u@example.com"},
			want:  false,
		},
		{
			name:  "group match",
			entry: sql.WarehouseAccessControlResponse{GroupName: "admins"},
			req:   SqlWarehousePermissionState{GroupName: "admins"},
			want:  true,
		},
		{
			name:  "service principal match",
			entry: sql.WarehouseAccessControlResponse{ServicePrincipalName: "sp"},
			req:   SqlWarehousePermissionState{ServicePrincipalName: "sp"},
			want:  true,
		},
		{
			name:  "no principal in request",
			entry: sql.WarehouseAccessControlResponse{UserName: "u@example.com"},
			req:   SqlWarehousePermissionState{},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesWarehousePrincipal(tt.entry, &tt.req); got != tt.want {
				t.Errorf("matchesWarehousePrincipal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectWarehousePermissionLevel(t *testing.T) {
	tests := []struct {
		name  string
		entry sql.WarehouseAccessControlResponse
		want  string
	}{
		{
			name: "direct permission",
			entry: sql.WarehouseAccessControlResponse{
				AllPermissions: []sql.WarehousePermission{
					{PermissionLevel: sql.WarehousePermissionLevelCanUse, Inherited: false},
				},
			},
			want: "CAN_USE",
		},
		{
			name: "inherited only",
			entry: sql.WarehouseAccessControlResponse{
				AllPermissions: []sql.WarehousePermission{
					{PermissionLevel: sql.WarehousePermissionLevelCanManage, Inherited: true},
				},
			},
			want: "",
		},
		{
			name: "direct among inherited",
			entry: sql.WarehouseAccessControlResponse{
				AllPermissions: []sql.WarehousePermission{
					{PermissionLevel: sql.WarehousePermissionLevelCanManage, Inherited: true},
					{PermissionLevel: sql.WarehousePermissionLevelCanMonitor, Inherited: false},
				},
			},
			want: "CAN_MONITOR",
		},
		{
			name:  "no permissions",
			entry: sql.WarehouseAccessControlResponse{},
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := directWarehousePermissionLevel(tt.entry); got != tt.want {
				t.Errorf("directWarehousePermissionLevel() = %q, want %q", got, tt.want)
			}
		})
	}
}
