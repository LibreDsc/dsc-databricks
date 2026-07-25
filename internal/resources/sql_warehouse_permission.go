package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

// SqlWarehousePermissionState represents the state of a single permission entry
// on a Databricks SQL warehouse.
type SqlWarehousePermissionState struct {
	dsc.ExistProperty
	WarehouseID          string `json:"warehouse_id,omitempty" description:"The unique identifier of the SQL warehouse."`
	WarehouseName        string `json:"warehouse_name,omitempty" description:"The logical name of the SQL warehouse. Can be used instead of warehouse_id for lookup."`
	UserName             string `json:"user_name,omitempty" description:"The name of the user to which the permission applies."`
	GroupName            string `json:"group_name,omitempty" description:"The name of the group to which the permission applies."`
	ServicePrincipalName string `json:"service_principal_name,omitempty" description:"The application ID of the service principal to which the permission applies."`
	PermissionLevel      string `json:"permission_level,omitempty" description:"The permission level: CAN_MANAGE, CAN_MONITOR, CAN_USE, CAN_VIEW, or IS_OWNER." enum:"CAN_MANAGE,CAN_MONITOR,CAN_USE,CAN_VIEW,IS_OWNER"`
}

func (s *SqlWarehousePermissionState) principalKey() (string, string) {
	if s.UserName != "" {
		return "user_name", s.UserName
	}
	if s.GroupName != "" {
		return "group_name", s.GroupName
	}
	if s.ServicePrincipalName != "" {
		return "service_principal_name", s.ServicePrincipalName
	}
	return "", ""
}

func sqlWarehousePermissionConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/SqlWarehousePermission",
		Version:     "0.1.0",
		Description: "Manage Databricks SQL warehouse permissions.",
		Tags:        []string{"databricks", "sqlwarehouse", "permissions", "sql"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks SQL warehouse permissions.",
			AllowAdditionalProperties: true,
		},
	}
}

// SqlWarehousePermissionHandler handles SqlWarehousePermission resource operations.
type SqlWarehousePermissionHandler struct{}

func matchesWarehousePrincipal(entry sql.WarehouseAccessControlResponse, req *SqlWarehousePermissionState) bool {
	if req.UserName != "" {
		return entry.UserName == req.UserName
	}
	if req.GroupName != "" {
		return entry.GroupName == req.GroupName
	}
	if req.ServicePrincipalName != "" {
		return entry.ServicePrincipalName == req.ServicePrincipalName
	}
	return false
}

func directWarehousePermissionLevel(entry sql.WarehouseAccessControlResponse) string {
	for _, p := range entry.AllPermissions {
		if !p.Inherited {
			return string(p.PermissionLevel)
		}
	}
	return ""
}

func validateWarehousePermissionIdentity(in *SqlWarehousePermissionState) error {
	if err := requireAtLeastOne("warehouse_id or warehouse_name", in.WarehouseID, in.WarehouseName); err != nil {
		return err
	}
	return requireAtLeastOne("user_name, group_name, or service_principal_name",
		in.UserName, in.GroupName, in.ServicePrincipalName)
}

func (h *SqlWarehousePermissionHandler) Get(ctx context.Context, in SqlWarehousePermissionState) (SqlWarehousePermissionState, error) {
	if err := validateWarehousePermissionIdentity(&in); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	warehouseID := in.WarehouseID
	warehouseName := in.WarehouseName

	notFoundState := func() SqlWarehousePermissionState {
		return SqlWarehousePermissionState{
			WarehouseID:          warehouseID,
			WarehouseName:        warehouseName,
			UserName:             in.UserName,
			GroupName:            in.GroupName,
			ServicePrincipalName: in.ServicePrincipalName,
		}
	}

	if warehouseID == "" {
		logDebugf(MsgLookup, "SqlWarehousePermission", "warehouse_name="+warehouseName)
		info, err := w.Warehouses.GetByName(ctx, warehouseName)
		if err != nil {
			logInfof(MsgNotFound, "SqlWarehousePermission", "warehouse_name="+warehouseName)
			return dsc.NotFound(notFoundState(), "SqlWarehousePermission", "warehouse_name="+warehouseName)
		}
		warehouseID = info.Id
		warehouseName = info.Name
	}

	principalType, principalName := in.principalKey()
	logDebugf(MsgLookup, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)

	perms, err := w.Warehouses.GetPermissions(ctx, sql.GetWarehousePermissionsRequest{WarehouseId: warehouseID})
	if err != nil {
		logInfof(MsgNotFound, "SqlWarehousePermission", "warehouse_id="+warehouseID)
		return dsc.NotFound(notFoundState(), "SqlWarehousePermission", "warehouse_id="+warehouseID)
	}

	for _, entry := range perms.AccessControlList {
		if !matchesWarehousePrincipal(entry, &in) {
			continue
		}
		level := directWarehousePermissionLevel(entry)
		if level != "" {
			state := SqlWarehousePermissionState{
				WarehouseID:          warehouseID,
				WarehouseName:        warehouseName,
				UserName:             entry.UserName,
				GroupName:            entry.GroupName,
				ServicePrincipalName: entry.ServicePrincipalName,
				PermissionLevel:      level,
			}
			state.SetExist(true)
			return state, nil
		}
	}

	logInfof(MsgNotFound, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)
	return dsc.NotFound(notFoundState(), "SqlWarehousePermission",
		principalType+"="+principalName, "warehouse_id="+warehouseID)
}

func (h *SqlWarehousePermissionHandler) Set(ctx context.Context, desired SqlWarehousePermissionState) (SqlWarehousePermissionState, error) {
	if err := validateWarehousePermissionIdentity(&desired); err != nil {
		return desired, err
	}
	if err := requireFields(field{"permission_level", desired.PermissionLevel}); err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	// Resolve warehouse ID.
	warehouseID := desired.WarehouseID
	if warehouseID == "" {
		info, err := w.Warehouses.GetByName(ctx, desired.WarehouseName)
		if err != nil {
			return desired, fmt.Errorf("SQL warehouse not found by name=%s: %w", desired.WarehouseName, err)
		}
		warehouseID = info.Id
	}

	principalType, principalName := desired.principalKey()
	logInfof(MsgPut, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID+" permission_level="+desired.PermissionLevel)

	// PATCH (UpdatePermissions) adds or updates the specific entry without
	// disturbing other entries on the warehouse.
	_, err = w.Warehouses.UpdatePermissions(ctx, sql.WarehousePermissionsRequest{
		WarehouseId: warehouseID,
		AccessControlList: []sql.WarehouseAccessControlRequest{{
			UserName:             desired.UserName,
			GroupName:            desired.GroupName,
			ServicePrincipalName: desired.ServicePrincipalName,
			PermissionLevel:      sql.WarehousePermissionLevel(desired.PermissionLevel),
		}},
	})
	if err != nil {
		return desired, fmt.Errorf("failed to update SQL warehouse permissions: %w", err)
	}

	return h.Get(ctx, desired)
}

// SetWhatIf predicts the state Set would produce without writing the
// permission entry. The warehouse lookup is read-only.
func (h *SqlWarehousePermissionHandler) SetWhatIf(ctx context.Context, desired SqlWarehousePermissionState) (SqlWarehousePermissionState, error) {
	if err := validateWarehousePermissionIdentity(&desired); err != nil {
		return desired, err
	}
	if err := requireFields(field{"permission_level", desired.PermissionLevel}); err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	warehouseID := desired.WarehouseID
	warehouseName := desired.WarehouseName
	if warehouseID == "" {
		info, err := w.Warehouses.GetByName(ctx, warehouseName)
		if err != nil {
			return desired, fmt.Errorf("SQL warehouse not found by name=%s: %w", warehouseName, err)
		}
		warehouseID = info.Id
		warehouseName = info.Name
	}

	principalType, principalName := desired.principalKey()
	logInfof(MsgWhatIfPut, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID+" permission_level="+desired.PermissionLevel)

	projected := SqlWarehousePermissionState{
		WarehouseID:          warehouseID,
		WarehouseName:        warehouseName,
		UserName:             desired.UserName,
		GroupName:            desired.GroupName,
		ServicePrincipalName: desired.ServicePrincipalName,
		PermissionLevel:      desired.PermissionLevel,
	}
	projected.SetExist(true)
	return projected, nil
}

func (h *SqlWarehousePermissionHandler) Delete(ctx context.Context, in SqlWarehousePermissionState) error {
	if err := validateWarehousePermissionIdentity(&in); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	// Resolve warehouse ID.
	warehouseID := in.WarehouseID
	if warehouseID == "" {
		info, lookupErr := w.Warehouses.GetByName(ctx, in.WarehouseName)
		if lookupErr != nil {
			return nil // Warehouse not found — nothing to delete.
		}
		warehouseID = info.Id
	}

	principalType, principalName := in.principalKey()
	logDebugf(MsgDelete, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)

	// GET current permissions so we can rebuild the list without the target entry.
	perms, err := w.Warehouses.GetPermissions(ctx, sql.GetWarehousePermissionsRequest{WarehouseId: warehouseID})
	if err != nil {
		return nil // Cannot read permissions — nothing to delete.
	}

	// Build a new ACL list containing all direct permissions except the one being removed.
	var filtered []sql.WarehouseAccessControlRequest
	for _, entry := range perms.AccessControlList {
		if matchesWarehousePrincipal(entry, &in) {
			continue
		}
		level := directWarehousePermissionLevel(entry)
		if level == "" {
			continue // Skip inherited-only entries.
		}
		filtered = append(filtered, sql.WarehouseAccessControlRequest{
			UserName:             entry.UserName,
			GroupName:            entry.GroupName,
			ServicePrincipalName: entry.ServicePrincipalName,
			PermissionLevel:      sql.WarehousePermissionLevel(level),
		})
	}

	// PUT (SetPermissions) replaces all direct permissions with the filtered list.
	_, err = w.Warehouses.SetPermissions(ctx, sql.WarehousePermissionsRequest{
		WarehouseId:       warehouseID,
		AccessControlList: filtered,
	})
	return err
}

func (h *SqlWarehousePermissionHandler) Export(ctx context.Context, _ SqlWarehousePermissionState) ([]SqlWarehousePermissionState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "SqlWarehousePermission")
	warehouses, err := w.Warehouses.ListAll(ctx, sql.ListWarehousesRequest{})
	if err != nil {
		return nil, err
	}

	var all []SqlWarehousePermissionState
	for _, wh := range warehouses {
		perms, err := w.Warehouses.GetPermissions(ctx, sql.GetWarehousePermissionsRequest{WarehouseId: wh.Id})
		if err != nil {
			continue
		}
		for _, entry := range perms.AccessControlList {
			level := directWarehousePermissionLevel(entry)
			if level == "" {
				continue
			}
			state := SqlWarehousePermissionState{
				WarehouseID:          wh.Id,
				WarehouseName:        wh.Name,
				UserName:             entry.UserName,
				GroupName:            entry.GroupName,
				ServicePrincipalName: entry.ServicePrincipalName,
				PermissionLevel:      level,
			}
			state.SetExist(true)
			all = append(all, state)
		}
	}

	return all, nil
}
