package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/SqlWarehousePermission", &SqlWarehousePermissionHandler{}, sqlWarehousePermissionMetadata())
}

var sqlWarehousePermissionPropertyDescriptions = dsc.PropertyDescriptions{
	"warehouse_id":          "The unique identifier of the SQL warehouse.",
	"warehouse_name":        "The logical name of the SQL warehouse. Can be used instead of warehouse_id for lookup.",
	"user_name":             "The name of the user to which the permission applies.",
	"group_name":            "The name of the group to which the permission applies.",
	"service_principal_name": "The application ID of the service principal to which the permission applies.",
	"permission_level":      "The permission level: CAN_MANAGE, CAN_MONITOR, CAN_USE, CAN_VIEW, or IS_OWNER.",
}

// SqlWarehousePermissionSchemaInput defines the input fields for the schema.
type SqlWarehousePermissionSchemaInput struct {
	WarehouseID          string `json:"warehouse_id,omitempty"`
	WarehouseName        string `json:"warehouse_name,omitempty"`
	UserName             string `json:"user_name,omitempty"`
	GroupName            string `json:"group_name,omitempty"`
	ServicePrincipalName string `json:"service_principal_name,omitempty"`
	PermissionLevel      string `json:"permission_level,omitempty"`
}

func (s *SqlWarehousePermissionSchemaInput) principalKey() (string, string) {
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

func sqlWarehousePermissionMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/SqlWarehousePermission",
		Description:       "Manage Databricks SQL warehouse permissions.",
		SchemaDescription: "Schema for managing Databricks SQL warehouse permissions.",
		ResourceName:      "SQL warehouse permission",
		Tags:              []string{"databricks", "sqlwarehouse", "permissions", "sql"},
		Descriptions:      sqlWarehousePermissionPropertyDescriptions,
		SchemaType:        reflect.TypeFor[SqlWarehousePermissionSchemaInput](),
		OmitTest:          true,
	})
}

// SqlWarehousePermissionState represents the state of a single permission entry
// on a Databricks SQL warehouse.
type SqlWarehousePermissionState struct {
	WarehouseID          string `json:"warehouse_id,omitempty"`
	WarehouseName        string `json:"warehouse_name,omitempty"`
	UserName             string `json:"user_name,omitempty"`
	GroupName            string `json:"group_name,omitempty"`
	ServicePrincipalName string `json:"service_principal_name,omitempty"`
	PermissionLevel      string `json:"permission_level,omitempty"`
	Exist                bool   `json:"_exist"`
}

// SqlWarehousePermissionHandler handles SqlWarehousePermission resource operations.
type SqlWarehousePermissionHandler struct{}

func matchesWarehousePrincipal(entry sql.WarehouseAccessControlResponse, req *SqlWarehousePermissionSchemaInput) bool {
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

func (h *SqlWarehousePermissionHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[SqlWarehousePermissionSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("warehouse_id or warehouse_name", req.WarehouseID, req.WarehouseName); err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("user_name, group_name, or service_principal_name", req.UserName, req.GroupName, req.ServicePrincipalName); err != nil {
		return nil, err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	warehouseID := req.WarehouseID
	warehouseName := req.WarehouseName

	if warehouseID == "" && warehouseName != "" {
		dsc.Logger.Debugf(dsc.MsgLookup, "SqlWarehousePermission", "warehouse_name="+warehouseName)
		info, err := w.Warehouses.GetByName(cmdCtx, warehouseName)
		if err != nil {
			dsc.Logger.Infof(dsc.MsgNotFound, "SqlWarehousePermission", "warehouse_name="+warehouseName)
			return &dsc.GetResult{ActualState: SqlWarehousePermissionState{
				WarehouseName:        warehouseName,
				UserName:             req.UserName,
				GroupName:            req.GroupName,
				ServicePrincipalName: req.ServicePrincipalName,
				Exist:                false,
			}}, nil
		}
		warehouseID = info.Id
		warehouseName = info.Name
	}

	principalType, principalName := req.principalKey()
	dsc.Logger.Debugf(dsc.MsgLookup, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)

	perms, err := w.Warehouses.GetPermissions(cmdCtx, sql.GetWarehousePermissionsRequest{WarehouseId: warehouseID})
	if err != nil {
		dsc.Logger.Infof(dsc.MsgNotFound, "SqlWarehousePermission", "warehouse_id="+warehouseID)
		return &dsc.GetResult{ActualState: SqlWarehousePermissionState{
			WarehouseID:          warehouseID,
			WarehouseName:        warehouseName,
			UserName:             req.UserName,
			GroupName:            req.GroupName,
			ServicePrincipalName: req.ServicePrincipalName,
			Exist:                false,
		}}, nil
	}

	for _, entry := range perms.AccessControlList {
		if !matchesWarehousePrincipal(entry, &req) {
			continue
		}
		level := directWarehousePermissionLevel(entry)
		if level != "" {
			return &dsc.GetResult{ActualState: SqlWarehousePermissionState{
				WarehouseID:          warehouseID,
				WarehouseName:        warehouseName,
				UserName:             entry.UserName,
				GroupName:            entry.GroupName,
				ServicePrincipalName: entry.ServicePrincipalName,
				PermissionLevel:      level,
				Exist:                true,
			}}, nil
		}
	}

	dsc.Logger.Infof(dsc.MsgNotFound, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)
	return &dsc.GetResult{ActualState: SqlWarehousePermissionState{
		WarehouseID:          warehouseID,
		WarehouseName:        warehouseName,
		UserName:             req.UserName,
		GroupName:            req.GroupName,
		ServicePrincipalName: req.ServicePrincipalName,
		Exist:                false,
	}}, nil
}

func (h *SqlWarehousePermissionHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[SqlWarehousePermissionSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("warehouse_id or warehouse_name", req.WarehouseID, req.WarehouseName); err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("user_name, group_name, or service_principal_name", req.UserName, req.GroupName, req.ServicePrincipalName); err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "permission_level", Value: req.PermissionLevel}); err != nil {
		return nil, err
	}

	// Capture before state.
	beforeResult, _ := h.Get(ctx, input)
	var beforeState SqlWarehousePermissionState
	if beforeResult != nil {
		if s, ok := beforeResult.ActualState.(SqlWarehousePermissionState); ok {
			beforeState = s
		}
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	// Resolve warehouse ID.
	warehouseID := req.WarehouseID
	if warehouseID == "" && req.WarehouseName != "" {
		info, err := w.Warehouses.GetByName(cmdCtx, req.WarehouseName)
		if err != nil {
			return nil, fmt.Errorf("SQL warehouse not found by name=%s: %w", req.WarehouseName, err)
		}
		warehouseID = info.Id
	}

	principalType, principalName := req.principalKey()
	dsc.Logger.Infof(dsc.MsgPut, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID+" permission_level="+req.PermissionLevel)

	// PATCH (UpdatePermissions) adds or updates the specific entry without
	// disturbing other entries on the warehouse.
	_, err = w.Warehouses.UpdatePermissions(cmdCtx, sql.WarehousePermissionsRequest{
		WarehouseId: warehouseID,
		AccessControlList: []sql.WarehouseAccessControlRequest{{
			UserName:             req.UserName,
			GroupName:            req.GroupName,
			ServicePrincipalName: req.ServicePrincipalName,
			PermissionLevel:      sql.WarehousePermissionLevel(req.PermissionLevel),
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update SQL warehouse permissions: %w", err)
	}

	// Capture after state.
	afterResult, _ := h.Get(ctx, input)
	var afterState SqlWarehousePermissionState
	if afterResult != nil {
		if s, ok := afterResult.ActualState.(SqlWarehousePermissionState); ok {
			afterState = s
		}
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *SqlWarehousePermissionHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[SqlWarehousePermissionSchemaInput](input)
	if err != nil {
		return nil, err
	}

	result, err := h.Get(ctx, input)
	if err != nil {
		return nil, err
	}

	actualState := result.ActualState
	desiredState := SqlWarehousePermissionState{
		WarehouseID:          req.WarehouseID,
		WarehouseName:        req.WarehouseName,
		UserName:             req.UserName,
		GroupName:            req.GroupName,
		ServicePrincipalName: req.ServicePrincipalName,
		PermissionLevel:      req.PermissionLevel,
		Exist:                true,
	}

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *SqlWarehousePermissionHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[SqlWarehousePermissionSchemaInput](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateAtLeastOne("warehouse_id or warehouse_name", req.WarehouseID, req.WarehouseName); err != nil {
		return err
	}
	if err := dsc.ValidateAtLeastOne("user_name, group_name, or service_principal_name", req.UserName, req.GroupName, req.ServicePrincipalName); err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	// Resolve warehouse ID.
	warehouseID := req.WarehouseID
	if warehouseID == "" && req.WarehouseName != "" {
		info, lookupErr := w.Warehouses.GetByName(cmdCtx, req.WarehouseName)
		if lookupErr != nil {
			return nil // Warehouse not found — nothing to delete.
		}
		warehouseID = info.Id
	}

	principalType, principalName := req.principalKey()
	dsc.Logger.Debugf(dsc.MsgDelete, "SqlWarehousePermission", principalType+"="+principalName+" warehouse_id="+warehouseID)

	// GET current permissions so we can rebuild the list without the target entry.
	perms, err := w.Warehouses.GetPermissions(cmdCtx, sql.GetWarehousePermissionsRequest{WarehouseId: warehouseID})
	if err != nil {
		return nil // Cannot read permissions — nothing to delete.
	}

	// Build a new ACL list containing all direct permissions except the one being removed.
	var filtered []sql.WarehouseAccessControlRequest
	for _, entry := range perms.AccessControlList {
		if matchesWarehousePrincipal(entry, &req) {
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
	_, err = w.Warehouses.SetPermissions(cmdCtx, sql.WarehousePermissionsRequest{
		WarehouseId:       warehouseID,
		AccessControlList: filtered,
	})
	return err
}

func (h *SqlWarehousePermissionHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(dsc.MsgListAll, "SqlWarehousePermission")
	warehouses, err := w.Warehouses.ListAll(cmdCtx, sql.ListWarehousesRequest{})
	if err != nil {
		return nil, err
	}

	var all []any
	for _, wh := range warehouses {
		perms, err := w.Warehouses.GetPermissions(cmdCtx, sql.GetWarehousePermissionsRequest{WarehouseId: wh.Id})
		if err != nil {
			continue
		}
		for _, entry := range perms.AccessControlList {
			level := directWarehousePermissionLevel(entry)
			if level == "" {
				continue
			}
			all = append(all, SqlWarehousePermissionState{
				WarehouseID:          wh.Id,
				WarehouseName:        wh.Name,
				UserName:             entry.UserName,
				GroupName:            entry.GroupName,
				ServicePrincipalName: entry.ServicePrincipalName,
				PermissionLevel:      level,
				Exist:                true,
			})
		}
	}

	return all, nil
}
