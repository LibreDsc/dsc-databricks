package resources

import (
	"context"
	"fmt"
	"slices"
	"strings"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// GrantState represents the privileges a single principal holds directly on a
// single Unity Catalog securable.
type GrantState struct {
	dsc.ExistProperty
	Privileges    []string `json:"privileges,omitempty" description:"Privileges the principal holds directly on the securable (e.g. USE_CATALOG, SELECT, ALL_PRIVILEGES). Order-insensitive. Required for set."`
	SecurableType string   `json:"securable_type" description:"Type of the securable (lowercase)." enum:"catalog,clean_room,connection,credential,external_location,external_metadata,function,metastore,pipeline,provider,recipient,schema,share,staging_table,storage_credential,table,volume"`
	FullName      string   `json:"full_name" description:"Full name of the securable (e.g. 'main', 'main.default', or 'main.default.my_table')."`
	Principal     string   `json:"principal" description:"User email, group name, or service principal application ID the privileges are granted to."`
}

// grantKey renders the identifying triple for log messages.
func grantKey(securableType, fullName, principal string) string {
	return "securable=" + securableType + "/" + fullName + " principal=" + principal
}

func grantConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Grant",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog privilege grants for a principal on a securable.",
		Tags:        []string{"databricks", "grant", "unitycatalog", "permissions"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog privilege grants.",
			AllowAdditionalProperties: true,
		},
	}
}

// GrantHandler handles Grant resource operations.
type GrantHandler struct{}

// sortedPrivileges returns a sorted copy of the privilege list, the canonical
// form used in all returned states so comparisons and diffs are stable.
func sortedPrivileges(privileges []catalog.Privilege) []string {
	if len(privileges) == 0 {
		return nil
	}
	out := make([]string, 0, len(privileges))
	for _, p := range privileges {
		out = append(out, string(p))
	}
	slices.Sort(out)
	return out
}

// computeGrantChanges returns the privileges to add and remove to converge
// the principal's current privilege set to the desired one.
func computeGrantChanges(desired, current []string) (add, remove []catalog.Privilege) {
	for _, p := range desired {
		if !slices.Contains(current, p) {
			add = append(add, catalog.Privilege(p))
		}
	}
	for _, p := range current {
		if !slices.Contains(desired, p) {
			remove = append(remove, catalog.Privilege(p))
		}
	}
	return add, remove
}

func (h *GrantHandler) validate(in *GrantState) error {
	return requireFields(
		field{"securable_type", in.SecurableType},
		field{"full_name", in.FullName},
		field{"principal", in.Principal},
	)
}

func (h *GrantHandler) Get(ctx context.Context, in GrantState) (GrantState, error) {
	if err := h.validate(&in); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	secType := strings.ToLower(in.SecurableType)
	key := grantKey(secType, in.FullName, in.Principal)
	logDebugf(MsgLookup, "Grant", key)
	resp, err := w.Grants.Get(ctx, catalog.GetGrantRequest{
		SecurableType: secType,
		FullName:      in.FullName,
		Principal:     in.Principal,
	})
	if err != nil {
		logInfof(MsgNotFound, "Grant", key)
		return dsc.NotFound(GrantState{
			SecurableType: secType,
			FullName:      in.FullName,
			Principal:     in.Principal,
		}, "Grant", key)
	}

	for i := range resp.PrivilegeAssignments {
		assignment := &resp.PrivilegeAssignments[i]
		if assignment.Principal != in.Principal || len(assignment.Privileges) == 0 {
			continue
		}
		state := GrantState{
			SecurableType: secType,
			FullName:      in.FullName,
			Principal:     in.Principal,
			Privileges:    sortedPrivileges(assignment.Privileges),
		}
		state.SetExist(true)
		return state, nil
	}

	logInfof(MsgNotFound, "Grant", key)
	return dsc.NotFound(GrantState{
		SecurableType: secType,
		FullName:      in.FullName,
		Principal:     in.Principal,
	}, "Grant", key)
}

func (h *GrantHandler) Set(ctx context.Context, desired GrantState) (GrantState, error) {
	if err := h.validate(&desired); err != nil {
		return desired, err
	}
	if len(desired.Privileges) == 0 {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "missing required field(s): privileges")
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	secType := strings.ToLower(desired.SecurableType)
	key := grantKey(secType, desired.FullName, desired.Principal)
	add, remove := computeGrantChanges(desired.Privileges, current.Privileges)
	if len(add) == 0 && len(remove) == 0 {
		logDebugf(MsgAlreadyExists, "Grant", key)
		return current, nil
	}

	logInfof(MsgPut, "Grant", key)
	if _, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
		SecurableType: secType,
		FullName:      desired.FullName,
		Changes: []catalog.PermissionsChange{{
			Principal: desired.Principal,
			Add:       add,
			Remove:    remove,
		}},
	}); err != nil {
		return desired, fmt.Errorf("failed to update grants: %w", err)
	}

	return h.Get(ctx, desired)
}

// projectGrantSet returns the state Set would produce: exactly the desired
// privileges in canonical (sorted) order.
func projectGrantSet(desired *GrantState) GrantState {
	privileges := make([]catalog.Privilege, 0, len(desired.Privileges))
	for _, p := range desired.Privileges {
		privileges = append(privileges, catalog.Privilege(p))
	}
	projected := GrantState{
		SecurableType: strings.ToLower(desired.SecurableType),
		FullName:      desired.FullName,
		Principal:     desired.Principal,
		Privileges:    sortedPrivileges(privileges),
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without changing any grants.
func (h *GrantHandler) SetWhatIf(ctx context.Context, desired GrantState) (GrantState, error) {
	if err := h.validate(&desired); err != nil {
		return desired, err
	}
	if len(desired.Privileges) == 0 {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "missing required field(s): privileges")
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	key := grantKey(strings.ToLower(desired.SecurableType), desired.FullName, desired.Principal)
	if current.ShouldExist() {
		logInfof(MsgWhatIfPut, "Grant", key)
	} else {
		logInfof(MsgWhatIfCreate, "Grant", key)
	}
	return projectGrantSet(&desired), nil
}

// sortedGrantState returns a copy of the state with privileges sorted into
// the canonical order used by Get.
func sortedGrantState(in *GrantState) GrantState {
	out := *in
	out.SecurableType = strings.ToLower(in.SecurableType)
	if len(in.Privileges) > 0 {
		out.Privileges = slices.Clone(in.Privileges)
		slices.Sort(out.Privileges)
	}
	return out
}

// Test compares the desired privilege set against the actual one with
// order-insensitive semantics: both sides are sorted into canonical form
// before comparison.
func (h *GrantHandler) Test(ctx context.Context, desired GrantState) (dsc.TestResult[GrantState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[GrantState]{}, err
	}

	result := dsc.TestResult[GrantState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	normalized := sortedGrantState(&desired)
	result.DifferingProperties = dsc.CompareStates(normalized, actual)
	return result, nil
}

// Delete revokes every privilege the principal holds directly on the
// securable; the privileges in the input are ignored (the resource owns the
// principal's whole direct privilege set).
func (h *GrantHandler) Delete(ctx context.Context, in GrantState) error {
	if err := h.validate(&in); err != nil {
		return err
	}

	current, err := h.Get(ctx, in)
	if err != nil {
		return err
	}
	if !current.ShouldExist() {
		return nil
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	secType := strings.ToLower(in.SecurableType)
	remove := make([]catalog.Privilege, 0, len(current.Privileges))
	for _, p := range current.Privileges {
		remove = append(remove, catalog.Privilege(p))
	}

	logDebugf(MsgDelete, "Grant", grantKey(secType, in.FullName, in.Principal))
	if _, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
		SecurableType: secType,
		FullName:      in.FullName,
		Changes: []catalog.PermissionsChange{{
			Principal: in.Principal,
			Remove:    remove,
		}},
	}); err != nil {
		return fmt.Errorf("failed to revoke grants: %w", err)
	}
	return nil
}

// Export enumerates direct grants on metastore-level securables that can be
// listed with a single call each: catalogs, external locations, storage
// credentials, service credentials, and connections. Grants on schemas,
// tables, volumes, and functions are not exported — walking every securable
// in the metastore would be unbounded.
func (h *GrantHandler) Export(ctx context.Context, _ GrantState) ([]GrantState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Grant")

	type securable struct {
		secType string
		name    string
	}
	var securables []securable

	if catalogs, err := w.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{}); err == nil {
		for i := range catalogs {
			securables = append(securables, securable{"catalog", catalogs[i].Name})
		}
	}
	if locations, err := w.ExternalLocations.ListAll(ctx, catalog.ListExternalLocationsRequest{}); err == nil {
		for i := range locations {
			securables = append(securables, securable{"external_location", locations[i].Name})
		}
	}
	if credentials, err := w.StorageCredentials.ListAll(ctx, catalog.ListStorageCredentialsRequest{}); err == nil {
		for i := range credentials {
			securables = append(securables, securable{"storage_credential", credentials[i].Name})
		}
	}
	if credentials, err := w.Credentials.ListCredentialsAll(ctx, catalog.ListCredentialsRequest{}); err == nil {
		for i := range credentials {
			securables = append(securables, securable{"credential", credentials[i].Name})
		}
	}
	if connections, err := w.Connections.ListAll(ctx, catalog.ListConnectionsRequest{}); err == nil {
		for i := range connections {
			securables = append(securables, securable{"connection", connections[i].Name})
		}
	}

	var all []GrantState
	for _, s := range securables {
		resp, err := w.Grants.GetBySecurableTypeAndFullName(ctx, s.secType, s.name)
		if err != nil {
			logInfof(MsgSkipping, "Grant", s.secType+"/"+s.name, err)
			continue
		}
		for i := range resp.PrivilegeAssignments {
			assignment := &resp.PrivilegeAssignments[i]
			if assignment.Principal == "" || len(assignment.Privileges) == 0 {
				continue
			}
			state := GrantState{
				SecurableType: s.secType,
				FullName:      s.name,
				Principal:     assignment.Principal,
				Privileges:    sortedPrivileges(assignment.Privileges),
			}
			state.SetExist(true)
			all = append(all, state)
		}
	}

	return all, nil
}
