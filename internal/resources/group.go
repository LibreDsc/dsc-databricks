package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

// GroupState represents the full state of a Databricks group.
type GroupState struct {
	dsc.ExistProperty
	DisplayName  string             `json:"display_name" dsc:"optional" description:"String that represents a human-readable group name. Required when creating a group; either id or display_name identifies an existing one."`
	ExternalID   string             `json:"external_id,omitempty" description:"External ID used to identify the group in an external system."`
	ID           string             `json:"id,omitempty" description:"Databricks group ID."`
	Entitlements []UserComplexValue `json:"entitlements,omitempty" description:"Entitlements assigned to the group."`
	Members      []UserComplexValue `json:"members,omitempty" description:"Members belonging to the group."`
	Roles        []UserComplexValue `json:"roles,omitempty" description:"Corresponds to AWS instance profile/ARN role."`
}

func groupConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Group",
		Version:     "0.1.0",
		Description: "Manage Databricks groups",
		Tags:        []string{"databricks", "group", "iam", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks groups.",
			AllowAdditionalProperties: true,
		},
	}
}

// GroupHandler handles Group resource operations.
type GroupHandler struct{}

func groupToState(g *iam.Group) GroupState {
	state := GroupState{
		DisplayName:  g.DisplayName,
		ExternalID:   g.ExternalId,
		ID:           g.Id,
		Entitlements: fromIamComplexValues(g.Entitlements),
		Members:      fromIamComplexValues(g.Members),
		Roles:        fromIamComplexValues(g.Roles),
	}
	state.SetExist(true)
	return state
}

func (h *GroupHandler) Get(ctx context.Context, in GroupState) (GroupState, error) {
	if err := requireAtLeastOne("id or display_name", in.ID, in.DisplayName); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.ID != "" {
		logDebugf(MsgLookup, "Group", "id="+in.ID)
		g, err := w.GroupsV2.Get(ctx, iam.GetGroupRequest{Id: in.ID})
		if err != nil {
			logInfof(MsgNotFound, "Group", "id="+in.ID)
			return dsc.NotFound(GroupState{ID: in.ID, DisplayName: in.DisplayName}, "Group", "id="+in.ID)
		}
		return groupToState(g), nil
	}

	logDebugf(MsgLookup, "Group", "display_name="+in.DisplayName)
	groups, err := w.GroupsV2.ListAll(ctx, iam.ListGroupsRequest{})
	if err == nil {
		for _, g := range groups {
			if g.DisplayName == in.DisplayName {
				full, err := w.GroupsV2.Get(ctx, iam.GetGroupRequest{Id: g.Id})
				if err != nil {
					break
				}
				return groupToState(full), nil
			}
		}
	}
	logInfof(MsgNotFound, "Group", "display_name="+in.DisplayName)
	return dsc.NotFound(GroupState{DisplayName: in.DisplayName}, "Group", "display_name="+in.DisplayName)
}

func (h *GroupHandler) Set(ctx context.Context, desired GroupState) (GroupState, error) {
	if err := requireAtLeastOne("id or display_name", desired.ID, desired.DisplayName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgUpdate, "Group", "id="+current.ID)
		// Group exists — build an UpdateGroupRequest from desired state, overlaying
		// onto the current full state so the SCIM PUT is complete.
		full, err := w.GroupsV2.Get(ctx, iam.GetGroupRequest{Id: current.ID})
		if err != nil {
			return desired, fmt.Errorf("failed to get group for update: %w", err)
		}
		update := iam.UpdateGroupRequest{
			Id:              full.Id,
			DisplayName:     full.DisplayName,
			ExternalId:      full.ExternalId,
			Entitlements:    full.Entitlements,
			Groups:          full.Groups,
			Members:         full.Members,
			Roles:           full.Roles,
			Schemas:         full.Schemas,
			ForceSendFields: []string{"DisplayName"},
		}
		if desired.DisplayName != "" {
			update.DisplayName = desired.DisplayName
		}
		if desired.ExternalID != "" {
			update.ExternalId = desired.ExternalID
			update.ForceSendFields = append(update.ForceSendFields, "ExternalId")
		}
		if len(desired.Entitlements) > 0 {
			update.Entitlements = toIamComplexValues(desired.Entitlements)
		}
		if len(desired.Members) > 0 {
			update.Members = toIamComplexValues(desired.Members)
		}
		if len(desired.Roles) > 0 {
			update.Roles = toIamComplexValues(desired.Roles)
		}
		if err := w.GroupsV2.Update(ctx, update); err != nil {
			return desired, fmt.Errorf("failed to update group: %w", err)
		}
		// Build the after state directly from the applied update values to avoid
		// eventual-consistency issues where an immediate re-GET may return stale data.
		afterFull := *full
		afterFull.DisplayName = update.DisplayName
		afterFull.ExternalId = update.ExternalId
		afterFull.Entitlements = update.Entitlements
		afterFull.Members = update.Members
		afterFull.Roles = update.Roles
		return groupToState(&afterFull), nil
	}

	logInfof(MsgCreate, "Group", "display_name="+desired.DisplayName)
	// Group does not exist — create it.
	if err := requireFields(field{"display_name", desired.DisplayName}); err != nil {
		return desired, err
	}
	created, err := w.GroupsV2.Create(ctx, iam.CreateGroupRequest{
		DisplayName:  desired.DisplayName,
		ExternalId:   desired.ExternalID,
		Entitlements: toIamComplexValues(desired.Entitlements),
		Members:      toIamComplexValues(desired.Members),
		Roles:        toIamComplexValues(desired.Roles),
	})
	if err != nil {
		return desired, fmt.Errorf("failed to create group: %w", err)
	}
	// Use the server response directly for the after state — it contains the assigned ID.
	return groupToState(created), nil
}

// projectGroupUpdate mirrors Set's SCIM overlay: non-empty desired strings
// and non-empty slices win; the id carries over from the current state.
func projectGroupUpdate(desired, current *GroupState) GroupState {
	projected := *current
	if desired.DisplayName != "" {
		projected.DisplayName = desired.DisplayName
	}
	if desired.ExternalID != "" {
		projected.ExternalID = desired.ExternalID
	}
	if len(desired.Entitlements) > 0 {
		projected.Entitlements = desired.Entitlements
	}
	if len(desired.Members) > 0 {
		projected.Members = desired.Members
	}
	if len(desired.Roles) > 0 {
		projected.Roles = desired.Roles
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the group.
func (h *GroupHandler) SetWhatIf(ctx context.Context, desired GroupState) (GroupState, error) {
	if err := requireAtLeastOne("id or display_name", desired.ID, desired.DisplayName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Group", "id="+current.ID)
		return projectGroupUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"display_name", desired.DisplayName}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "Group", "display_name="+desired.DisplayName)
	// The server-assigned id is unknown before creation.
	projected := desired
	projected.ID = ""
	projected.SetExist(true)
	return projected, nil
}

func (h *GroupHandler) Test(ctx context.Context, desired GroupState) (dsc.TestResult[GroupState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[GroupState]{}, err
	}

	result := dsc.TestResult[GroupState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *GroupHandler) Delete(ctx context.Context, in GroupState) error {
	if err := requireAtLeastOne("id or display_name", in.ID, in.DisplayName); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	if in.ID != "" {
		logDebugf(MsgDelete, "Group", "id="+in.ID)
		return w.GroupsV2.Delete(ctx, iam.DeleteGroupRequest{Id: in.ID})
	}

	logDebugf(MsgDelete, "Group", "display_name="+in.DisplayName)
	groups, err := w.GroupsV2.ListAll(ctx, iam.ListGroupsRequest{})
	if err == nil {
		for _, g := range groups {
			if g.DisplayName == in.DisplayName {
				return w.GroupsV2.Delete(ctx, iam.DeleteGroupRequest{Id: g.Id})
			}
		}
	}
	// Already absent — deleting a missing instance succeeds.
	logInfof(MsgNotFound, "Group", "display_name="+in.DisplayName)
	return nil
}

func (h *GroupHandler) Export(ctx context.Context, _ GroupState) ([]GroupState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Group")
	groups, err := w.GroupsV2.ListAll(ctx, iam.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}

	var allGroups []GroupState
	for i := range groups {
		allGroups = append(allGroups, groupToState(&groups[i]))
	}

	return allGroups, nil
}
