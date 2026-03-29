package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Group", &GroupHandler{}, groupMetadata())
}

// Group property descriptions from SDK documentation.
var groupPropertyDescriptions = dsc.PropertyDescriptions{
	"id":           "Databricks group ID.",
	"display_name": "String that represents a human-readable group name.",
	"external_id":  "External ID used to identify the group in an external system.",
	"entitlements": "Entitlements assigned to the group.",
	"members":      "Members belonging to the group.",
	"roles":        "Corresponds to AWS instance profile/ARN role.",
}

// GroupSchemaInput defines the desired-state fields for the schema
// and for unmarshaling input. id is computed on create so it carries omitempty.
type GroupSchemaInput struct {
	DisplayName  string             `json:"display_name"`
	ExternalID   string             `json:"external_id,omitempty"`
	ID           string             `json:"id,omitempty"`
	Entitlements []UserComplexValue `json:"entitlements,omitempty"`
	Members      []UserComplexValue `json:"members,omitempty"`
	Roles        []UserComplexValue `json:"roles,omitempty"`
}

func groupMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/Group",
		Description:       "Manage Databricks groups",
		SchemaDescription: "Schema for managing Databricks groups.",
		ResourceName:      "group",
		Tags:              []string{"databricks", "group", "iam", "workspace"},
		Descriptions:      groupPropertyDescriptions,
		SchemaType:        reflect.TypeFor[GroupSchemaInput](),
	})
}

// GroupState represents the full state of a Databricks group.
type GroupState struct {
	DisplayName  string             `json:"display_name"`
	ExternalID   string             `json:"external_id,omitempty"`
	ID           string             `json:"id,omitempty"`
	Entitlements []UserComplexValue `json:"entitlements,omitempty"`
	Members      []UserComplexValue `json:"members,omitempty"`
	Roles        []UserComplexValue `json:"roles,omitempty"`
	Exist        bool               `json:"_exist"`
}

// GroupHandler handles Group resource operations.
type GroupHandler struct{}

func groupToState(g *iam.Group) GroupState {
	return GroupState{
		DisplayName:  g.DisplayName,
		ExternalID:   g.ExternalId,
		ID:           g.Id,
		Entitlements: fromIamComplexValues(g.Entitlements),
		Members:      fromIamComplexValues(g.Members),
		Roles:        fromIamComplexValues(g.Roles),
		Exist:        true,
	}
}

func (h *GroupHandler) getCurrentState(ctx dsc.ResourceContext, req *GroupSchemaInput) (GroupState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return GroupState{Exist: false}, err
	}

	if req.ID != "" {
		dsc.Logger.Debugf(dsc.MsgLookup, "Group", "id="+req.ID)
		g, err := w.GroupsV2.Get(cmdCtx, iam.GetGroupRequest{Id: req.ID})
		if err != nil {
			dsc.Logger.Infof(dsc.MsgNotFound, "Group", "id="+req.ID)
			return GroupState{Exist: false}, nil
		}
		return groupToState(g), nil
	}

	if req.DisplayName != "" {
		dsc.Logger.Debugf(dsc.MsgLookup, "Group", "display_name="+req.DisplayName)
		groups, err := w.GroupsV2.ListAll(cmdCtx, iam.ListGroupsRequest{})
		if err != nil {
			return GroupState{DisplayName: req.DisplayName, Exist: false}, nil
		}
		for _, g := range groups {
			if g.DisplayName == req.DisplayName {
				full, err := w.GroupsV2.Get(cmdCtx, iam.GetGroupRequest{Id: g.Id})
				if err != nil {
					return GroupState{DisplayName: req.DisplayName, Exist: false}, nil
				}
				return groupToState(full), nil
			}
		}
		dsc.Logger.Infof(dsc.MsgNotFound, "Group", "display_name="+req.DisplayName)
		return GroupState{DisplayName: req.DisplayName, Exist: false}, nil
	}

	return GroupState{Exist: false}, fmt.Errorf("id or display_name must be provided")
}

func (h *GroupHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[GroupSchemaInput](input)
	if err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *GroupHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[GroupSchemaInput](input)
	if err != nil {
		return nil, err
	}

	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var afterState GroupState

	if beforeState.Exist {
		dsc.Logger.Infof(dsc.MsgUpdate, "Group", "id="+beforeState.ID)
		// Group exists — build an UpdateGroupRequest from desired state, overlaying
		// onto the current full state so the SCIM PUT is complete.
		full, err := w.GroupsV2.Get(cmdCtx, iam.GetGroupRequest{Id: beforeState.ID})
		if err != nil {
			return nil, fmt.Errorf("failed to get group for update: %w", err)
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
		if schemaInput.DisplayName != "" {
			update.DisplayName = schemaInput.DisplayName
		}
		if schemaInput.ExternalID != "" {
			update.ExternalId = schemaInput.ExternalID
			update.ForceSendFields = append(update.ForceSendFields, "ExternalId")
		}
		if len(schemaInput.Entitlements) > 0 {
			update.Entitlements = toIamComplexValues(schemaInput.Entitlements)
		}
		if len(schemaInput.Members) > 0 {
			update.Members = toIamComplexValues(schemaInput.Members)
		}
		if len(schemaInput.Roles) > 0 {
			update.Roles = toIamComplexValues(schemaInput.Roles)
		}
		if err := w.GroupsV2.Update(cmdCtx, update); err != nil {
			return nil, fmt.Errorf("failed to update group: %w", err)
		}
		// Build afterState directly from the applied update values to avoid
		// eventual-consistency issues where an immediate re-GET may return stale data.
		afterFull := *full
		afterFull.DisplayName = update.DisplayName
		afterFull.ExternalId = update.ExternalId
		afterFull.Entitlements = update.Entitlements
		afterFull.Members = update.Members
		afterFull.Roles = update.Roles
		afterState = groupToState(&afterFull)
	} else if schemaInput.DisplayName != "" {
		dsc.Logger.Infof(dsc.MsgCreate, "Group", "display_name="+schemaInput.DisplayName)
		// Group does not exist — create it.
		created, err := w.GroupsV2.Create(cmdCtx, iam.CreateGroupRequest{
			DisplayName:  schemaInput.DisplayName,
			ExternalId:   schemaInput.ExternalID,
			Entitlements: toIamComplexValues(schemaInput.Entitlements),
			Members:      toIamComplexValues(schemaInput.Members),
			Roles:        toIamComplexValues(schemaInput.Roles),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create group: %w", err)
		}
		// Use the server response directly for afterState — it contains the assigned ID.
		afterState = groupToState(created)
	} else {
		return nil, dsc.ValidateRequired(dsc.RequiredField{Name: "display_name", Value: ""})
	}

	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *GroupHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[GroupSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := GroupState{
		ID:           schemaInput.ID,
		DisplayName:  schemaInput.DisplayName,
		ExternalID:   schemaInput.ExternalID,
		Entitlements: schemaInput.Entitlements,
		Members:      schemaInput.Members,
		Roles:        schemaInput.Roles,
		Exist:        true,
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

func (h *GroupHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[GroupSchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	if schemaInput.ID != "" {
		dsc.Logger.Debugf(dsc.MsgDelete, "Group", "id="+schemaInput.ID)
		return w.GroupsV2.Delete(cmdCtx, iam.DeleteGroupRequest{Id: schemaInput.ID})
	}

	if schemaInput.DisplayName != "" {
		dsc.Logger.Debugf(dsc.MsgDelete, "Group", "display_name="+schemaInput.DisplayName)
		groups, err := w.GroupsV2.ListAll(cmdCtx, iam.ListGroupsRequest{})
		if err != nil {
			return dsc.NotFoundError("group", "display_name="+schemaInput.DisplayName)
		}
		for _, g := range groups {
			if g.DisplayName == schemaInput.DisplayName {
				return w.GroupsV2.Delete(cmdCtx, iam.DeleteGroupRequest{Id: g.Id})
			}
		}
		return dsc.NotFoundError("group", "display_name="+schemaInput.DisplayName)
	}

	return dsc.ValidateRequired(dsc.RequiredField{Name: "id or display_name", Value: ""})
}

func (h *GroupHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(dsc.MsgListAll, "Group")
	groups, err := w.GroupsV2.ListAll(cmdCtx, iam.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}

	var allGroups []any
	for i := range groups {
		allGroups = append(allGroups, groupToState(&groups[i]))
	}

	return allGroups, nil
}
