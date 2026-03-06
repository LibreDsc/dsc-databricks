package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/ClusterPolicy", &ClusterPolicyHandler{}, clusterPolicyMetadata())
}

// ClusterPolicy property descriptions from SDK documentation.
var clusterPolicyPropertyDescriptions = dsc.PropertyDescriptions{
	"policy_id":                          "Canonical unique identifier for the cluster policy. Computed on create.",
	"name":                               "Cluster policy name. Must be unique. Length must be between 1 and 100 characters.",
	"definition":                         "Policy definition document expressed in Databricks Cluster Policy Definition Language (JSON string).",
	"description":                        "Additional human-readable description of the cluster policy.",
	"max_clusters_per_user":              "Max number of clusters per user that can be active using this policy. If not present, there is no limit.",
	"policy_family_id":                   "ID of the policy family whose definition this policy inherits. Cannot be used with definition.",
	"policy_family_definition_overrides": "Policy definition JSON document to customize the inherited policy family definition.",
}

// ClusterPolicySchemaInput defines the desired-state fields for the schema
// and for unmarshaling input. policy_id is computed on create; name is the
// human-readable identifier used for lookup — both carry omitempty so that
// inputs supplying only one of the two pass schema validation.
type ClusterPolicySchemaInput struct {
	Name                            string `json:"name,omitempty"`
	PolicyID                        string `json:"policy_id,omitempty"`
	Definition                      string `json:"definition,omitempty"`
	Description                     string `json:"description,omitempty"`
	MaxClustersPerUser              int64  `json:"max_clusters_per_user,omitempty"`
	PolicyFamilyID                  string `json:"policy_family_id,omitempty"`
	PolicyFamilyDefinitionOverrides string `json:"policy_family_definition_overrides,omitempty"`
}

func clusterPolicyMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/ClusterPolicy",
		Description:       "Manage Databricks cluster policies.",
		SchemaDescription: "Schema for managing Databricks cluster policies.",
		ResourceName:      "cluster_policy",
		Tags:              []string{"databricks", "clusterpolicy", "compute"},
		Descriptions:      clusterPolicyPropertyDescriptions,
		SchemaType:        reflect.TypeFor[ClusterPolicySchemaInput](),
		// definition is a JSON string; the server may normalise key ordering so
		// synthetic Test (which compares the raw string values) is sufficient —
		// both synthetic and custom implementations would behave identically here.
		OmitTest: true,
	})
}

// ClusterPolicyState represents the full state of a Databricks cluster policy.
type ClusterPolicyState struct {
	Name                            string `json:"name,omitempty"`
	PolicyID                        string `json:"policy_id,omitempty"`
	Definition                      string `json:"definition,omitempty"`
	Description                     string `json:"description,omitempty"`
	MaxClustersPerUser              int64  `json:"max_clusters_per_user,omitempty"`
	PolicyFamilyID                  string `json:"policy_family_id,omitempty"`
	PolicyFamilyDefinitionOverrides string `json:"policy_family_definition_overrides,omitempty"`
	Exist                           bool   `json:"_exist"`
}

// ClusterPolicyHandler handles ClusterPolicy resource operations.
type ClusterPolicyHandler struct{}

func policyToState(p *compute.Policy) ClusterPolicyState {
	return ClusterPolicyState{
		PolicyID:                       p.PolicyId,
		Name:                           p.Name,
		Definition:                     p.Definition,
		Description:                    p.Description,
		MaxClustersPerUser:             p.MaxClustersPerUser,
		PolicyFamilyID:                 p.PolicyFamilyId,
		PolicyFamilyDefinitionOverrides: p.PolicyFamilyDefinitionOverrides,
		Exist:                          true,
	}
}

func (h *ClusterPolicyHandler) getCurrentState(ctx dsc.ResourceContext, req *ClusterPolicySchemaInput) (ClusterPolicyState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return ClusterPolicyState{Exist: false}, err
	}

	if req.PolicyID != "" {
		p, err := w.ClusterPolicies.Get(cmdCtx, compute.GetClusterPolicyRequest{PolicyId: req.PolicyID})
		if err != nil {
			return ClusterPolicyState{Exist: false}, nil
		}
		return policyToState(p), nil
	}

	if req.Name != "" {
		p, err := w.ClusterPolicies.GetByName(cmdCtx, req.Name)
		if err != nil {
			return ClusterPolicyState{Name: req.Name, Exist: false}, nil
		}
		return policyToState(p), nil
	}

	return ClusterPolicyState{Exist: false}, fmt.Errorf("policy_id or name must be provided")
}

func (h *ClusterPolicyHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[ClusterPolicySchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("policy_id or name", req.PolicyID, req.Name); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *ClusterPolicyHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ClusterPolicySchemaInput](input)
	if err != nil {
		return nil, err
	}
	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	// For create, name is required. For update, fall back to the server-held name.
	effectiveName := schemaInput.Name
	if effectiveName == "" {
		effectiveName = beforeState.Name
	}
	if !beforeState.Exist {
		if err := dsc.ValidateRequired(dsc.RequiredField{Name: "name", Value: effectiveName}); err != nil {
			return nil, err
		}
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var afterState ClusterPolicyState

	if beforeState.Exist {
		// Policy exists — edit it.
		if err := w.ClusterPolicies.Edit(cmdCtx, compute.EditPolicy{
			PolicyId:                       beforeState.PolicyID,
			Name:                           effectiveName,
			Definition:                     schemaInput.Definition,
			Description:                    schemaInput.Description,
			MaxClustersPerUser:             schemaInput.MaxClustersPerUser,
			PolicyFamilyId:                 schemaInput.PolicyFamilyID,
			PolicyFamilyDefinitionOverrides: schemaInput.PolicyFamilyDefinitionOverrides,
		}); err != nil {
			return nil, fmt.Errorf("failed to update cluster policy: %w", err)
		}
		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.ClusterPolicies.Get(cmdCtx, compute.GetClusterPolicyRequest{PolicyId: beforeState.PolicyID})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve updated cluster policy: %w", err)
		}
		afterState = policyToState(updated)
	} else {
		// Policy does not exist — create it.
		resp, err := w.ClusterPolicies.Create(cmdCtx, compute.CreatePolicy{
			Name:                           effectiveName,
			Definition:                     schemaInput.Definition,
			Description:                    schemaInput.Description,
			MaxClustersPerUser:             schemaInput.MaxClustersPerUser,
			PolicyFamilyId:                 schemaInput.PolicyFamilyID,
			PolicyFamilyDefinitionOverrides: schemaInput.PolicyFamilyDefinitionOverrides,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create cluster policy: %w", err)
		}
		// Fetch the full policy using the new ID (direct GET — strongly consistent).
		created, err := w.ClusterPolicies.Get(cmdCtx, compute.GetClusterPolicyRequest{PolicyId: resp.PolicyId})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve created cluster policy: %w", err)
		}
		afterState = policyToState(created)
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *ClusterPolicyHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ClusterPolicySchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := ClusterPolicyState{
		PolicyID:                       schemaInput.PolicyID,
		Name:                           schemaInput.Name,
		Definition:                     schemaInput.Definition,
		Description:                    schemaInput.Description,
		MaxClustersPerUser:             schemaInput.MaxClustersPerUser,
		PolicyFamilyID:                 schemaInput.PolicyFamilyID,
		PolicyFamilyDefinitionOverrides: schemaInput.PolicyFamilyDefinitionOverrides,
		Exist:                          true,
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

func (h *ClusterPolicyHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[ClusterPolicySchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	if schemaInput.PolicyID != "" {
		return w.ClusterPolicies.Delete(cmdCtx, compute.DeletePolicy{PolicyId: schemaInput.PolicyID})
	}

	if schemaInput.Name != "" {
		p, err := w.ClusterPolicies.GetByName(cmdCtx, schemaInput.Name)
		if err != nil {
			return dsc.NotFoundError("cluster policy", "name="+schemaInput.Name)
		}
		return w.ClusterPolicies.Delete(cmdCtx, compute.DeletePolicy{PolicyId: p.PolicyId})
	}

	return dsc.ValidateRequired(dsc.RequiredField{Name: "policy_id or name", Value: ""})
}

func (h *ClusterPolicyHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	policies, err := w.ClusterPolicies.ListAll(cmdCtx, compute.ListClusterPoliciesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster policies: %w", err)
	}

	var allPolicies []any
	for i := range policies {
		// Skip built-in default policies — they cannot be created or deleted.
		if policies[i].IsDefault {
			continue
		}
		allPolicies = append(allPolicies, policyToState(&policies[i]))
	}

	return allPolicies, nil
}
