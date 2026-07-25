package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

// ClusterPolicyState represents the full state of a Databricks cluster policy.
// policy_id is computed on create; name is the human-readable identifier used
// for lookup — both are optional so inputs supplying only one of the two pass
// schema validation.
type ClusterPolicyState struct {
	dsc.ExistProperty
	Name                            string `json:"name,omitempty" description:"Cluster policy name. Must be unique. Length must be between 1 and 100 characters."`
	PolicyID                        string `json:"policy_id,omitempty" description:"Canonical unique identifier for the cluster policy. Computed on create."`
	Definition                      string `json:"definition,omitempty" description:"Policy definition document expressed in Databricks Cluster Policy Definition Language (JSON string)."`
	Description                     string `json:"description,omitempty" description:"Additional human-readable description of the cluster policy."`
	PolicyFamilyID                  string `json:"policy_family_id,omitempty" description:"ID of the policy family whose definition this policy inherits. Cannot be used with definition."`
	PolicyFamilyDefinitionOverrides string `json:"policy_family_definition_overrides,omitempty" description:"Policy definition JSON document to customize the inherited policy family definition."`
	MaxClustersPerUser              int64  `json:"max_clusters_per_user,omitempty" description:"Max number of clusters per user that can be active using this policy. If not present, there is no limit."`
}

func clusterPolicyConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/ClusterPolicy",
		Version:     "0.1.0",
		Description: "Manage Databricks cluster policies.",
		Tags:        []string{"databricks", "clusterpolicy", "compute"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks cluster policies.",
			AllowAdditionalProperties: true,
		},
	}
}

// ClusterPolicyHandler handles ClusterPolicy resource operations. definition is
// a JSON string; the server may normalise key ordering so the DSC synthetic
// test (raw value equality) is sufficient — no Testable is implemented.
type ClusterPolicyHandler struct{}

func policyToState(p *compute.Policy) ClusterPolicyState {
	state := ClusterPolicyState{
		PolicyID:                        p.PolicyId,
		Name:                            p.Name,
		Definition:                      p.Definition,
		Description:                     p.Description,
		MaxClustersPerUser:              p.MaxClustersPerUser,
		PolicyFamilyID:                  p.PolicyFamilyId,
		PolicyFamilyDefinitionOverrides: p.PolicyFamilyDefinitionOverrides,
	}
	state.SetExist(true)
	return state
}

func (h *ClusterPolicyHandler) Get(ctx context.Context, in ClusterPolicyState) (ClusterPolicyState, error) {
	if err := requireAtLeastOne("policy_id or name", in.PolicyID, in.Name); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.PolicyID != "" {
		logDebugf(MsgLookup, "ClusterPolicy", "policy_id="+in.PolicyID)
		p, err := w.ClusterPolicies.Get(ctx, compute.GetClusterPolicyRequest{PolicyId: in.PolicyID})
		if err != nil {
			logInfof(MsgNotFound, "ClusterPolicy", "policy_id="+in.PolicyID)
			return dsc.NotFound(ClusterPolicyState{PolicyID: in.PolicyID}, "ClusterPolicy", "policy_id="+in.PolicyID)
		}
		return policyToState(p), nil
	}

	logDebugf(MsgLookup, "ClusterPolicy", "name="+in.Name)
	p, err := w.ClusterPolicies.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "ClusterPolicy", "name="+in.Name)
		return dsc.NotFound(ClusterPolicyState{Name: in.Name}, "ClusterPolicy", "name="+in.Name)
	}
	return policyToState(p), nil
}

func (h *ClusterPolicyHandler) Set(ctx context.Context, desired ClusterPolicyState) (ClusterPolicyState, error) {
	if err := requireAtLeastOne("policy_id or name", desired.PolicyID, desired.Name); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	// For create, name is required. For update, fall back to the server-held name.
	effectiveName := desired.Name
	if effectiveName == "" {
		effectiveName = current.Name
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgUpdate, "ClusterPolicy", "policy_id="+current.PolicyID)
		if err := w.ClusterPolicies.Edit(ctx, compute.EditPolicy{
			PolicyId:                        current.PolicyID,
			Name:                            effectiveName,
			Definition:                      desired.Definition,
			Description:                     desired.Description,
			MaxClustersPerUser:              desired.MaxClustersPerUser,
			PolicyFamilyId:                  desired.PolicyFamilyID,
			PolicyFamilyDefinitionOverrides: desired.PolicyFamilyDefinitionOverrides,
		}); err != nil {
			return desired, fmt.Errorf("failed to update cluster policy: %w", err)
		}
		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.ClusterPolicies.Get(ctx, compute.GetClusterPolicyRequest{PolicyId: current.PolicyID})
		if err != nil {
			return desired, fmt.Errorf("failed to retrieve updated cluster policy: %w", err)
		}
		return policyToState(updated), nil
	}

	logInfof(MsgCreate, "ClusterPolicy", "name="+effectiveName)
	if err := requireFields(field{"name", effectiveName}); err != nil {
		return desired, err
	}
	resp, err := w.ClusterPolicies.Create(ctx, compute.CreatePolicy{
		Name:                            effectiveName,
		Definition:                      desired.Definition,
		Description:                     desired.Description,
		MaxClustersPerUser:              desired.MaxClustersPerUser,
		PolicyFamilyId:                  desired.PolicyFamilyID,
		PolicyFamilyDefinitionOverrides: desired.PolicyFamilyDefinitionOverrides,
	})
	if err != nil {
		return desired, fmt.Errorf("failed to create cluster policy: %w", err)
	}
	// Fetch the full policy using the new ID (direct GET — strongly consistent).
	created, err := w.ClusterPolicies.Get(ctx, compute.GetClusterPolicyRequest{PolicyId: resp.PolicyId})
	if err != nil {
		return desired, fmt.Errorf("failed to retrieve created cluster policy: %w", err)
	}
	return policyToState(created), nil
}

// projectClusterPolicyCreate returns the state Set's create path would
// produce. policy_id is computed by the server and stays empty.
func projectClusterPolicyCreate(desired *ClusterPolicyState, effectiveName string) ClusterPolicyState {
	projected := *desired
	projected.Name = effectiveName
	projected.PolicyID = ""
	projected.SetExist(true)
	return projected
}

// projectClusterPolicyUpdate mirrors compute.EditPolicy: the SDK omits empty
// values, so non-empty desired fields win and the rest carry over from the
// current state (which keeps policy_id).
func projectClusterPolicyUpdate(desired, current *ClusterPolicyState, effectiveName string) ClusterPolicyState {
	projected := *current
	projected.Name = effectiveName
	if desired.Definition != "" {
		projected.Definition = desired.Definition
	}
	if desired.Description != "" {
		projected.Description = desired.Description
	}
	if desired.PolicyFamilyID != "" {
		projected.PolicyFamilyID = desired.PolicyFamilyID
	}
	if desired.PolicyFamilyDefinitionOverrides != "" {
		projected.PolicyFamilyDefinitionOverrides = desired.PolicyFamilyDefinitionOverrides
	}
	if desired.MaxClustersPerUser > 0 {
		projected.MaxClustersPerUser = desired.MaxClustersPerUser
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the policy.
func (h *ClusterPolicyHandler) SetWhatIf(ctx context.Context, desired ClusterPolicyState) (ClusterPolicyState, error) {
	if err := requireAtLeastOne("policy_id or name", desired.PolicyID, desired.Name); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	effectiveName := desired.Name
	if effectiveName == "" {
		effectiveName = current.Name
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "ClusterPolicy", "policy_id="+current.PolicyID)
		return projectClusterPolicyUpdate(&desired, &current, effectiveName), nil
	}

	if err := requireFields(field{"name", effectiveName}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "ClusterPolicy", "name="+effectiveName)
	return projectClusterPolicyCreate(&desired, effectiveName), nil
}

func (h *ClusterPolicyHandler) Delete(ctx context.Context, in ClusterPolicyState) error {
	if err := requireAtLeastOne("policy_id or name", in.PolicyID, in.Name); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	if in.PolicyID != "" {
		logDebugf(MsgDelete, "ClusterPolicy", "policy_id="+in.PolicyID)
		return w.ClusterPolicies.Delete(ctx, compute.DeletePolicy{PolicyId: in.PolicyID})
	}

	logDebugf(MsgDelete, "ClusterPolicy", "name="+in.Name)
	p, err := w.ClusterPolicies.GetByName(ctx, in.Name)
	if err != nil {
		// Already absent — deleting a missing instance succeeds.
		logInfof(MsgNotFound, "ClusterPolicy", "name="+in.Name)
		return nil
	}
	return w.ClusterPolicies.Delete(ctx, compute.DeletePolicy{PolicyId: p.PolicyId})
}

func (h *ClusterPolicyHandler) Export(ctx context.Context, _ ClusterPolicyState) ([]ClusterPolicyState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "ClusterPolicy")
	policies, err := w.ClusterPolicies.ListAll(ctx, compute.ListClusterPoliciesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster policies: %w", err)
	}

	var allPolicies []ClusterPolicyState
	for i := range policies {
		// Skip built-in default policies — they cannot be created or deleted.
		if policies[i].IsDefault {
			continue
		}
		allPolicies = append(allPolicies, policyToState(&policies[i]))
	}

	return allPolicies, nil
}
