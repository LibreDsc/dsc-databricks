package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

// ServicePrincipalState represents the full state of a Databricks service principal.
type ServicePrincipalState struct {
	dsc.ExistProperty
	DisplayName   string             `json:"display_name" description:"Display name of the service principal."`
	ApplicationID string             `json:"application_id,omitempty" description:"UUID of the Azure app registration or identity relating to the service principal."`
	ExternalID    string             `json:"external_id,omitempty" description:"External ID reserved for future use."`
	ID            string             `json:"id,omitempty" description:"Databricks service principal ID."`
	Entitlements  []UserComplexValue `json:"entitlements,omitempty" description:"Entitlements assigned to the service principal."`
	Roles         []UserComplexValue `json:"roles,omitempty" description:"Corresponds to AWS instance profile/ARN role."`
	Active        bool               `json:"active" dsc:"optional" description:"If this service principal is active."`
}

func servicePrincipalConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/ServicePrincipal",
		Version:     "0.1.0",
		Description: "Manage Databricks service principals",
		Tags:        []string{"databricks", "serviceprincipal", "iam", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks service principals.",
			AllowAdditionalProperties: true,
		},
	}
}

// ServicePrincipalHandler handles ServicePrincipal resource operations.
type ServicePrincipalHandler struct{}

func spToState(sp *iam.ServicePrincipal) ServicePrincipalState {
	state := ServicePrincipalState{
		DisplayName:   sp.DisplayName,
		ApplicationID: sp.ApplicationId,
		ExternalID:    sp.ExternalId,
		Entitlements:  fromIamComplexValues(sp.Entitlements),
		Roles:         fromIamComplexValues(sp.Roles),
		ID:            sp.Id,
		Active:        sp.Active,
	}
	state.SetExist(true)
	return state
}

func (h *ServicePrincipalHandler) Get(ctx context.Context, in ServicePrincipalState) (ServicePrincipalState, error) {
	if err := requireAtLeastOne("id or display_name", in.ID, in.DisplayName); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.ID != "" {
		logDebugf(MsgLookup, "ServicePrincipal", "id="+in.ID)
		sp, err := w.ServicePrincipalsV2.Get(ctx, iam.GetServicePrincipalRequest{Id: in.ID})
		if err != nil {
			logInfof(MsgNotFound, "ServicePrincipal", "id="+in.ID)
			return dsc.NotFound(ServicePrincipalState{ID: in.ID, DisplayName: in.DisplayName}, "ServicePrincipal", "id="+in.ID)
		}
		return spToState(sp), nil
	}

	logDebugf(MsgLookup, "ServicePrincipal", "display_name="+in.DisplayName)
	sps := w.ServicePrincipalsV2.List(ctx, iam.ListServicePrincipalsRequest{
		Filter: "displayName eq \"" + in.DisplayName + "\"",
	})

	sp, err := sps.Next(ctx)
	if err != nil {
		logInfof(MsgNotFound, "ServicePrincipal", "display_name="+in.DisplayName)
		return dsc.NotFound(ServicePrincipalState{DisplayName: in.DisplayName}, "ServicePrincipal", "display_name="+in.DisplayName)
	}
	return spToState(&sp), nil
}

func (h *ServicePrincipalHandler) Set(ctx context.Context, desired ServicePrincipalState) (ServicePrincipalState, error) {
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
		logInfof(MsgUpdate, "ServicePrincipal", "id="+current.ID)
		// Service principal already exists — GET the full object, overlay desired
		// fields, then PUT back via Update (SCIM PUT requires the complete representation).
		full, err := w.ServicePrincipalsV2.Get(ctx, iam.GetServicePrincipalRequest{Id: current.ID})
		if err != nil {
			return desired, fmt.Errorf("failed to get service principal for update: %w", err)
		}
		if desired.DisplayName != "" {
			full.DisplayName = desired.DisplayName
		}
		full.Active = desired.Active
		if desired.ApplicationID != "" {
			full.ApplicationId = desired.ApplicationID
		}
		if desired.ExternalID != "" {
			full.ExternalId = desired.ExternalID
		}
		if len(desired.Entitlements) > 0 {
			full.Entitlements = toIamComplexValues(desired.Entitlements)
		}
		if len(desired.Roles) > 0 {
			full.Roles = toIamComplexValues(desired.Roles)
		}
		if err := w.ServicePrincipalsV2.Update(ctx, iam.UpdateServicePrincipalRequest{
			Id:              full.Id,
			DisplayName:     full.DisplayName,
			Active:          full.Active,
			ApplicationId:   full.ApplicationId,
			ExternalId:      full.ExternalId,
			Entitlements:    full.Entitlements,
			Groups:          full.Groups,
			Roles:           full.Roles,
			Schemas:         full.Schemas,
			ForceSendFields: []string{"Active"},
		}); err != nil {
			return desired, fmt.Errorf("failed to update service principal: %w", err)
		}
		desired.ID = current.ID
	} else {
		logInfof(MsgCreate, "ServicePrincipal", "display_name="+desired.DisplayName)
		// Service principal does not exist — create it.
		if err := requireFields(field{"display_name", desired.DisplayName}); err != nil {
			return desired, err
		}
		created, err := w.ServicePrincipalsV2.Create(ctx, iam.CreateServicePrincipalRequest{
			DisplayName:   desired.DisplayName,
			Active:        desired.Active,
			ApplicationId: desired.ApplicationID,
			ExternalId:    desired.ExternalID,
			Entitlements:  toIamComplexValues(desired.Entitlements),
			Roles:         toIamComplexValues(desired.Roles),
		})
		if err != nil {
			return desired, fmt.Errorf("failed to create service principal: %w", err)
		}
		desired.ID = created.Id
	}

	return h.Get(ctx, desired)
}

// projectServicePrincipalUpdate mirrors Set's SCIM overlay: non-empty desired
// strings and non-empty slices win, active is always taken from desired (Set
// force-sends it), and the id carries over from the current state.
func projectServicePrincipalUpdate(desired, current *ServicePrincipalState) ServicePrincipalState {
	projected := *current
	if desired.DisplayName != "" {
		projected.DisplayName = desired.DisplayName
	}
	projected.Active = desired.Active
	if desired.ApplicationID != "" {
		projected.ApplicationID = desired.ApplicationID
	}
	if desired.ExternalID != "" {
		projected.ExternalID = desired.ExternalID
	}
	if len(desired.Entitlements) > 0 {
		projected.Entitlements = desired.Entitlements
	}
	if len(desired.Roles) > 0 {
		projected.Roles = desired.Roles
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the
// service principal.
func (h *ServicePrincipalHandler) SetWhatIf(ctx context.Context, desired ServicePrincipalState) (ServicePrincipalState, error) {
	if err := requireAtLeastOne("id or display_name", desired.ID, desired.DisplayName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "ServicePrincipal", "id="+current.ID)
		return projectServicePrincipalUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"display_name", desired.DisplayName}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "ServicePrincipal", "display_name="+desired.DisplayName)
	// The server-assigned id is unknown before creation.
	projected := desired
	projected.ID = ""
	projected.SetExist(true)
	return projected, nil
}

func (h *ServicePrincipalHandler) Test(ctx context.Context, desired ServicePrincipalState) (dsc.TestResult[ServicePrincipalState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[ServicePrincipalState]{}, err
	}

	result := dsc.TestResult[ServicePrincipalState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *ServicePrincipalHandler) Delete(ctx context.Context, in ServicePrincipalState) error {
	if err := requireAtLeastOne("id or display_name", in.ID, in.DisplayName); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	if in.ID != "" {
		logDebugf(MsgDelete, "ServicePrincipal", "id="+in.ID)
		return w.ServicePrincipalsV2.Delete(ctx, iam.DeleteServicePrincipalRequest{Id: in.ID})
	}

	logDebugf(MsgDelete, "ServicePrincipal", "display_name="+in.DisplayName)
	sps := w.ServicePrincipalsV2.List(ctx, iam.ListServicePrincipalsRequest{
		Filter: "displayName eq \"" + in.DisplayName + "\"",
	})

	sp, err := sps.Next(ctx)
	if err != nil {
		// Already absent — deleting a missing instance succeeds.
		logInfof(MsgNotFound, "ServicePrincipal", "display_name="+in.DisplayName)
		return nil
	}
	return w.ServicePrincipalsV2.Delete(ctx, iam.DeleteServicePrincipalRequest{Id: sp.Id})
}

func (h *ServicePrincipalHandler) Export(ctx context.Context, _ ServicePrincipalState) ([]ServicePrincipalState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "ServicePrincipal")
	sps, err := w.ServicePrincipalsV2.ListAll(ctx, iam.ListServicePrincipalsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list service principals: %w", err)
	}

	var allSPs []ServicePrincipalState
	for i := range sps {
		allSPs = append(allSPs, spToState(&sps[i]))
	}

	return allSPs, nil
}
