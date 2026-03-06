package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/ServicePrincipal", &ServicePrincipalHandler{}, servicePrincipalMetadata())
}

// ServicePrincipal property descriptions from SDK documentation.
var servicePrincipalPropertyDescriptions = dsc.PropertyDescriptions{
	"id":             "Databricks service principal ID.",
	"application_id": "UUID of the Azure app registration or identity relating to the service principal.",
	"display_name":   "Display name of the service principal.",
	"active":         "If this service principal is active.",
	"external_id":    "External ID reserved for future use.",
	"entitlements":   "Entitlements assigned to the service principal.",
	"roles":          "Corresponds to AWS instance profile/ARN role.",
}

// ServicePrincipalSchemaInput defines the desired-state fields for the schema
// and for unmarshaling input. id is computed on create so it carries omitempty.
type ServicePrincipalSchemaInput struct {
	DisplayName   string             `json:"display_name"`
	ApplicationID string             `json:"application_id,omitempty"`
	ExternalID    string             `json:"external_id,omitempty"`
	ID            string             `json:"id,omitempty"`
	Entitlements  []UserComplexValue `json:"entitlements,omitempty"`
	Roles         []UserComplexValue `json:"roles,omitempty"`
	Active        bool               `json:"active,omitempty"`
}

func servicePrincipalMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/ServicePrincipal",
		Description:       "Manage Databricks service principals",
		SchemaDescription: "Schema for managing Databricks service principals.",
		ResourceName:      "service principal",
		Tags:              []string{"databricks", "serviceprincipal", "iam", "workspace"},
		Descriptions:      servicePrincipalPropertyDescriptions,
		SchemaType:        reflect.TypeFor[ServicePrincipalSchemaInput](),
	})
}

// ServicePrincipalState represents the full state of a Databricks service principal.
type ServicePrincipalState struct {
	DisplayName   string             `json:"display_name"`
	ApplicationID string             `json:"application_id,omitempty"`
	ExternalID    string             `json:"external_id,omitempty"`
	ID            string             `json:"id,omitempty"`
	Entitlements  []UserComplexValue `json:"entitlements,omitempty"`
	Roles         []UserComplexValue `json:"roles,omitempty"`
	Active        bool               `json:"active"`
	Exist         bool               `json:"_exist"`
}

// ServicePrincipalHandler handles ServicePrincipal resource operations.
type ServicePrincipalHandler struct{}

func spToState(sp *iam.ServicePrincipal) ServicePrincipalState {
	return ServicePrincipalState{
		DisplayName:   sp.DisplayName,
		ApplicationID: sp.ApplicationId,
		ExternalID:    sp.ExternalId,
		Entitlements:  fromIamComplexValues(sp.Entitlements),
		Roles:         fromIamComplexValues(sp.Roles),
		ID:            sp.Id,
		Active:        sp.Active,
		Exist:         true,
	}
}

func (h *ServicePrincipalHandler) getCurrentState(ctx dsc.ResourceContext, req *ServicePrincipalSchemaInput) (ServicePrincipalState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return ServicePrincipalState{Exist: false}, err
	}

	if req.ID != "" {
		sp, err := w.ServicePrincipalsV2.Get(cmdCtx, iam.GetServicePrincipalRequest{Id: req.ID})
		if err != nil {
			return ServicePrincipalState{Exist: false}, nil
		}
		return spToState(sp), nil
	}

	if req.DisplayName != "" {
		sps := w.ServicePrincipalsV2.List(cmdCtx, iam.ListServicePrincipalsRequest{
			Filter: "displayName eq \"" + req.DisplayName + "\"",
		})

		sp, err := sps.Next(cmdCtx)
		if err != nil {
			return ServicePrincipalState{DisplayName: req.DisplayName, Exist: false}, nil
		}
		return spToState(&sp), nil
	}

	return ServicePrincipalState{Exist: false}, fmt.Errorf("id or display_name must be provided")
}

func (h *ServicePrincipalHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[ServicePrincipalSchemaInput](input)
	if err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *ServicePrincipalHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ServicePrincipalSchemaInput](input)
	if err != nil {
		return nil, err
	}

	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if beforeState.Exist {
		// Service principal already exists — GET the full object, overlay desired
		// fields, then PUT back via Update (SCIM PUT requires the complete representation).
		full, err := w.ServicePrincipalsV2.Get(cmdCtx, iam.GetServicePrincipalRequest{Id: beforeState.ID})
		if err != nil {
			return nil, fmt.Errorf("failed to get service principal for update: %w", err)
		}
		if schemaInput.DisplayName != "" {
			full.DisplayName = schemaInput.DisplayName
		}
		full.Active = schemaInput.Active
		if schemaInput.ApplicationID != "" {
			full.ApplicationId = schemaInput.ApplicationID
		}
		if schemaInput.ExternalID != "" {
			full.ExternalId = schemaInput.ExternalID
		}
		if len(schemaInput.Entitlements) > 0 {
			full.Entitlements = toIamComplexValues(schemaInput.Entitlements)
		}
		if len(schemaInput.Roles) > 0 {
			full.Roles = toIamComplexValues(schemaInput.Roles)
		}
		if err := w.ServicePrincipalsV2.Update(cmdCtx, iam.UpdateServicePrincipalRequest{
			Id:           full.Id,
			DisplayName:  full.DisplayName,
			Active:       full.Active,
			ApplicationId: full.ApplicationId,
			ExternalId:   full.ExternalId,
			Entitlements: full.Entitlements,
			Groups:       full.Groups,
			Roles:        full.Roles,
			Schemas:      full.Schemas,
		}); err != nil {
			return nil, fmt.Errorf("failed to update service principal: %w", err)
		}
		schemaInput.ID = beforeState.ID
	} else if schemaInput.DisplayName != "" {
		// Service principal does not exist — create it.
		created, err := w.ServicePrincipalsV2.Create(cmdCtx, iam.CreateServicePrincipalRequest{
			DisplayName:   schemaInput.DisplayName,
			Active:        schemaInput.Active,
			ApplicationId: schemaInput.ApplicationID,
			ExternalId:    schemaInput.ExternalID,
			Entitlements:  toIamComplexValues(schemaInput.Entitlements),
			Roles:         toIamComplexValues(schemaInput.Roles),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create service principal: %w", err)
		}
		schemaInput.ID = created.Id
	} else {
		return nil, dsc.ValidateRequired(dsc.RequiredField{Name: "display_name", Value: ""})
	}

	afterState, _ := h.getCurrentState(ctx, &schemaInput)
	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *ServicePrincipalHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ServicePrincipalSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := ServicePrincipalState{
		ID:           schemaInput.ID,
		DisplayName:  schemaInput.DisplayName,
		ApplicationID: schemaInput.ApplicationID,
		ExternalID:   schemaInput.ExternalID,
		Active:       schemaInput.Active,
		Entitlements: schemaInput.Entitlements,
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

func (h *ServicePrincipalHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[ServicePrincipalSchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	if schemaInput.ID != "" {
		return w.ServicePrincipalsV2.Delete(cmdCtx, iam.DeleteServicePrincipalRequest{Id: schemaInput.ID})
	}

	if schemaInput.DisplayName != "" {
		sps := w.ServicePrincipalsV2.List(cmdCtx, iam.ListServicePrincipalsRequest{
			Filter: "displayName eq \"" + schemaInput.DisplayName + "\"",
		})

		sp, err := sps.Next(cmdCtx)
		if err != nil {
			return dsc.NotFoundError("service principal", "display_name="+schemaInput.DisplayName)
		}
		return w.ServicePrincipalsV2.Delete(cmdCtx, iam.DeleteServicePrincipalRequest{Id: sp.Id})
	}

	return dsc.ValidateRequired(dsc.RequiredField{Name: "id or display_name", Value: ""})
}

func (h *ServicePrincipalHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var allSPs []any

	sps := w.ServicePrincipalsV2.List(cmdCtx, iam.ListServicePrincipalsRequest{})
	for {
		sp, err := sps.Next(cmdCtx)
		if err != nil {
			break
		}
		allSPs = append(allSPs, spToState(&sp))
	}

	return allSPs, nil
}
