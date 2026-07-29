package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ServiceCredentialState represents the full state of a Unity Catalog
// credential (the unified credentials API, used for service credentials).
type ServiceCredentialState struct {
	dsc.ExistProperty
	AzureManagedIdentity  *AzureManagedIdentityState  `json:"azure_managed_identity,omitempty" description:"Azure managed identity credential configuration."`
	AzureServicePrincipal *AzureServicePrincipalState `json:"azure_service_principal,omitempty" description:"Azure service principal credential configuration."`
	Name                  string                      `json:"name" description:"Name of the credential. Unique among storage and service credentials within the metastore."`
	Purpose               string                      `json:"purpose,omitempty" description:"Purpose of the credential: SERVICE or STORAGE. Defaults to SERVICE when creating. (create-only)" enum:"SERVICE,STORAGE"`
	Comment               string                      `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner                 string                      `json:"owner,omitempty" description:"Username of the current owner of the credential."`
	IsolationMode         string                      `json:"isolation_mode,omitempty" description:"Whether the credential is accessible from all workspaces or a specific set. Valid values: ISOLATION_MODE_ISOLATED, ISOLATION_MODE_OPEN." enum:"ISOLATION_MODE_ISOLATED,ISOLATION_MODE_OPEN"`
	ID                    string                      `json:"id,omitempty" description:"Unique identifier of the credential. (read-only)"`
	MetastoreID           string                      `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
	ReadOnly              bool                        `json:"read_only" dsc:"optional" description:"Whether the credential is usable only for read operations. Only applicable when purpose is STORAGE."`
	SkipValidation        bool                        `json:"skip_validation,omitempty" description:"Skip validation of the credential when creating or updating. Write-only behavior toggle."`
}

func serviceCredentialConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/ServiceCredential",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog service credentials in a Databricks workspace.",
		Tags:        []string{"databricks", "servicecredential", "unitycatalog"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog service credentials.",
			AllowAdditionalProperties: true,
		},
	}
}

// ServiceCredentialHandler handles ServiceCredential resource operations.
type ServiceCredentialHandler struct{}

// The credentials API uses a single AzureManagedIdentity struct for create,
// update, and info payloads (unlike the storage credentials API).
func amiStateToSdk(s *AzureManagedIdentityState) *catalog.AzureManagedIdentity {
	if s == nil {
		return nil
	}
	return &catalog.AzureManagedIdentity{
		AccessConnectorId: s.AccessConnectorID,
		ManagedIdentityId: s.ManagedIdentityID,
		CredentialId:      s.CredentialID,
	}
}

func amiSdkToState(r *catalog.AzureManagedIdentity) *AzureManagedIdentityState {
	if r == nil {
		return nil
	}
	return &AzureManagedIdentityState{
		AccessConnectorID: r.AccessConnectorId,
		ManagedIdentityID: r.ManagedIdentityId,
		CredentialID:      r.CredentialId,
	}
}

func credentialInfoToState(c *catalog.CredentialInfo) ServiceCredentialState {
	state := ServiceCredentialState{
		Name:                  c.Name,
		Purpose:               string(c.Purpose),
		Comment:               c.Comment,
		Owner:                 c.Owner,
		IsolationMode:         string(c.IsolationMode),
		ID:                    c.Id,
		MetastoreID:           c.MetastoreId,
		ReadOnly:              c.ReadOnly,
		AzureManagedIdentity:  amiSdkToState(c.AzureManagedIdentity),
		AzureServicePrincipal: aspSdkToState(c.AzureServicePrincipal),
	}
	state.SetExist(true)
	return state
}

func (h *ServiceCredentialHandler) Get(ctx context.Context, in ServiceCredentialState) (ServiceCredentialState, error) {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "ServiceCredential", "name="+in.Name)
	c, err := w.Credentials.GetCredentialByNameArg(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "ServiceCredential", "name="+in.Name)
		return dsc.NotFound(ServiceCredentialState{Name: in.Name}, "ServiceCredential", "name="+in.Name)
	}

	return credentialInfoToState(c), nil
}

func (h *ServiceCredentialHandler) Set(ctx context.Context, desired ServiceCredentialState) (ServiceCredentialState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
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
		logInfof(MsgUpdate, "ServiceCredential", "name="+desired.Name)
		updated, err := w.Credentials.UpdateCredential(ctx, catalog.UpdateCredentialRequest{
			NameArg:               desired.Name,
			Comment:               desired.Comment,
			Owner:                 desired.Owner,
			IsolationMode:         catalog.IsolationMode(desired.IsolationMode),
			ReadOnly:              desired.ReadOnly,
			SkipValidation:        desired.SkipValidation,
			AzureManagedIdentity:  updateAmi(desired.AzureManagedIdentity, current.AzureManagedIdentity),
			AzureServicePrincipal: aspStateToSdk(desired.AzureServicePrincipal),
			ForceSendFields:       []string{"ReadOnly"},
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update service credential: %w", err)
		}
		return credentialInfoToState(updated), nil
	}

	if err := requireAtLeastOneCredentialBlock(desired.AzureManagedIdentity, desired.AzureServicePrincipal); err != nil {
		return desired, err
	}

	purpose := desired.Purpose
	if purpose == "" {
		purpose = string(catalog.CredentialPurposeService)
	}

	logInfof(MsgCreate, "ServiceCredential", "name="+desired.Name)
	if _, err := w.Credentials.CreateCredential(ctx, catalog.CreateCredentialRequest{
		Name:                  desired.Name,
		Purpose:               catalog.CredentialPurpose(purpose),
		Comment:               desired.Comment,
		ReadOnly:              desired.ReadOnly,
		SkipValidation:        desired.SkipValidation,
		AzureManagedIdentity:  amiStateToSdk(desired.AzureManagedIdentity),
		AzureServicePrincipal: aspStateToSdk(desired.AzureServicePrincipal),
		ForceSendFields:       []string{"ReadOnly"},
	}); err != nil {
		return desired, fmt.Errorf("failed to create service credential: %w", err)
	}

	// Owner and isolation mode are not part of the create API; apply them
	// with a follow-up update when specified.
	if desired.Owner != "" || desired.IsolationMode != "" {
		if _, err := w.Credentials.UpdateCredential(ctx, catalog.UpdateCredentialRequest{
			NameArg:       desired.Name,
			Owner:         desired.Owner,
			IsolationMode: catalog.IsolationMode(desired.IsolationMode),
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create service credential settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// updateAmi builds the managed identity block for update requests, carrying
// the read-only credential_id over from the current state when the desired
// block omits it.
func updateAmi(desired, current *AzureManagedIdentityState) *catalog.AzureManagedIdentity {
	if desired == nil {
		return nil
	}
	ami := amiStateToSdk(desired)
	if ami.CredentialId == "" && current != nil {
		ami.CredentialId = current.CredentialID
	}
	return ami
}

// projectServiceCredentialCreate returns the state Set's create path would
// produce. id, metastore_id, and the nested credential_id stay empty;
// client_secret is write-only and omitted; purpose defaults to SERVICE.
// Owner and isolation_mode are applied by the chained post-create update.
func projectServiceCredentialCreate(desired *ServiceCredentialState) ServiceCredentialState {
	purpose := desired.Purpose
	if purpose == "" {
		purpose = string(catalog.CredentialPurposeService)
	}
	projected := ServiceCredentialState{
		Name:                  desired.Name,
		Purpose:               purpose,
		Comment:               desired.Comment,
		Owner:                 desired.Owner,
		IsolationMode:         desired.IsolationMode,
		ReadOnly:              desired.ReadOnly,
		AzureManagedIdentity:  cloneAmiWithoutCredentialID(desired.AzureManagedIdentity),
		AzureServicePrincipal: cloneAspWithoutSecret(desired.AzureServicePrincipal),
	}
	projected.SetExist(true)
	return projected
}

// projectServiceCredentialUpdate mirrors catalog.UpdateCredentialRequest:
// read_only is force-sent (desired always wins); other fields follow
// omit-empty semantics. purpose is create-only and carries over from current.
func projectServiceCredentialUpdate(desired, current *ServiceCredentialState) ServiceCredentialState {
	projected := *current
	projected.ReadOnly = desired.ReadOnly
	if desired.Comment != "" {
		projected.Comment = desired.Comment
	}
	if desired.Owner != "" {
		projected.Owner = desired.Owner
	}
	if desired.IsolationMode != "" {
		projected.IsolationMode = desired.IsolationMode
	}
	if desired.AzureManagedIdentity != nil {
		ami := *desired.AzureManagedIdentity
		if ami.CredentialID == "" && current.AzureManagedIdentity != nil {
			ami.CredentialID = current.AzureManagedIdentity.CredentialID
		}
		projected.AzureManagedIdentity = &ami
		projected.AzureServicePrincipal = nil
	}
	if desired.AzureServicePrincipal != nil {
		projected.AzureServicePrincipal = cloneAspWithoutSecret(desired.AzureServicePrincipal)
		projected.AzureManagedIdentity = nil
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the
// credential.
func (h *ServiceCredentialHandler) SetWhatIf(ctx context.Context, desired ServiceCredentialState) (ServiceCredentialState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "ServiceCredential", "name="+desired.Name)
		return projectServiceCredentialUpdate(&desired, &current), nil
	}

	if err := requireAtLeastOneCredentialBlock(desired.AzureManagedIdentity, desired.AzureServicePrincipal); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "ServiceCredential", "name="+desired.Name)
	return projectServiceCredentialCreate(&desired), nil
}

// normalizeServiceCredentialDesired mirrors the StorageCredential
// normalization: write-only fields are excluded and server-computed nested
// fields derive from the actual state.
func normalizeServiceCredentialDesired(desired, actual *ServiceCredentialState) ServiceCredentialState {
	norm := *desired
	norm.SkipValidation = false
	norm.AzureServicePrincipal = cloneAspWithoutSecret(norm.AzureServicePrincipal)
	if norm.AzureManagedIdentity != nil {
		ami := *norm.AzureManagedIdentity
		if actual.AzureManagedIdentity != nil {
			if ami.CredentialID == "" {
				ami.CredentialID = actual.AzureManagedIdentity.CredentialID
			}
			if ami.ManagedIdentityID == "" {
				ami.ManagedIdentityID = actual.AzureManagedIdentity.ManagedIdentityID
			}
		}
		norm.AzureManagedIdentity = &ami
	}
	return norm
}

// Test compares desired against actual with credential-specific
// normalization (see normalizeServiceCredentialDesired).
func (h *ServiceCredentialHandler) Test(ctx context.Context, desired ServiceCredentialState) (dsc.TestResult[ServiceCredentialState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[ServiceCredentialState]{}, err
	}

	result := dsc.TestResult[ServiceCredentialState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	normalized := normalizeServiceCredentialDesired(&desired, &actual)
	result.DifferingProperties = dsc.CompareStates(normalized, actual)
	return result, nil
}

func (h *ServiceCredentialHandler) Delete(ctx context.Context, in ServiceCredentialState) error {
	if err := requireFields(field{"name", in.Name}); err != nil {
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

	logDebugf(MsgDelete, "ServiceCredential", "name="+in.Name)
	return w.Credentials.DeleteCredential(ctx, catalog.DeleteCredentialRequest{
		NameArg: in.Name,
		Force:   true,
	})
}

func (h *ServiceCredentialHandler) Export(ctx context.Context, _ ServiceCredentialState) ([]ServiceCredentialState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "ServiceCredential")
	credentials, err := w.Credentials.ListCredentialsAll(ctx, catalog.ListCredentialsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list service credentials: %w", err)
	}

	var all []ServiceCredentialState
	for i := range credentials {
		all = append(all, credentialInfoToState(&credentials[i]))
	}

	return all, nil
}
