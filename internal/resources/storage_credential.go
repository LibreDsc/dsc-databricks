package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// AzureManagedIdentityState models an Azure managed identity credential block,
// shared by the StorageCredential and ServiceCredential resources.
type AzureManagedIdentityState struct {
	AccessConnectorID string `json:"access_connector_id" description:"Azure resource ID of the Azure Databricks Access Connector."`
	ManagedIdentityID string `json:"managed_identity_id,omitempty" description:"Azure resource ID of a user-assigned managed identity. Omit to use the connector's system-assigned identity."`
	CredentialID      string `json:"credential_id,omitempty" description:"Databricks internal ID of the managed identity credential. (read-only)"`
}

// AzureServicePrincipalState models an Azure service principal credential
// block. client_secret is write-only: the API never returns it, so drift on
// the secret cannot be detected.
type AzureServicePrincipalState struct {
	ApplicationID string `json:"application_id" description:"Application (client) ID of the Azure Active Directory application."`
	DirectoryID   string `json:"directory_id" description:"Directory (tenant) ID of the Azure Active Directory application."`
	ClientSecret  string `json:"client_secret,omitempty" description:"Client secret of the application. Write-only; never returned by the API."`
}

// StorageCredentialState represents the full state of a Unity Catalog storage
// credential. Renames are not modeled (rename = delete + create). Exactly one
// credential block (azure_managed_identity or azure_service_principal) must
// be provided when creating; AWS/GCP credential types are not modeled.
type StorageCredentialState struct {
	dsc.ExistProperty
	AzureManagedIdentity  *AzureManagedIdentityState  `json:"azure_managed_identity,omitempty" description:"Azure managed identity credential configuration."`
	AzureServicePrincipal *AzureServicePrincipalState `json:"azure_service_principal,omitempty" description:"Azure service principal credential configuration."`
	Name                  string                      `json:"name" description:"Name of the storage credential. Unique among storage and service credentials within the metastore."`
	Comment               string                      `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner                 string                      `json:"owner,omitempty" description:"Username of the current owner of the storage credential."`
	IsolationMode         string                      `json:"isolation_mode,omitempty" description:"Whether the credential is accessible from all workspaces or a specific set. Valid values: ISOLATION_MODE_ISOLATED, ISOLATION_MODE_OPEN." enum:"ISOLATION_MODE_ISOLATED,ISOLATION_MODE_OPEN"`
	ID                    string                      `json:"id,omitempty" description:"Unique identifier of the storage credential. (read-only)"`
	MetastoreID           string                      `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
	ReadOnly              bool                        `json:"read_only" dsc:"optional" description:"Whether the credential is usable only for read operations."`
	SkipValidation        bool                        `json:"skip_validation,omitempty" description:"Skip validation of the credential when creating or updating. Write-only behavior toggle."`
}

func storageCredentialConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/StorageCredential",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog storage credentials in a Databricks workspace.",
		Tags:        []string{"databricks", "storagecredential", "unitycatalog", "storage"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog storage credentials.",
			AllowAdditionalProperties: true,
		},
	}
}

// StorageCredentialHandler handles StorageCredential resource operations.
type StorageCredentialHandler struct{}

// amiStateToRequest converts the managed identity block for create requests,
// which take AzureManagedIdentityRequest (no credential_id).
func amiStateToRequest(s *AzureManagedIdentityState) *catalog.AzureManagedIdentityRequest {
	if s == nil {
		return nil
	}
	return &catalog.AzureManagedIdentityRequest{
		AccessConnectorId: s.AccessConnectorID,
		ManagedIdentityId: s.ManagedIdentityID,
	}
}

// amiStateToResponse converts the managed identity block for update requests,
// which take AzureManagedIdentityResponse. The read-only credential_id is
// carried over from the current state when the desired block omits it.
func amiStateToResponse(desired, current *AzureManagedIdentityState) *catalog.AzureManagedIdentityResponse {
	if desired == nil {
		return nil
	}
	resp := &catalog.AzureManagedIdentityResponse{
		AccessConnectorId: desired.AccessConnectorID,
		ManagedIdentityId: desired.ManagedIdentityID,
		CredentialId:      desired.CredentialID,
	}
	if resp.CredentialId == "" && current != nil {
		resp.CredentialId = current.CredentialID
	}
	return resp
}

func amiResponseToState(r *catalog.AzureManagedIdentityResponse) *AzureManagedIdentityState {
	if r == nil {
		return nil
	}
	return &AzureManagedIdentityState{
		AccessConnectorID: r.AccessConnectorId,
		ManagedIdentityID: r.ManagedIdentityId,
		CredentialID:      r.CredentialId,
	}
}

func aspStateToSdk(s *AzureServicePrincipalState) *catalog.AzureServicePrincipal {
	if s == nil {
		return nil
	}
	return &catalog.AzureServicePrincipal{
		ApplicationId: s.ApplicationID,
		DirectoryId:   s.DirectoryID,
		ClientSecret:  s.ClientSecret,
	}
}

func aspSdkToState(s *catalog.AzureServicePrincipal) *AzureServicePrincipalState {
	if s == nil {
		return nil
	}
	// client_secret is write-only and never returned; leave it empty.
	return &AzureServicePrincipalState{
		ApplicationID: s.ApplicationId,
		DirectoryID:   s.DirectoryId,
	}
}

func storageCredentialInfoToState(c *catalog.StorageCredentialInfo) StorageCredentialState {
	state := StorageCredentialState{
		Name:                  c.Name,
		Comment:               c.Comment,
		Owner:                 c.Owner,
		IsolationMode:         string(c.IsolationMode),
		ID:                    c.Id,
		MetastoreID:           c.MetastoreId,
		ReadOnly:              c.ReadOnly,
		AzureManagedIdentity:  amiResponseToState(c.AzureManagedIdentity),
		AzureServicePrincipal: aspSdkToState(c.AzureServicePrincipal),
	}
	state.SetExist(true)
	return state
}

func (h *StorageCredentialHandler) Get(ctx context.Context, in StorageCredentialState) (StorageCredentialState, error) {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "StorageCredential", "name="+in.Name)
	c, err := w.StorageCredentials.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "StorageCredential", "name="+in.Name)
		return dsc.NotFound(StorageCredentialState{Name: in.Name}, "StorageCredential", "name="+in.Name)
	}

	return storageCredentialInfoToState(c), nil
}

func (h *StorageCredentialHandler) Set(ctx context.Context, desired StorageCredentialState) (StorageCredentialState, error) {
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
		logInfof(MsgUpdate, "StorageCredential", "name="+desired.Name)
		updated, err := w.StorageCredentials.Update(ctx, catalog.UpdateStorageCredential{
			Name:                  desired.Name,
			Comment:               desired.Comment,
			Owner:                 desired.Owner,
			IsolationMode:         catalog.IsolationMode(desired.IsolationMode),
			ReadOnly:              desired.ReadOnly,
			SkipValidation:        desired.SkipValidation,
			AzureManagedIdentity:  amiStateToResponse(desired.AzureManagedIdentity, current.AzureManagedIdentity),
			AzureServicePrincipal: aspStateToSdk(desired.AzureServicePrincipal),
			ForceSendFields:       []string{"ReadOnly"},
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update storage credential: %w", err)
		}
		return storageCredentialInfoToState(updated), nil
	}

	if err := requireAtLeastOneCredentialBlock(desired.AzureManagedIdentity, desired.AzureServicePrincipal); err != nil {
		return desired, err
	}

	logInfof(MsgCreate, "StorageCredential", "name="+desired.Name)
	if _, err := w.StorageCredentials.Create(ctx, catalog.CreateStorageCredential{
		Name:                  desired.Name,
		Comment:               desired.Comment,
		ReadOnly:              desired.ReadOnly,
		SkipValidation:        desired.SkipValidation,
		AzureManagedIdentity:  amiStateToRequest(desired.AzureManagedIdentity),
		AzureServicePrincipal: aspStateToSdk(desired.AzureServicePrincipal),
		ForceSendFields:       []string{"ReadOnly"},
	}); err != nil {
		return desired, fmt.Errorf("failed to create storage credential: %w", err)
	}

	// Owner and isolation mode are not part of the create API; apply them
	// with a follow-up update when specified.
	if desired.Owner != "" || desired.IsolationMode != "" {
		if _, err := w.StorageCredentials.Update(ctx, catalog.UpdateStorageCredential{
			Name:          desired.Name,
			Owner:         desired.Owner,
			IsolationMode: catalog.IsolationMode(desired.IsolationMode),
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create storage credential settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// projectStorageCredentialCreate returns the state Set's create path would
// produce. id, metastore_id, and the nested credential_id are computed by the
// server and stay empty; client_secret is write-only and never appears in Get
// output, so it is omitted from the projection. Owner and isolation_mode are
// applied by the chained post-create update.
func projectStorageCredentialCreate(desired *StorageCredentialState) StorageCredentialState {
	projected := StorageCredentialState{
		Name:                  desired.Name,
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

// projectStorageCredentialUpdate mirrors catalog.UpdateStorageCredential:
// read_only is force-sent (desired always wins); other fields follow
// omit-empty semantics. The nested credential_id carries over from current.
func projectStorageCredentialUpdate(desired, current *StorageCredentialState) StorageCredentialState {
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

func cloneAmiWithoutCredentialID(s *AzureManagedIdentityState) *AzureManagedIdentityState {
	if s == nil {
		return nil
	}
	return &AzureManagedIdentityState{
		AccessConnectorID: s.AccessConnectorID,
		ManagedIdentityID: s.ManagedIdentityID,
	}
}

func cloneAspWithoutSecret(s *AzureServicePrincipalState) *AzureServicePrincipalState {
	if s == nil {
		return nil
	}
	return &AzureServicePrincipalState{
		ApplicationID: s.ApplicationID,
		DirectoryID:   s.DirectoryID,
	}
}

// SetWhatIf predicts the state Set would produce without touching the
// storage credential.
func (h *StorageCredentialHandler) SetWhatIf(ctx context.Context, desired StorageCredentialState) (StorageCredentialState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "StorageCredential", "name="+desired.Name)
		return projectStorageCredentialUpdate(&desired, &current), nil
	}

	if err := requireAtLeastOneCredentialBlock(desired.AzureManagedIdentity, desired.AzureServicePrincipal); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "StorageCredential", "name="+desired.Name)
	return projectStorageCredentialCreate(&desired), nil
}

// normalizeCredentialCompareState prepares a desired credential-style state
// for comparison against the actual state: skip_validation is a write-only
// behavior toggle, client_secret is never returned by the API, and the
// nested credential_id is server-computed, so all three would otherwise
// report permanent drift.
func normalizeStorageCredentialDesired(desired, actual *StorageCredentialState) StorageCredentialState {
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
// normalization: server-computed nested fields (credential_id,
// system-assigned managed_identity_id) are derived from the actual state and
// write-only fields (client_secret, skip_validation) are excluded.
func (h *StorageCredentialHandler) Test(ctx context.Context, desired StorageCredentialState) (dsc.TestResult[StorageCredentialState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[StorageCredentialState]{}, err
	}

	result := dsc.TestResult[StorageCredentialState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	normalized := normalizeStorageCredentialDesired(&desired, &actual)
	result.DifferingProperties = dsc.CompareStates(normalized, actual)
	return result, nil
}

func (h *StorageCredentialHandler) Delete(ctx context.Context, in StorageCredentialState) error {
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

	logDebugf(MsgDelete, "StorageCredential", "name="+in.Name)
	return w.StorageCredentials.Delete(ctx, catalog.DeleteStorageCredentialRequest{
		Name:  in.Name,
		Force: true,
	})
}

func (h *StorageCredentialHandler) Export(ctx context.Context, _ StorageCredentialState) ([]StorageCredentialState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "StorageCredential")
	credentials, err := w.StorageCredentials.ListAll(ctx, catalog.ListStorageCredentialsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list storage credentials: %w", err)
	}

	var all []StorageCredentialState
	for i := range credentials {
		all = append(all, storageCredentialInfoToState(&credentials[i]))
	}

	return all, nil
}
