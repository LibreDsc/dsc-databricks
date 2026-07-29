package resources

import (
	"testing"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

func TestAmiStateToRequestDropsCredentialID(t *testing.T) {
	req := amiStateToRequest(&AzureManagedIdentityState{
		AccessConnectorID: "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac",
		ManagedIdentityID: "mi-id",
		CredentialID:      "server-side-id",
	})

	if req.AccessConnectorId == "" || req.ManagedIdentityId != "mi-id" {
		t.Errorf("fields not mapped: %+v", req)
	}
	// AzureManagedIdentityRequest has no credential_id field by design;
	// nothing to assert beyond the mapping above.

	if amiStateToRequest(nil) != nil {
		t.Error("nil input must map to nil")
	}
}

func TestAmiStateToResponseCarriesCurrentCredentialID(t *testing.T) {
	desired := &AzureManagedIdentityState{AccessConnectorID: "ac-id"}
	current := &AzureManagedIdentityState{AccessConnectorID: "ac-id", CredentialID: "cred-1"}

	resp := amiStateToResponse(desired, current)
	if resp.CredentialId != "cred-1" {
		t.Errorf("CredentialId = %q, want carried over from current", resp.CredentialId)
	}

	desired.CredentialID = "explicit"
	resp = amiStateToResponse(desired, current)
	if resp.CredentialId != "explicit" {
		t.Errorf("CredentialId = %q, want explicit desired value to win", resp.CredentialId)
	}

	if amiStateToResponse(nil, current) != nil {
		t.Error("nil desired must map to nil")
	}
}

func TestAspSdkToStateDropsClientSecret(t *testing.T) {
	state := aspSdkToState(&catalog.AzureServicePrincipal{
		ApplicationId: "app",
		DirectoryId:   "dir",
		ClientSecret:  "should-never-surface",
	})

	if state.ApplicationID != "app" || state.DirectoryID != "dir" {
		t.Errorf("fields not mapped: %+v", state)
	}
	if state.ClientSecret != "" {
		t.Error("client_secret must never be populated from API responses")
	}
}

func TestNormalizeStorageCredentialDesired(t *testing.T) {
	desired := StorageCredentialState{
		Name:           "cred",
		SkipValidation: true,
		AzureManagedIdentity: &AzureManagedIdentityState{
			AccessConnectorID: "ac-id",
		},
	}
	actual := StorageCredentialState{
		Name: "cred",
		AzureManagedIdentity: &AzureManagedIdentityState{
			AccessConnectorID: "ac-id",
			ManagedIdentityID: "system-mi",
			CredentialID:      "cred-1",
		},
	}

	norm := normalizeStorageCredentialDesired(&desired, &actual)

	if norm.SkipValidation {
		t.Error("skip_validation must be stripped (write-only toggle)")
	}
	if norm.AzureManagedIdentity.CredentialID != "cred-1" {
		t.Error("server-computed credential_id must derive from actual")
	}
	if norm.AzureManagedIdentity.ManagedIdentityID != "system-mi" {
		t.Error("system-assigned managed_identity_id must derive from actual")
	}
	if desired.AzureManagedIdentity.CredentialID != "" {
		t.Error("normalization must not mutate the desired state")
	}

	// The normalized desired must now compare clean against actual.
	if diff := dsc.CompareStates(norm, actual); len(diff) != 0 {
		t.Errorf("normalized compare = %v, want no drift", diff)
	}
}

func TestNormalizeStorageCredentialDesiredServicePrincipal(t *testing.T) {
	desired := StorageCredentialState{
		Name: "cred",
		AzureServicePrincipal: &AzureServicePrincipalState{
			ApplicationID: "app",
			DirectoryID:   "dir",
			ClientSecret:  "secret",
		},
	}
	actual := StorageCredentialState{
		Name: "cred",
		AzureServicePrincipal: &AzureServicePrincipalState{
			ApplicationID: "app",
			DirectoryID:   "dir",
		},
	}

	norm := normalizeStorageCredentialDesired(&desired, &actual)

	if norm.AzureServicePrincipal.ClientSecret != "" {
		t.Error("client_secret must be stripped before comparison")
	}
	if diff := dsc.CompareStates(norm, actual); len(diff) != 0 {
		t.Errorf("normalized compare = %v, want no drift", diff)
	}
}
