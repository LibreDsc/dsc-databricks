package resources

import (
	"strings"

	dsc "github.com/LibreDsc/dsc-go-rdk"
)

// field pairs a property name with its value for validation.
type field struct {
	name  string
	value string
}

// requireFields returns an invalid-input error listing every empty field.
func requireFields(fields ...field) error {
	var missing []string
	for _, f := range fields {
		if f.value == "" {
			missing = append(missing, f.name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "missing required field(s): %s", strings.Join(missing, ", "))
}

// requireAtLeastOne returns an invalid-input error when all values are empty.
func requireAtLeastOne(description string, values ...string) error {
	for _, v := range values {
		if v != "" {
			return nil
		}
	}
	return dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "at least one of %s must be provided", description)
}

// requireAtLeastOneCredentialBlock returns an invalid-input error when no
// Azure credential block is provided on a credential-style resource create.
func requireAtLeastOneCredentialBlock(ami *AzureManagedIdentityState, asp *AzureServicePrincipalState) error {
	if ami == nil && asp == nil {
		return dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "at least one of azure_managed_identity or azure_service_principal must be provided")
	}
	return nil
}
