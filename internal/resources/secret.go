package resources

import (
	"encoding/json"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Secret", &SecretHandler{}, secretMetadata())
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/SecretScope", &SecretScopeHandler{}, secretScopeMetadata())
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/SecretAcl", &SecretAclHandler{}, secretAclMetadata())
}

// Secret property descriptions from SDK documentation.
var secretPropertyDescriptions = dsc.PropertyDescriptions{
	"key":          "A unique name to identify the secret.",
	"scope":        "The name of the scope to which the secret will be associated with.",
	"string_value": "If specified, note that the value will be stored in UTF-8 (MB4) form.",
	"bytes_value":  "If specified, value will be stored as bytes.",
}

var secretScopePropertyDescriptions = dsc.PropertyDescriptions{
	"scope":                    "A unique name to identify the scope.",
	"initial_manage_principal": "The principal that is initially granted MANAGE permission to the created scope.",
	"scope_backend_type":       "The backend type the scope will be created with (DATABRICKS or AZURE_KEYVAULT).",
	"backend_azure_keyvault":   "The metadata for the Azure KeyVault if using Azure-backed secret scope.",
}

var secretAclPropertyDescriptions = dsc.PropertyDescriptions{
	"scope":      "The name of the scope to apply permissions to.",
	"principal":  "The principal (user or group) to apply permissions to.",
	"permission": "The permission level applied to the principal (READ, WRITE, or MANAGE).",
}

func secretMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/Secret",
		Description:       "Manage Databricks secrets",
		SchemaDescription: "Schema for managing Databricks secrets.",
		ResourceName:      "secret",
		Tags:              []string{"databricks", "secret", "workspace"},
		Descriptions:      secretPropertyDescriptions,
		SchemaType:        reflect.TypeFor[workspace.PutSecret](),
		// Secret only exposes scope, key, and _exist. Value equality is
		// sufficient — the Databricks API never returns the secret value,
		// so a custom test method adds no benefit over the synthetic one.
		OmitTest: true,
	})
}

// SecretScopeSchemaInput is used for JSON schema generation only.
// Unlike workspace.CreateScope which uses scope_backend_type, we expose
// backend_type to match the state output and keep the schema consistent.
type SecretScopeSchemaInput struct {
	Scope       string `json:"scope"`
	BackendType string `json:"backend_type,omitempty"`
}

func secretScopeMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/SecretScope",
		Description:       "Manage Databricks secret scopes",
		SchemaDescription: "Schema for managing Databricks secret scopes.",
		ResourceName:      "secret scope",
		Tags:              []string{"databricks", "secret", "scope", "workspace"},
		Descriptions:      secretScopePropertyDescriptions,
		SchemaType:        reflect.TypeFor[SecretScopeSchemaInput](),
	})
}

// SecretAclSchemaInput is used for JSON schema generation only.
// Unlike workspace.PutAcl, permission is optional here because get and delete
// operations only require scope and principal.
type SecretAclSchemaInput struct {
	Permission string `json:"permission,omitempty"`
	Principal  string `json:"principal"`
	Scope      string `json:"scope"`
}

func secretAclMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/SecretAcl",
		Description:       "Manage Databricks secret ACLs",
		SchemaDescription: "Schema for managing Databricks secret ACLs.",
		ResourceName:      "secret ACL",
		Tags:              []string{"databricks", "secret", "acl", "permissions", "workspace"},
		Descriptions:      secretAclPropertyDescriptions,
		SchemaType:        reflect.TypeFor[SecretAclSchemaInput](),
	})
}

// SecretState represents the state of a Databricks secret.
type SecretState struct {
	Scope string `json:"scope"`
	Key   string `json:"key"`
	Exist bool   `json:"_exist"`
}

// SecretHandler handles Secret resource operations.
type SecretHandler struct{}

func (h *SecretHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutSecret](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "key", Value: req.Key},
	); err != nil {
		return nil, err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	secrets := w.Secrets.ListSecrets(cmdCtx, workspace.ListSecretsRequest{Scope: req.Scope})
	for {
		secret, err := secrets.Next(cmdCtx)
		if err != nil {
			break
		}
		if secret.Key == req.Key {
			return &dsc.GetResult{ActualState: SecretState{Scope: req.Scope, Key: req.Key, Exist: true}}, nil
		}
	}
	return &dsc.GetResult{ActualState: SecretState{Scope: req.Scope, Key: req.Key, Exist: false}}, nil
}

func (h *SecretHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutSecret](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "key", Value: req.Key},
	); err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("string_value or bytes_value", req.StringValue, req.BytesValue); err != nil {
		return nil, err
	}

	beforeState := SecretState{Scope: req.Scope, Key: req.Key, Exist: false}
	beforeResult, _ := h.Get(ctx, input)
	if beforeResult != nil {
		if s, ok := beforeResult.ActualState.(SecretState); ok {
			beforeState = s
		}
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if err := w.Secrets.PutSecret(cmdCtx, req); err != nil {
		return nil, err
	}

	afterState := SecretState{Scope: req.Scope, Key: req.Key, Exist: true}
	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *SecretHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutSecret](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "key", Value: req.Key},
	); err != nil {
		return nil, err
	}

	result, err := h.Get(ctx, input)
	if err != nil {
		return nil, err
	}

	actualState := result.ActualState
	desiredState := SecretState{Scope: req.Scope, Key: req.Key, Exist: true}

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *SecretHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[workspace.DeleteSecret](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "key", Value: req.Key},
	); err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	return w.Secrets.DeleteSecret(cmdCtx, req)
}

func (h *SecretHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var allSecrets []any

	scopes := w.Secrets.ListScopes(cmdCtx)
	for {
		scope, err := scopes.Next(cmdCtx)
		if err != nil {
			break
		}

		secrets := w.Secrets.ListSecrets(cmdCtx, workspace.ListSecretsRequest{Scope: scope.Name})
		for {
			secret, err := secrets.Next(cmdCtx)
			if err != nil {
				break
			}
			allSecrets = append(allSecrets, SecretState{
				Scope: scope.Name,
				Key:   secret.Key,
				Exist: true,
			})
		}
	}

	return allSecrets, nil
}

// SecretScopeState represents the state of a Databricks secret scope.
type SecretScopeState struct {
	Scope       string `json:"scope"`
	BackendType string `json:"backend_type"`
	Exist       bool   `json:"_exist"`
}

// SecretScopeHandler handles SecretScope resource operations.
type SecretScopeHandler struct{}

func (h *SecretScopeHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.CreateScope](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "scope", Value: req.Scope}); err != nil {
		return nil, err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	scopes := w.Secrets.ListScopes(cmdCtx)
	for {
		scope, err := scopes.Next(cmdCtx)
		if err != nil {
			break
		}
		if scope.Name == req.Scope {
			return &dsc.GetResult{ActualState: SecretScopeState{
				Scope:       scope.Name,
				BackendType: scope.BackendType.String(),
				Exist:       true,
			}}, nil
		}
	}
	return &dsc.GetResult{ActualState: SecretScopeState{Scope: req.Scope, Exist: false}}, nil
}

func (h *SecretScopeHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.CreateScope](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "scope", Value: req.Scope}); err != nil {
		return nil, err
	}

	beforeResult, _ := h.Get(ctx, input)
	var beforeState SecretScopeState
	if beforeResult != nil {
		if s, ok := beforeResult.ActualState.(SecretScopeState); ok {
			beforeState = s
		}
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if !beforeState.Exist {
		if err := w.Secrets.CreateScope(cmdCtx, req); err != nil {
			return nil, err
		}
	}

	afterResult, _ := h.Get(ctx, input)
	var afterState SecretScopeState
	if afterResult != nil {
		if s, ok := afterResult.ActualState.(SecretScopeState); ok {
			afterState = s
		}
	}

	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *SecretScopeHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[SecretScopeState](input)
	if err != nil {
		return nil, err
	}

	result, err := h.Get(ctx, input)
	if err != nil {
		return nil, err
	}

	actualState := result.ActualState
	desiredState := SecretScopeState{Scope: req.Scope, BackendType: req.BackendType, Exist: true}

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *SecretScopeHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[workspace.DeleteScope](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "scope", Value: req.Scope}); err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	return w.Secrets.DeleteScope(cmdCtx, req)
}

func (h *SecretScopeHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var allScopes []any

	scopes := w.Secrets.ListScopes(cmdCtx)
	for {
		scope, err := scopes.Next(cmdCtx)
		if err != nil {
			break
		}
		allScopes = append(allScopes, SecretScopeState{
			Scope:       scope.Name,
			BackendType: scope.BackendType.String(),
			Exist:       true,
		})
	}

	return allScopes, nil
}

// SecretAclState represents the state of a Databricks secret ACL.
type SecretAclState struct {
	Scope      string `json:"scope"`
	Principal  string `json:"principal"`
	Permission string `json:"permission"`
	Exist      bool   `json:"_exist"`
}

// SecretAclHandler handles SecretAcl resource operations.
type SecretAclHandler struct{}

func (h *SecretAclHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutAcl](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "principal", Value: req.Principal},
	); err != nil {
		return nil, err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	acl, err := w.Secrets.GetAcl(cmdCtx, workspace.GetAclRequest{Scope: req.Scope, Principal: req.Principal})
	if err != nil {
		return &dsc.GetResult{ActualState: SecretAclState{
			Scope:     req.Scope,
			Principal: req.Principal,
			Exist:     false,
		}}, nil
	}

	return &dsc.GetResult{ActualState: SecretAclState{
		Scope:      req.Scope,
		Principal:  acl.Principal,
		Permission: acl.Permission.String(),
		Exist:      true,
	}}, nil
}

func (h *SecretAclHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutAcl](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "principal", Value: req.Principal},
		dsc.RequiredField{Name: "permission", Value: string(req.Permission)},
	); err != nil {
		return nil, err
	}

	getInput, _ := json.Marshal(map[string]string{"scope": req.Scope, "principal": req.Principal})
	beforeResult, _ := h.Get(ctx, getInput)
	var beforeState SecretAclState
	if beforeResult != nil {
		if s, ok := beforeResult.ActualState.(SecretAclState); ok {
			beforeState = s
		}
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if err := w.Secrets.PutAcl(cmdCtx, req); err != nil {
		return nil, err
	}

	afterResult, _ := h.Get(ctx, getInput)
	var afterState SecretAclState
	if afterResult != nil {
		if s, ok := afterResult.ActualState.(SecretAclState); ok {
			afterState = s
		}
	}

	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *SecretAclHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[workspace.PutAcl](input)
	if err != nil {
		return nil, err
	}

	getInput, _ := json.Marshal(map[string]string{"scope": req.Scope, "principal": req.Principal})
	result, err := h.Get(ctx, getInput)
	if err != nil {
		return nil, err
	}

	actualState := result.ActualState
	desiredState := SecretAclState{
		Scope:      req.Scope,
		Principal:  req.Principal,
		Permission: string(req.Permission),
		Exist:      true,
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

func (h *SecretAclHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[workspace.PutAcl](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "scope", Value: req.Scope},
		dsc.RequiredField{Name: "principal", Value: req.Principal},
	); err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	return w.Secrets.DeleteAcl(cmdCtx, workspace.DeleteAcl{Scope: req.Scope, Principal: req.Principal})
}

func (h *SecretAclHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var allAcls []any

	scopes := w.Secrets.ListScopes(cmdCtx)
	for {
		scope, err := scopes.Next(cmdCtx)
		if err != nil {
			break
		}

		acls := w.Secrets.ListAcls(cmdCtx, workspace.ListAclsRequest{Scope: scope.Name})
		for {
			acl, err := acls.Next(cmdCtx)
			if err != nil {
				break
			}
			allAcls = append(allAcls, SecretAclState{
				Scope:      scope.Name,
				Principal:  acl.Principal,
				Permission: acl.Permission.String(),
				Exist:      true,
			})
		}
	}

	return allAcls, nil
}
