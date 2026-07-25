package resources

import (
	"context"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

// SecretState represents the state of a Databricks secret. The secret value is
// write-only: the Databricks API never returns it, so Get only reports existence.
type SecretState struct {
	dsc.ExistProperty
	Scope       string `json:"scope" description:"The name of the scope to which the secret will be associated with."`
	Key         string `json:"key" description:"A unique name to identify the secret."`
	StringValue string `json:"string_value,omitempty" description:"If specified, note that the value will be stored in UTF-8 (MB4) form."`
	BytesValue  string `json:"bytes_value,omitempty" description:"If specified, value will be stored as bytes."`
}

func secretConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Secret",
		Version:     "0.1.0",
		Description: "Manage Databricks secrets",
		Tags:        []string{"databricks", "secret", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks secrets.",
			AllowAdditionalProperties: true,
		},
	}
}

// SecretHandler handles Secret resource operations. Value equality cannot be
// tested — the API never returns the secret value — so no Testable is
// implemented and the DSC engine synthesizes test from Get.
type SecretHandler struct{}

func (h *SecretHandler) Get(ctx context.Context, in SecretState) (SecretState, error) {
	if err := requireFields(field{"scope", in.Scope}, field{"key", in.Key}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	dsc.Logger.Debugf(MsgLookup, "Secret", "key="+in.Key+" scope="+in.Scope)
	secrets := w.Secrets.ListSecrets(ctx, workspace.ListSecretsRequest{Scope: in.Scope})
	for {
		secret, err := secrets.Next(ctx)
		if err != nil {
			break
		}
		if secret.Key == in.Key {
			state := SecretState{Scope: in.Scope, Key: in.Key}
			state.SetExist(true)
			return state, nil
		}
	}
	dsc.Logger.Infof(MsgNotFound, "Secret", "key="+in.Key+" scope="+in.Scope)
	return dsc.NotFound(SecretState{Scope: in.Scope, Key: in.Key}, "Secret", "key="+in.Key, "scope="+in.Scope)
}

func (h *SecretHandler) Set(ctx context.Context, desired SecretState) (SecretState, error) {
	if err := requireFields(field{"scope", desired.Scope}, field{"key", desired.Key}); err != nil {
		return desired, err
	}
	if err := requireAtLeastOne("string_value or bytes_value", desired.StringValue, desired.BytesValue); err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	dsc.Logger.Infof(MsgPut, "Secret", "key="+desired.Key+" scope="+desired.Scope)
	if err := w.Secrets.PutSecret(ctx, workspace.PutSecret{
		Scope:       desired.Scope,
		Key:         desired.Key,
		StringValue: desired.StringValue,
		BytesValue:  desired.BytesValue,
	}); err != nil {
		return desired, err
	}

	return h.Get(ctx, desired)
}

func (h *SecretHandler) Delete(ctx context.Context, in SecretState) error {
	if err := requireFields(field{"scope", in.Scope}, field{"key", in.Key}); err != nil {
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

	dsc.Logger.Debugf(MsgDelete, "Secret", "key="+in.Key+" scope="+in.Scope)
	return w.Secrets.DeleteSecret(ctx, workspace.DeleteSecret{Scope: in.Scope, Key: in.Key})
}

func (h *SecretHandler) Export(ctx context.Context, _ SecretState) ([]SecretState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(MsgListAll, "Secret")
	var allSecrets []SecretState

	scopes := w.Secrets.ListScopes(ctx)
	for {
		scope, err := scopes.Next(ctx)
		if err != nil {
			break
		}

		secrets := w.Secrets.ListSecrets(ctx, workspace.ListSecretsRequest{Scope: scope.Name})
		for {
			secret, err := secrets.Next(ctx)
			if err != nil {
				break
			}
			state := SecretState{Scope: scope.Name, Key: secret.Key}
			state.SetExist(true)
			allSecrets = append(allSecrets, state)
		}
	}

	return allSecrets, nil
}

// SecretScopeState represents the state of a Databricks secret scope.
type SecretScopeState struct {
	dsc.ExistProperty
	Scope       string `json:"scope" description:"A unique name to identify the scope."`
	BackendType string `json:"backend_type,omitempty" description:"The backend type the scope was created with (read-only)."`
}

func secretScopeConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/SecretScope",
		Version:     "0.1.0",
		Description: "Manage Databricks secret scopes",
		Tags:        []string{"databricks", "secret", "scope", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks secret scopes.",
			AllowAdditionalProperties: true,
		},
	}
}

// SecretScopeHandler handles SecretScope resource operations.
type SecretScopeHandler struct{}

func (h *SecretScopeHandler) Get(ctx context.Context, in SecretScopeState) (SecretScopeState, error) {
	if err := requireFields(field{"scope", in.Scope}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	dsc.Logger.Debugf(MsgLookup, "SecretScope", "scope="+in.Scope)
	scopes := w.Secrets.ListScopes(ctx)
	for {
		scope, err := scopes.Next(ctx)
		if err != nil {
			break
		}
		if scope.Name == in.Scope {
			state := SecretScopeState{Scope: scope.Name, BackendType: scope.BackendType.String()}
			state.SetExist(true)
			return state, nil
		}
	}
	dsc.Logger.Infof(MsgNotFound, "SecretScope", "scope="+in.Scope)
	return dsc.NotFound(SecretScopeState{Scope: in.Scope}, "SecretScope", "scope="+in.Scope)
}

func (h *SecretScopeHandler) Set(ctx context.Context, desired SecretScopeState) (SecretScopeState, error) {
	if err := requireFields(field{"scope", desired.Scope}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if !current.ShouldExist() {
		w, err := workspaceClient()
		if err != nil {
			return desired, err
		}
		dsc.Logger.Infof(MsgCreate, "SecretScope", "scope="+desired.Scope)
		if err := w.Secrets.CreateScope(ctx, workspace.CreateScope{Scope: desired.Scope}); err != nil {
			return desired, err
		}
	} else {
		dsc.Logger.Debugf(MsgAlreadyExists, "SecretScope", "scope="+desired.Scope)
	}

	return h.Get(ctx, desired)
}

func (h *SecretScopeHandler) Test(ctx context.Context, desired SecretScopeState) (dsc.TestResult[SecretScopeState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[SecretScopeState]{}, err
	}

	result := dsc.TestResult[SecretScopeState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *SecretScopeHandler) Delete(ctx context.Context, in SecretScopeState) error {
	if err := requireFields(field{"scope", in.Scope}); err != nil {
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

	dsc.Logger.Debugf(MsgDelete, "SecretScope", "scope="+in.Scope)
	return w.Secrets.DeleteScope(ctx, workspace.DeleteScope{Scope: in.Scope})
}

func (h *SecretScopeHandler) Export(ctx context.Context, _ SecretScopeState) ([]SecretScopeState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(MsgListAll, "SecretScope")
	var allScopes []SecretScopeState

	scopes := w.Secrets.ListScopes(ctx)
	for {
		scope, err := scopes.Next(ctx)
		if err != nil {
			break
		}
		state := SecretScopeState{Scope: scope.Name, BackendType: scope.BackendType.String()}
		state.SetExist(true)
		allScopes = append(allScopes, state)
	}

	return allScopes, nil
}

// SecretAclState represents the state of a Databricks secret ACL. Permission is
// optional because get and delete operations only require scope and principal.
type SecretAclState struct {
	dsc.ExistProperty
	Scope      string `json:"scope" description:"The name of the scope to apply permissions to."`
	Principal  string `json:"principal" description:"The principal (user or group) to apply permissions to."`
	Permission string `json:"permission,omitempty" description:"The permission level applied to the principal (READ, WRITE, or MANAGE)." enum:"READ,WRITE,MANAGE"`
}

func secretAclConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/SecretAcl",
		Version:     "0.1.0",
		Description: "Manage Databricks secret ACLs",
		Tags:        []string{"databricks", "secret", "acl", "permissions", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks secret ACLs.",
			AllowAdditionalProperties: true,
		},
	}
}

// SecretAclHandler handles SecretAcl resource operations.
type SecretAclHandler struct{}

func (h *SecretAclHandler) Get(ctx context.Context, in SecretAclState) (SecretAclState, error) {
	if err := requireFields(field{"scope", in.Scope}, field{"principal", in.Principal}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	dsc.Logger.Debugf(MsgLookup, "SecretAcl", "scope="+in.Scope+" principal="+in.Principal)
	acl, err := w.Secrets.GetAcl(ctx, workspace.GetAclRequest{Scope: in.Scope, Principal: in.Principal})
	if err != nil {
		dsc.Logger.Infof(MsgNotFound, "SecretAcl", "scope="+in.Scope+" principal="+in.Principal)
		return dsc.NotFound(SecretAclState{Scope: in.Scope, Principal: in.Principal}, "SecretAcl",
			"scope="+in.Scope, "principal="+in.Principal)
	}

	state := SecretAclState{Scope: in.Scope, Principal: acl.Principal, Permission: acl.Permission.String()}
	state.SetExist(true)
	return state, nil
}

func (h *SecretAclHandler) Set(ctx context.Context, desired SecretAclState) (SecretAclState, error) {
	if err := requireFields(
		field{"scope", desired.Scope},
		field{"principal", desired.Principal},
		field{"permission", desired.Permission},
	); err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	dsc.Logger.Infof(MsgPut, "SecretAcl", "scope="+desired.Scope+" principal="+desired.Principal+" permission="+desired.Permission)
	if err := w.Secrets.PutAcl(ctx, workspace.PutAcl{
		Scope:      desired.Scope,
		Principal:  desired.Principal,
		Permission: workspace.AclPermission(desired.Permission),
	}); err != nil {
		return desired, err
	}

	return h.Get(ctx, desired)
}

func (h *SecretAclHandler) Test(ctx context.Context, desired SecretAclState) (dsc.TestResult[SecretAclState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[SecretAclState]{}, err
	}

	result := dsc.TestResult[SecretAclState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *SecretAclHandler) Delete(ctx context.Context, in SecretAclState) error {
	if err := requireFields(field{"scope", in.Scope}, field{"principal", in.Principal}); err != nil {
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

	dsc.Logger.Debugf(MsgDelete, "SecretAcl", "scope="+in.Scope+" principal="+in.Principal)
	return w.Secrets.DeleteAcl(ctx, workspace.DeleteAcl{Scope: in.Scope, Principal: in.Principal})
}

func (h *SecretAclHandler) Export(ctx context.Context, _ SecretAclState) ([]SecretAclState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(MsgListAll, "SecretAcl")
	var allAcls []SecretAclState

	scopes := w.Secrets.ListScopes(ctx)
	for {
		scope, err := scopes.Next(ctx)
		if err != nil {
			break
		}

		acls := w.Secrets.ListAcls(ctx, workspace.ListAclsRequest{Scope: scope.Name})
		for {
			acl, err := acls.Next(ctx)
			if err != nil {
				break
			}
			state := SecretAclState{Scope: scope.Name, Principal: acl.Principal, Permission: acl.Permission.String()}
			state.SetExist(true)
			allAcls = append(allAcls, state)
		}
	}

	return allAcls, nil
}
