package resources

import (
	dsc "github.com/LibreDsc/dsc-go-rdk"
)

// RegisterAll registers every Databricks resource with the manager.
// Adding a new resource requires exactly one MustRegister line here.
func RegisterAll(m *dsc.Manager) {
	dsc.MustRegister(m, dsc.MustResource[UserState](&UserHandler{}, userConfig()))
	dsc.MustRegister(m, dsc.MustResource[AccountUserState](&AccountUserHandler{}, accountUserConfig()))
	dsc.MustRegister(m, dsc.MustResource[GroupState](&GroupHandler{}, groupConfig()))
	dsc.MustRegister(m, dsc.MustResource[ServicePrincipalState](&ServicePrincipalHandler{}, servicePrincipalConfig()))
	dsc.MustRegister(m, dsc.MustResource[CatalogState](&CatalogHandler{}, catalogConfig()))
	dsc.MustRegister(m, dsc.MustResource[SchemaState](&SchemaHandler{}, schemaConfig()))
	dsc.MustRegister(m, dsc.MustResource[VolumeState](&VolumeHandler{}, volumeConfig()))
	dsc.MustRegister(m, dsc.MustResource[StorageCredentialState](&StorageCredentialHandler{}, storageCredentialConfig()))
	dsc.MustRegister(m, dsc.MustResource[ServiceCredentialState](&ServiceCredentialHandler{}, serviceCredentialConfig()))
	dsc.MustRegister(m, dsc.MustResource[ExternalLocationState](&ExternalLocationHandler{}, externalLocationConfig()))
	dsc.MustRegister(m, dsc.MustResource[ConnectionState](&ConnectionHandler{}, connectionConfig()))
	dsc.MustRegister(m, dsc.MustResource[GrantState](&GrantHandler{}, grantConfig()))
	dsc.MustRegister(m, dsc.MustResource[ClusterState](&ClusterHandler{}, clusterConfig()))
	dsc.MustRegister(m, dsc.MustResource[ClusterPolicyState](&ClusterPolicyHandler{}, clusterPolicyConfig()))
	dsc.MustRegister(m, dsc.MustResource[RepoState](&RepoHandler{}, repoConfig()))
	dsc.MustRegister(m, dsc.MustResource[SecretState](&SecretHandler{}, secretConfig()))
	dsc.MustRegister(m, dsc.MustResource[SecretScopeState](&SecretScopeHandler{}, secretScopeConfig()))
	dsc.MustRegister(m, dsc.MustResource[SecretAclState](&SecretAclHandler{}, secretAclConfig()))
	dsc.MustRegister(m, dsc.MustResource[SqlWarehouseState](&SqlWarehouseHandler{}, sqlWarehouseConfig()))
	dsc.MustRegister(m, dsc.MustResource[SqlWarehousePermissionState](&SqlWarehousePermissionHandler{}, sqlWarehousePermissionConfig()))
	dsc.MustRegister(m, dsc.MustResource[WorkspaceConfState](&WorkspaceConfHandler{}, workspaceConfConfig()))
	dsc.MustRegister(m, dsc.MustResource[WorkspaceSettingState](&WorkspaceSettingHandler{}, workspaceSettingConfig()))
}
