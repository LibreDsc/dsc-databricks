# NEXT CHANGELOG

## Release v0.1.0

### Notable Changes

- Added `User` resource for managing workspace users with
  entitlements and roles.
- Added `AccountUser` resource for managing account-level
  users.
- Added `Group` resource with support for member and role
  assignments.
- Added `ServicePrincipal` resource for managing service
  principals and their entitlements.
- Added `SecretScope` resource for creating and managing
  secret scopes.
- Added `Secret` resource for storing and updating secrets
  within scopes.
- Added `SecretAcl` resource for controlling secret scope
  access permissions.
- Added `Cluster` resource with support for autoscaling,
  Spark configuration, and custom tags.
- Added `ClusterPolicy` resource for defining and enforcing
  cluster creation policies.
- Added `SqlWarehouse` resource with Photon support and
  auto-stop configuration.
- Added `Catalog` resource for managing Unity Catalog
  catalogs.
- Added `Repo` resource for managing Git folders in the
  workspace.
- Added `WorkspaceConf` resource for managing workspace
  configuration keys.
- Added `WorkspaceSetting` resource for managing
  workspace-level settings.
- Implemented `get`, `set`, `test`, `delete`, and `export`
  operations across all resources.
- Full support for Databricks unified authentication via
  environment variables.
- DSC v3 manifest with schema definitions for all 14
  resource types.

### Bug Fixes

### Dependency Updates

- Databricks SDK for Go v0.118.0.
