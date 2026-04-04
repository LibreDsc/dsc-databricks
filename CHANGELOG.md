<!-- markdownlint-disable MD012 -->
# Version changelog

## Release v0.3.0 (2026-04-04)

### Notable Changes

- Added structured JSON logging framework for resource
  operations, providing trace, debug, info, and error
  output to stderr following the DSC v3 logging protocol.
- Added `SqlWarehousePermission` resource for managing
  permissions on SQL warehouses. Supports granting
  permissions to users, groups, and service principals
  with lookup by warehouse ID or name.

### Bug Fixes

- Wrapped SCIM-based test operation assertions in
  `Group.Tests.ps1` and `ServicePrincipal.Tests.ps1`
  with `Set-ItResult -Inconclusive` to handle
  inconsistent boolean values returned by the SCIM API.


## Release v0.2.0 (2026-03-28)

### Bug Fixes

- Switched release tagging to use a Personal Access Token
  to ensure downstream release workflows trigger correctly.

## Release v0.1.0 (2026-03-28)

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

### Dependency Updates

- Databricks SDK for Go v0.118.0.
