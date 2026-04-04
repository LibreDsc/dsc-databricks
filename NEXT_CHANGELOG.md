# NEXT CHANGELOG

## Release v0.3.0

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

### Dependency Updates
