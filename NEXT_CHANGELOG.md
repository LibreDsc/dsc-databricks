# NEXT CHANGELOG

## Release v0.4.0

### Notable Changes

- Rebuilt all resources on the released
  [dsc-go-rdk](https://github.com/LibreDsc/dsc-go-rdk) v0.1.0 library,
  replacing the in-house `internal/dsc` framework. Handlers now implement the
  rdk capability interfaces (`Gettable`, `Settable`, `Testable`, `Deletable`,
  `Exportable`) over a single state struct per resource, and the Cobra CLI was
  replaced by the rdk `Manager` host.
- Manifest generation now produces one
  `libredsc.databricks.<name>.dsc.resource.json` file per resource (via
  `manifest --out-dir`) instead of the aggregate
  `LibreDsc.Databricks.dsc.manifests.json`.
- `changedProperties` and `differingProperties` no longer include canonical
  (underscore-prefixed) properties such as `_exist`, except that custom test
  implementations still report `_exist` when the instance is missing.
- Read-only fields (cluster `state`/`state_message`, SQL warehouse
  `state`/`num_clusters`, workspace setting `etag`, repo
  `id`/`head_commit_id`, catalog
  `storage_location`/`catalog_type`/`metastore_id`) now appear in the resource
  schemas as optional properties.
- Enum constraints added to schemas for `SecretAcl.permission`,
  `SqlWarehousePermission.permission_level`, `Catalog.isolation_mode`, and
  `Catalog.enable_predictive_optimization`.
- Deleting an instance that does not exist now always succeeds (previously
  User, AccountUser, Group, ServicePrincipal, ClusterPolicy, Cluster, and
  SqlWarehouse returned a not-found error).
- Authoring guidance moved to `CLAUDE.md`;
  `.github/instructions/dsc-databricks.instructions.md` now points there.
- Added Go unit tests for validation helpers, request builders, SCIM
  conversions, principal matching, and manifest/schema parity.
- Added `tests/SqlWarehousePermission.Tests.ps1` E2E suite.

### Bug Fixes

### Dependency Updates

- Added `github.com/LibreDsc/dsc-go-rdk` v0.1.0; removed `spf13/cobra` and
  `spf13/pflag`. Go toolchain bumped to 1.26.
