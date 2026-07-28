# NEXT CHANGELOG

## Release v0.4.0

### Notable Changes

- **Preview changes before applying them.** Every resource now supports
  what-if: add `-w` to `dsc config set` and the result shows exactly what
  would be created or updated — including the predicted `afterState` and
  `changedProperties` — without touching your workspace. The prediction is
  computed natively by each resource (no API calls that modify anything),
  so it is more accurate than the DSC engine's synthetic fallback and does
  not leak `_metadata` into `changedProperties`. `dsc resource list` shows
  the new `setWhatIf` capability on all 15 resources. What-if is advertised
  via a `whatIfArg` on the `set` method (requires DSC v3.2 or later),
  replacing the deprecated dedicated `whatIf` operation.
- **Log messages can now be translated.** Diagnostic output (shown with
  `DSC_TRACE_LEVEL=info` or higher) goes through a localization layer.
  Set `DSC_DATABRICKS_LANG` (or rely on `LC_ALL`/`LANG`) to pick a
  language; English ships today and new languages only need a translation
  table, no code changes.
- **Deleting something that is already gone now succeeds.** Previously
  User, AccountUser, Group, ServicePrincipal, ClusterPolicy, Cluster, and
  SqlWarehouse returned a not-found error, which could fail a
  configuration run that was already in the desired state.
- **Richer schemas.** Read-only fields you get back from the API (cluster
  `state`/`state_message`, SQL warehouse `state`/`num_clusters`, workspace
  setting `etag`, repo `id`/`head_commit_id`, catalog
  `storage_location`/`catalog_type`/`metastore_id`) are now documented as
  optional properties in `dsc resource schema` output, and enum values are
  listed for `SecretAcl.permission`,
  `SqlWarehousePermission.permission_level`, `Catalog.isolation_mode`, and
  `Catalog.enable_predictive_optimization` — so editors with schema
  completion can offer the valid values.
- **Heads-up if you script against set/test output:** `changedProperties`
  and `differingProperties` no longer include underscore-prefixed
  properties such as `_exist` (custom tests still report `_exist` when the
  instance is missing). Update any automation that looked for `_exist` in
  those arrays.
- **Manifest layout changed for discovery.** The build now produces one
  `libredsc.databricks.<name>.dsc.resource.json` file per resource instead
  of the single aggregate `LibreDsc.Databricks.dsc.manifests.json`. Point
  `DSC_RESOURCE_PATH` at the output directory as before; if you copied or
  parsed the old aggregate file, switch to the per-resource files.
- **Under the hood:** all resources were rebuilt on the released
  [dsc-go-rdk](https://github.com/LibreDsc/dsc-go-rdk) v0.1.0 library,
  replacing the in-house framework and Cobra CLI. Day-to-day usage
  (`get`/`set`/`test`/`delete`/`export`/`schema`/`manifest`) is unchanged.
- **For contributors:** authoring guidance now lives in `CLAUDE.md`
  (the `.github/instructions` file points there). Test coverage grew with
  Go unit tests (validation, request builders, SCIM conversions, what-if
  projections, localization, manifest/schema parity) and Pester `WhatIf`
  contexts in every suite via the new `Invoke-DscWhatIf` helper;
  SqlWarehousePermission is covered inline in the SqlWarehouse suite.
- **For contributors: Unity Catalog test environment.** Groundwork for the
  upcoming Unity Catalog object resources:
  `tools/Initialize-DatabricksTests.ps1` gains `Test-UnityCatalogAvailable`
  (metastore-attachment detection; UC suites skip on workspaces that are not
  UC-enabled), `New-UnityCatalogTestEnvironment` /
  `Remove-UnityCatalogTestEnvironment` (tear up / tear down a dedicated
  catalog + schema fixture through the Unity Catalog REST API, with
  force-delete cascade on teardown), a `New-TestSchemaName` generator, and an
  `Invoke-DatabricksApi` REST helper. Set
  `DATABRICKS_CATALOG_STORAGE_LOCATION` when the metastore has no default
  managed storage; a per-catalog subdirectory is used so storage locations
  never overlap.

### Bug Fixes

- **WorkspaceSetting `get` no longer fails on never-written settings.** On a
  fresh workspace, settings that have never been written (e.g.
  `default_namespace`) return 404 from the settings API, which Get treated
  as a fatal error (exit code 2). Get now reports such a setting at its
  server-side default — `_exist: true` with an explicit empty `value` and no
  `etag` — and Set tolerates the missing etag on its pre-read so the first
  write goes through (updates already send `allow_missing`). As part of
  this, `value` is now always present in Get output (empty string when
  unset) instead of being omitted.
- **Group `display_name` is no longer schema-required.** The handler always
  accepted either `id` or `display_name` to identify a group, but the schema
  marked `display_name` required, so the DSC engine rejected id-only input
  (e.g. `dsc resource get` by `id`) with a `Schema: "display_name" is a
  required property` error before the handler ran. `display_name` is now
  optional (still always emitted in output and still required when creating a
  group).
- **Repo lookup by path works again.** The Repos list API now serves
  canonical `/Workspace/Repos/...` paths, so the SDK's `GetByPath` (a
  client-side exact-string match over the full repo list) no longer found
  repos requested by their legacy `/Repos/...` path. Get treated existing
  repos as missing, which made branch-only `set` runs fail with
  `missing required field(s): url, provider` and export report paths that
  didn't match the requested form. Repo Get now resolves the path
  server-side (workspace `get-status`, which accepts both forms) and fetches
  the repo by id; Get and Export echo the caller's/tree-walk path form so no
  spurious `path` diffs appear.
- **Repo export no longer uses the deprecated `Repos.ListAll`.** As of
  databricks-sdk-go v0.163.0 that method is deprecated because it omits
  Git-CLI-enabled repos. Export now walks the Workspace API tree, collecting
  every `REPO` object and enriching it with Git metadata via the Repos API;
  subtrees the caller cannot list are skipped rather than failing the export.

### Dependency Updates

- Bumped `github.com/databricks/databricks-sdk-go` from v0.118.0 to v0.163.0.
- Bumped `github.com/LibreDsc/dsc-go-rdk` to v0.2.0, which advertises native
  what-if through a `whatIfArg` on `set` instead of the deprecated dedicated
  `whatIf` manifest method (see
  [PowerShell/DSC#1361](https://github.com/PowerShell/DSC/issues/1361);
  requires DSC v3.2+).
  Resource handlers are unchanged — only manifest generation and the
  `dsc resource list` capability name (`whatIf` → `setWhatIf`) differ.
- Added `github.com/LibreDsc/dsc-go-rdk` v0.1.0 and promoted
  `golang.org/x/text` to a direct dependency; removed `spf13/cobra` and
  `spf13/pflag`. Go toolchain bumped to 1.26.
- E2E test suites migrated to Pester v6 (`Invoke-Pester` now uses
  `New-PesterConfiguration` with an explicit `tests/` path; CI installs
  Pester 6.0.1).
