# NEXT CHANGELOG

## Release v0.5.0

### Notable Changes

- **Seven new Unity Catalog resources.** The module now manages the full UC
  object hierarchy and its storage plumbing:
  - `LibreDsc.Databricks/Schema` and `LibreDsc.Databricks/Volume` — schemas
    and (managed or external) volumes under a catalog. Create-only fields
    (`storage_root`, `volume_type`, `storage_location`) are documented in
    the schema and left untouched on update.
  - `LibreDsc.Databricks/StorageCredential` and
    `LibreDsc.Databricks/ServiceCredential` — Azure credential blocks
    (`azure_managed_identity` backed by an access connector, or
    `azure_service_principal`; `client_secret` is write-only). Both ship a
    custom `test` that ignores server-computed nested fields, so
    configurations converge cleanly.
  - `LibreDsc.Databricks/ExternalLocation` — storage URLs bound to a
    storage credential; `read_only` and `fallback` are always enforced
    (explicitly sent even when `false`).
  - `LibreDsc.Databricks/Connection` — Lakehouse Federation connections.
    Updates resend the full `options` map; secret option values come back
    redacted and report as drift when specified.
  - `LibreDsc.Databricks/Grant` — declarative per-principal privileges on
    any securable (`securable_type` + `full_name` + `principal` →
    `privileges`). Set converges the principal's direct privilege set via
    the permissions delta API, `test` compares privileges
    order-insensitively, delete revokes everything, and export enumerates
    grants on metastore-level securables (catalogs, external locations,
    credentials, connections).
  Owner and isolation/predictive-optimization settings that the create APIs
  don't accept are applied by a chained post-create update, so a single
  `dsc config set` converges them.

### Bug Fixes

### Dependency Updates

- Bumped `github.com/databricks/databricks-sdk-go` from v0.163.0 to
  v0.165.0.
