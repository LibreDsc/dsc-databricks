# Grant

Manages Unity Catalog privilege grants. A grant instance represents the
complete set of privileges one principal holds directly on one securable.
The instance is identified by the combination of `securable_type`,
`full_name`, and `principal`.

Type: `LibreDsc.Databricks/Grant`

## Syntax

```json
{
  "securable_type": "catalog",
  "full_name": "string",
  "principal": "string",
  "privileges": ["USE_CATALOG", "CREATE_SCHEMA"],
  "_exist": true
}
```

## Properties

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `securable_type` | string | Yes | Type of the securable, lowercase. Valid values: `catalog`, `clean_room`, `connection`, `credential`, `external_location`, `external_metadata`, `function`, `metastore`, `pipeline`, `provider`, `recipient`, `schema`, `share`, `staging_table`, `storage_credential`, `table`, `volume`. |
| `full_name` | string | Yes | Full name of the securable, for example `main`, `main.default`, or `main.default.my_table`. |
| `principal` | string | Yes | User email, group name, or service principal application ID. |
| `privileges` | array of string | No | Privileges held directly on the securable, for example `USE_CATALOG`, `SELECT`, `ALL_PRIVILEGES`. Order-insensitive. Required for `set`. |
| `_exist` | boolean | No | Whether the instance should exist. Default: `true`. |

`set` converges the principal's direct privilege set to exactly the listed
privileges: missing privileges are granted and extra privileges are revoked
in a single request. `delete` revokes every privilege the principal holds
directly on the securable. Inherited privileges are not considered.
Privileges are returned in sorted order.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The native `test` compares the privilege list as a set: the same privileges
in a different order are in the desired state. Export is bounded to
metastore-level securables — catalogs, external locations, storage
credentials, service credentials, and connections. Grants on schemas,
tables, and volumes are not exported.

## Example

Grant catalog usage and schema creation to a group:

```json
{
  "securable_type": "catalog",
  "full_name": "engineering",
  "principal": "data-engineers",
  "privileges": ["USE_CATALOG", "CREATE_SCHEMA"]
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Grant --input (Get-Content .\grant.json -Raw)
```

## See also

- [Catalog][01]
- [About Unity Catalog dependencies][02]
- [How to export existing resources][03]

<!-- Link references -->
[01]: catalog.md
[02]: ../../explanation/about-unity-catalog-dependencies.md
[03]: ../../how-to/export-resources.md
