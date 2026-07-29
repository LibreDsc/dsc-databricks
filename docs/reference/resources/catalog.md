# Catalog

Manages Unity Catalog catalogs in a Databricks workspace. A catalog is the
top-level data container in a metastore and holds schemas.

Type: `LibreDsc.Databricks/Catalog`

## Syntax

```json
{
  "name": "string",
  "comment": "string",
  "owner": "string",
  "isolation_mode": "ISOLATED | OPEN",
  "storage_root": "string",
  "connection_name": "string",
  "provider_name": "string",
  "share_name": "string",
  "enable_predictive_optimization": "DISABLE | ENABLE | INHERIT",
  "properties": { "string": "string" },
  "options": { "string": "string" },
  "_exist": true
}
```

## Properties

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `name` | string | Yes | Name of the catalog. Unique identifier used for lookup. |
| `comment` | string | No | User-provided free-form text description. |
| `owner` | string | No | Username of the current owner of the catalog. |
| `isolation_mode` | string | No | Workspace accessibility. Valid values: `ISOLATED`, `OPEN`. |
| `storage_root` | string | No | Storage root URL for managed tables within the catalog. Cannot be changed after creation. |
| `storage_location` | string | No | Storage location URL for managed tables. Read-only. |
| `connection_name` | string | No | Name of the connection to an external data source. |
| `provider_name` | string | No | Name of the delta sharing provider. |
| `share_name` | string | No | Name of the share under the share provider. |
| `enable_predictive_optimization` | string | No | Valid values: `DISABLE`, `ENABLE`, `INHERIT`. |
| `catalog_type` | string | No | Type of the catalog. Read-only. |
| `metastore_id` | string | No | Unique identifier of the parent metastore. Read-only. |
| `properties` | object | No | Key-value properties attached to the catalog. |
| `options` | object | No | Key-value options attached to the catalog. |
| `_exist` | boolean | No | Whether the instance should exist. Default: `true`. |

On a metastore without default managed storage, `storage_root` is required to
create a catalog, and the URL must be covered by an existing external
location. See [About Unity Catalog dependencies][01].

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`. Delete removes the catalog with
force semantics: contained schemas and objects are removed with it.

## Example

Create a catalog with a managed storage root:

```json
{
  "name": "engineering",
  "comment": "Engineering data",
  "storage_root": "abfss://managed@account.dfs.core.windows.net/engineering"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Catalog --input (Get-Content .\catalog.json -Raw)
```

## See also

- [Schema][02]
- [Grant][03]
- [About Unity Catalog dependencies][01]
- [Exit codes][04]

<!-- Link references -->
[01]: ../../explanation/about-unity-catalog-dependencies.md
[02]: schema.md
[03]: grant.md
[04]: ../exit-codes.md
