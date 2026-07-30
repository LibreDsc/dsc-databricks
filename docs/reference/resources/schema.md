# Schema

Manages Unity Catalog schemas in a Databricks workspace. A schema lives in a
catalog and contains tables, views, volumes, and functions. The schema is
identified by the combination of `catalog_name` and `name`; the API addresses
it by the two-level full name `catalog_name.schema_name`.

Type: `LibreDsc.Databricks/Schema`

## Syntax

```json
{
  "name": "string",
  "catalog_name": "string",
  "comment": "string",
  "owner": "string",
  "storage_root": "string",
  "enable_predictive_optimization": "DISABLE | ENABLE | INHERIT",
  "properties": { "string": "string" },
  "_exist": true
}
```

## Properties

| Name                             | Type    | Required | Description                                                                              |
|----------------------------------|---------|----------|------------------------------------------------------------------------------------------|
| `name`                           | string  | Yes      | Name of the schema, relative to its parent catalog.                                      |
| `catalog_name`                   | string  | Yes      | Name of the parent catalog.                                                              |
| `full_name`                      | string  | No       | Full name in the form `catalog_name.schema_name`. Read-only.                             |
| `comment`                        | string  | No       | User-provided free-form text description.                                                |
| `owner`                          | string  | No       | Username of the current owner of the schema.                                             |
| `storage_root`                   | string  | No       | Storage root URL for managed tables within the schema. Cannot be changed after creation. |
| `storage_location`               | string  | No       | Storage location URL for managed tables. Read-only.                                      |
| `enable_predictive_optimization` | string  | No       | Valid values: `DISABLE`, `ENABLE`, `INHERIT`.                                            |
| `schema_id`                      | string  | No       | Unique identifier of the schema. Read-only.                                              |
| `metastore_id`                   | string  | No       | Unique identifier of the parent metastore. Read-only.                                    |
| `properties`                     | object  | No       | Key-value properties attached to the schema.                                             |
| `_exist`                         | boolean | No       | Whether the instance should exist. Default: `true`.                                      |

Renaming is not modeled: changing `name` or `catalog_name` addresses a
different schema. `owner` and `enable_predictive_optimization` are applied by
a follow-up update when creating, because the create API does not accept
them.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

The DSC engine synthesizes `test` from `get`. Delete removes the schema with
force semantics: contained objects are removed with it. Export enumerates
the schemas of every catalog the caller can list.

## Example

Create a schema with a comment:

```json
{
  "name": "sales",
  "catalog_name": "engineering",
  "comment": "Sales data marts"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Schema --input (Get-Content .\schema.json -Raw)
```

## See also

- [Catalog][01]
- [Grant][02]
- [About Unity Catalog dependencies][03]
- [Exit codes][04]

<!-- Link references -->
[01]: catalog.md
[02]: grant.md
[03]: ../../explanation/about-unity-catalog-dependencies.md
[04]: ../exit-codes.md
