# Volume

Manages Unity Catalog volumes. A volume is a storage container for
non-tabular data inside a [Schema][02], and is identified by the
combination of `catalog_name`, `schema_name`, and `name`.

Type: `LibreDsc.Databricks/Volume`

## Syntax

```json
{
  "name": "string",
  "catalog_name": "string",
  "schema_name": "string",
  "volume_type": "MANAGED",
  "storage_location": "string",
  "comment": "string",
  "owner": "string",
  "_exist": true
}
```

## Properties

| Name               | Type    | Required | Description                                                                                                       |
|--------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------|
| `name`             | string  | Yes      | Name of the volume, relative to its parent schema.                                                                |
| `catalog_name`     | string  | Yes      | Name of the parent catalog.                                                                                       |
| `schema_name`      | string  | Yes      | Name of the parent schema.                                                                                        |
| `volume_type`      | string  | No       | `MANAGED` or `EXTERNAL`. Required when creating. Create-only.                                                     |
| `storage_location` | string  | No       | Storage location. Required when creating an `EXTERNAL` volume; computed by the server for `MANAGED`. Create-only. |
| `comment`          | string  | No       | Free-form description.                                                                                            |
| `owner`            | string  | No       | Username of the current owner.                                                                                    |
| `full_name`        | string  | No       | `catalog_name.schema_name.name`. Read-only.                                                                       |
| `volume_id`        | string  | No       | Unique identifier of the volume. Read-only.                                                                       |
| `metastore_id`     | string  | No       | Unique identifier of the parent metastore. Read-only.                                                             |
| `_exist`           | boolean | No       | Whether the instance should exist. Default: `true`.                                                               |

`volume_type` and `storage_location` are create-only: the API rejects
changes to them, so `set` leaves them untouched on an existing volume.
Changing either means deleting and recreating the volume, which for a
`MANAGED` volume destroys its data.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`. Export walks the
catalog hierarchy, so catalogs the caller cannot enumerate are skipped.

## Example

Create a managed volume:

```json
{
  "name": "raw-drops",
  "catalog_name": "engineering",
  "schema_name": "landing",
  "volume_type": "MANAGED",
  "comment": "Inbound files awaiting ingestion"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Volume --input (Get-Content .\volume.json -Raw)
```

## See also

- [Catalog][01]
- [Schema][02]
- [About Unity Catalog dependencies][03]

<!-- Link references -->
[01]: catalog.md
[02]: schema.md
[03]: ../../explanation/about-unity-catalog-dependencies.md
