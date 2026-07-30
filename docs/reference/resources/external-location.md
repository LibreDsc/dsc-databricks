# ExternalLocation

Manages Unity Catalog external locations. An external location pairs a
storage URL with a [StorageCredential][01] that grants access to it, and is
the securable that catalogs and external volumes are built on.

Unity Catalog rejects overlapping locations, so a URL that falls inside an
existing external location cannot be registered again.

Type: `LibreDsc.Databricks/ExternalLocation`

## Syntax

```json
{
  "name": "string",
  "url": "abfss://container@account.dfs.core.windows.net/path",
  "credential_name": "string",
  "comment": "string",
  "read_only": false,
  "fallback": false,
  "_exist": true
}
```

## Properties

| Name              | Type    | Required | Description                                                                                                     |
|-------------------|---------|----------|-----------------------------------------------------------------------------------------------------------------|
| `name`            | string  | Yes      | Name of the external location.                                                                                  |
| `url`             | string  | No       | Storage URL, for example `abfss://container@account.dfs.core.windows.net/path`. Required when creating.         |
| `credential_name` | string  | No       | Name of the storage credential used with this location. Required when creating.                                 |
| `comment`         | string  | No       | Free-form description.                                                                                          |
| `owner`           | string  | No       | Username of the current owner.                                                                                  |
| `isolation_mode`  | string  | No       | `ISOLATION_MODE_ISOLATED` or `ISOLATION_MODE_OPEN`.                                                             |
| `read_only`       | boolean | No       | Whether the location is read-only. Always sent, including when `false`.                                         |
| `fallback`        | boolean | No       | Serve requests through cluster or warehouse credentials when access fails. Always sent, including when `false`. |
| `skip_validation` | boolean | No       | Skip validation of the storage credential on create or update. Write-only toggle.                               |
| `credential_id`   | string  | No       | Unique identifier of the storage credential. Read-only.                                                         |
| `metastore_id`    | string  | No       | Unique identifier of the parent metastore. Read-only.                                                           |
| `_exist`          | boolean | No       | Whether the instance should exist. Default: `true`.                                                             |

`read_only` and `fallback` are always sent to the API, so setting either to
`false` enforces that value rather than leaving the server default in place.

`skip_validation` is a write-only behavior toggle. The API never returns it,
so specifying it produces reported drift on `test`.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`.

## Example

Register a container as an external location:

```json
{
  "name": "landing-zone",
  "url": "abfss://landing@examplestorage.dfs.core.windows.net/",
  "credential_name": "example-storage-credential",
  "comment": "Inbound data drop"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/ExternalLocation --input (Get-Content .\location.json -Raw)
```

## See also

- [StorageCredential][01]
- [Catalog][02]
- [About Unity Catalog dependencies][03]

<!-- Link references -->
[01]: storage-credential.md
[02]: catalog.md
[03]: ../../explanation/about-unity-catalog-dependencies.md
