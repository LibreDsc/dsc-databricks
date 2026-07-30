# ServiceCredential

Manages Unity Catalog service credentials. A service credential
authenticates access to an external *service* rather than to storage, and
shares a namespace with [StorageCredential][01]: a name must be unique
across both within the metastore.

Exactly one credential block, `azure_managed_identity` or
`azure_service_principal`, is required when creating.

Type: `LibreDsc.Databricks/ServiceCredential`

## Syntax

```json
{
  "name": "string",
  "purpose": "SERVICE",
  "comment": "string",
  "owner": "string",
  "read_only": false,
  "skip_validation": false,
  "isolation_mode": "ISOLATION_MODE_ISOLATED | ISOLATION_MODE_OPEN",
  "azure_managed_identity": {
    "access_connector_id": "string",
    "managed_identity_id": "string"
  },
  "azure_service_principal": {
    "application_id": "string",
    "directory_id": "string",
    "client_secret": "string"
  },
  "_exist": true
}
```

## Properties

| Name                      | Type    | Required | Description                                                                                |
|---------------------------|---------|----------|--------------------------------------------------------------------------------------------|
| `name`                    | string  | Yes      | Name of the credential. Unique among storage and service credentials within the metastore. |
| `azure_managed_identity`  | object  | No       | Azure managed identity configuration. See the table below.                                 |
| `azure_service_principal` | object  | No       | Azure service principal configuration. See the table below.                                |
| `purpose`                 | string  | No       | `SERVICE` or `STORAGE`. Defaults to `SERVICE` when creating. Create-only.                  |
| `comment`                 | string  | No       | Free-form description.                                                                     |
| `owner`                   | string  | No       | Username of the current owner.                                                             |
| `read_only`               | boolean | No       | Usable only for read operations. Applies only when `purpose` is `STORAGE`. Always sent.    |
| `skip_validation`         | boolean | No       | Skip validation on create or update. Write-only toggle.                                    |
| `isolation_mode`          | string  | No       | `ISOLATION_MODE_ISOLATED` or `ISOLATION_MODE_OPEN`.                                        |
| `id`                      | string  | No       | Unique identifier of the credential. Read-only.                                            |
| `metastore_id`            | string  | No       | Unique identifier of the parent metastore. Read-only.                                      |
| `_exist`                  | boolean | No       | Whether the instance should exist. Default: `true`.                                        |

### azure_managed_identity

| Name                  | Type   | Required | Description                                                                                                  |
|-----------------------|--------|----------|--------------------------------------------------------------------------------------------------------------|
| `access_connector_id` | string | Yes      | Azure resource ID of the Azure Databricks Access Connector.                                                  |
| `managed_identity_id` | string | No       | Azure resource ID of a user-assigned managed identity. Omit to use the connector's system-assigned identity. |
| `credential_id`       | string | No       | Databricks internal ID of the credential. Read-only.                                                         |

### azure_service_principal

| Name             | Type   | Required | Description                                                                                                        |
|------------------|--------|----------|--------------------------------------------------------------------------------------------------------------------|
| `application_id` | string | Yes      | Application (client) ID of the Microsoft Entra application.                                                        |
| `directory_id`   | string | Yes      | Directory (tenant) ID of the Microsoft Entra application.                                                          |
| `client_secret`  | string | No       | Client secret of the application. Write-only: the API never returns it, so drift on the secret cannot be detected. |

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The native `test` normalizes server-computed nested fields (such as
`credential_id`) and write-only fields (`client_secret`, `skip_validation`)
before comparing, so a configuration that specifies them still converges.

## Example

Create a service credential backed by an access connector:

```json
{
  "name": "external-api-access",
  "purpose": "SERVICE",
  "comment": "Credential for the partner REST API",
  "azure_managed_identity": {
    "access_connector_id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-data/providers/Microsoft.Databricks/accessConnectors/lakehouse"
  }
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/ServiceCredential --input (Get-Content .\credential.json -Raw)
```

## See also

- [StorageCredential][01]
- [Connection][02]
- [About Unity Catalog dependencies][03]

<!-- Link references -->
[01]: storage-credential.md
[02]: connection.md
[03]: ../../explanation/about-unity-catalog-dependencies.md
