# ServicePrincipal

Manages Databricks service principals through the SCIM API. A service
principal is a non-human identity used by automation, and is identified by
its `display_name`.

Type: `LibreDsc.Databricks/ServicePrincipal`

## Syntax

```json
{
  "display_name": "string",
  "application_id": "string",
  "active": true,
  "entitlements": [{ "value": "string" }],
  "roles": [{ "value": "string" }],
  "_exist": true
}
```

## Properties

| Name             | Type             | Required | Description                                                                                    |
|------------------|------------------|----------|------------------------------------------------------------------------------------------------|
| `display_name`   | string           | Yes      | Display name of the service principal. The primary identifier.                                 |
| `application_id` | string           | No       | UUID of the Microsoft Entra app registration or identity behind the service principal.         |
| `external_id`    | string           | No       | Reserved for future use.                                                                       |
| `id`             | string           | No       | Databricks service principal ID. Read-only.                                                    |
| `active`         | boolean          | No       | Whether the service principal is active. Always sent, so `false` deactivates without deleting. |
| `entitlements`   | array of complex | No       | Entitlements assigned to the service principal.                                                |
| `roles`          | array of complex | No       | AWS instance profile ARN roles.                                                                |
| `_exist`         | boolean          | No       | Whether the instance should exist. Default: `true`.                                            |

Complex values are objects with `value` (required), and optional `display`,
`type`, and `primary`.

Note that `application_id` is the identifier used elsewhere: [Grant][02] and
[SqlWarehousePermission][03] both take the application ID as the principal,
not the display name.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`, because SCIM complex values carry
server-populated fields that literal comparison would report as drift.

## Example

Create a service principal for a deployment pipeline:

```json
{
  "display_name": "ci-deployer",
  "application_id": "11111111-2222-3333-4444-555555555555",
  "active": true,
  "entitlements": [{ "value": "allow-cluster-create" }]
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/ServicePrincipal --input (Get-Content .\sp.json -Raw)
```

## See also

- [Group][01]
- [Grant][02]
- [SqlWarehousePermission][03]

<!-- Link references -->
[01]: group.md
[02]: grant.md
[03]: sql-warehouse-permission.md
