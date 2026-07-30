# User

Manages Databricks workspace users through the SCIM API. A user is
identified by `user_name`, which is the account's email address.

To manage users at the account level rather than in a single workspace, use
[AccountUser][01].

Type: `LibreDsc.Databricks/User`

## Syntax

```json
{
  "user_name": "string",
  "display_name": "string",
  "active": true,
  "emails": [{ "value": "string", "type": "string", "primary": true }],
  "entitlements": [{ "value": "string" }],
  "roles": [{ "value": "string" }],
  "_exist": true
}
```

## Properties

| Name           | Type             | Required | Description                                                                       |
|----------------|------------------|----------|-----------------------------------------------------------------------------------|
| `user_name`    | string           | Yes      | Email address of the user. The primary identifier.                                |
| `display_name` | string           | No       | Concatenation of the given and family names.                                      |
| `active`       | boolean          | No       | Whether the user is active. Always sent, so `false` deactivates without deleting. |
| `emails`       | array of complex | No       | Email addresses associated with the user.                                         |
| `entitlements` | array of complex | No       | Entitlements assigned to the user, for example `allow-cluster-create`.            |
| `roles`        | array of complex | No       | AWS instance profile ARN roles.                                                   |
| `id`           | string           | No       | Databricks user ID. Read-only.                                                    |
| `_exist`       | boolean          | No       | Whether the instance should exist. Default: `true`.                               |

Complex values are objects with `value` (required), and optional `display`,
`type`, and `primary`.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`, because the SCIM API returns
complex values with server-populated fields that a literal comparison would
report as drift.

## Example

Create an active user with cluster-creation rights:

```json
{
  "user_name": "dana@example.com",
  "display_name": "Dana Lee",
  "active": true,
  "entitlements": [{ "value": "allow-cluster-create" }]
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/User --input (Get-Content .\user.json -Raw)
```

Deactivate the user without removing it:

```powershell
dsc resource set -r LibreDsc.Databricks/User --input '{"user_name":"dana@example.com","active":false}'
```

## See also

- [AccountUser][01]
- [Group][02]
- [ServicePrincipal][03]

<!-- Link references -->
[01]: account-user.md
[02]: group.md
[03]: service-principal.md
