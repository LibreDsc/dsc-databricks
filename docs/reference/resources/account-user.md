# AccountUser

Manages Databricks account-level users through the account SCIM API. An
account user exists across the whole Databricks account rather than in one
workspace.

This resource uses the account client, so `DATABRICKS_ACCOUNT_HOST` must be
set in addition to the usual credentials. To manage users inside a single
workspace, use [User][01].

Type: `LibreDsc.Databricks/AccountUser`

## Syntax

```json
{
  "user_name": "string",
  "display_name": "string",
  "active": true,
  "emails": [{ "value": "string", "type": "string", "primary": true }],
  "roles": [{ "value": "string" }],
  "_exist": true
}
```

## Properties

| Name           | Type             | Required | Description                                                                       |
|----------------|------------------|----------|-----------------------------------------------------------------------------------|
| `user_name`    | string           | Yes      | Email address of the account user. The primary identifier.                        |
| `display_name` | string           | No       | Concatenation of the given and family names.                                      |
| `active`       | boolean          | No       | Whether the user is active. Always sent, so `false` deactivates without deleting. |
| `emails`       | array of complex | No       | Email addresses associated with the account user.                                 |
| `roles`        | array of complex | No       | AWS instance profile ARN roles.                                                   |
| `id`           | string           | No       | Databricks account user ID. Read-only.                                            |
| `_exist`       | boolean          | No       | Whether the instance should exist. Default: `true`.                               |

Complex values are objects with `value` (required), and optional `display`,
`type`, and `primary`. Unlike [User][01], this resource has no
`entitlements` property; entitlements are assigned per workspace.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`, for the same reason as [User][01]:
SCIM complex values carry server-populated fields that literal comparison
would report as drift.

## Example

Create an account user:

```json
{
  "user_name": "dana@example.com",
  "display_name": "Dana Lee",
  "active": true
}
```

```powershell
$env:DATABRICKS_ACCOUNT_HOST = 'https://accounts.azuredatabricks.net'
dsc resource set -r LibreDsc.Databricks/AccountUser --input (Get-Content .\account-user.json -Raw)
```

## See also

- [User][01]
- [Environment variables][02]
- [How to authenticate to Databricks][03]

<!-- Link references -->
[01]: user.md
[02]: ../environment-variables.md
[03]: ../../how-to/authenticate.md
