# Group

Manages Databricks groups through the SCIM API. A group collects users,
service principals, and other groups, and is the usual target for
[Grant][03] and permission assignments.

Type: `LibreDsc.Databricks/Group`

## Syntax

```json
{
  "display_name": "string",
  "members": [{ "value": "string" }],
  "entitlements": [{ "value": "string" }],
  "roles": [{ "value": "string" }],
  "_exist": true
}
```

## Properties

| Name           | Type             | Required | Description                                                                                                    |
|----------------|------------------|----------|----------------------------------------------------------------------------------------------------------------|
| `display_name` | string           | No       | Human-readable group name. Required when creating; either `display_name` or `id` identifies an existing group. |
| `id`           | string           | No       | Databricks group ID. Read-only on create, usable for lookup.                                                   |
| `external_id`  | string           | No       | Identifier for the group in an external system.                                                                |
| `members`      | array of complex | No       | Members of the group. `value` is the ID of a user, service principal, or group.                                |
| `entitlements` | array of complex | No       | Entitlements assigned to the group, for example `allow-cluster-create`.                                        |
| `roles`        | array of complex | No       | AWS instance profile ARN roles.                                                                                |
| `_exist`       | boolean          | No       | Whether the instance should exist. Default: `true`.                                                            |

Complex values are objects with `value` (required), and optional `display`,
`type`, and `primary`. Members are referenced by ID, not by name, so read
the ID from [User][01] or [ServicePrincipal][02] first.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`, because SCIM returns members and
entitlements with server-populated `display` and `$ref` fields that literal
comparison would report as drift.

## Example

Create a group with two members:

```json
{
  "display_name": "data-engineers",
  "entitlements": [{ "value": "allow-cluster-create" }],
  "members": [{ "value": "1234567890123456" }, { "value": "6543210987654321" }]
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Group --input (Get-Content .\group.json -Raw)
```

## See also

- [User][01]
- [ServicePrincipal][02]
- [Grant][03]

<!-- Link references -->
[01]: user.md
[02]: service-principal.md
[03]: grant.md
