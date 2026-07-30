# SecretAcl

Manages access control entries on a [SecretScope][01]. An instance is one
principal's permission on one scope, identified by `scope` and `principal`.

Type: `LibreDsc.Databricks/SecretAcl`

## Syntax

```json
{
  "scope": "string",
  "principal": "string",
  "permission": "READ",
  "_exist": true
}
```

## Properties

| Name         | Type    | Required | Description                                         |
|--------------|---------|----------|-----------------------------------------------------|
| `scope`      | string  | Yes      | Name of the scope the entry applies to.             |
| `principal`  | string  | Yes      | User email or group name the entry applies to.      |
| `permission` | string  | No       | `READ`, `WRITE`, or `MANAGE`. Required for `set`.   |
| `_exist`     | boolean | No       | Whether the instance should exist. Default: `true`. |

Permissions are cumulative in the usual order: `MANAGE` implies `WRITE`,
which implies `READ`. A principal holds one entry per scope, so applying a
new `permission` replaces the previous one rather than adding to it.

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`. Deleting the parent scope removes
its ACLs, so a scope deletion also removes every entry on it.

## Example

Grant a group read access to a scope:

```json
{
  "scope": "deployment-credentials",
  "principal": "data-engineers",
  "permission": "READ"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/SecretAcl --input (Get-Content .\acl.json -Raw)
```

## See also

- [SecretScope][01]
- [Secret][02]
- [Manage secrets with a DSC configuration document][03]

<!-- Link references -->
[01]: secret-scope.md
[02]: secret.md
[03]: ../../tutorials/manage-secrets-with-a-configuration.md
