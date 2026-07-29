# SecretScope

Manages Databricks secret scopes. A secret scope is a named container for
secrets, stored in the Databricks-backed secret store.

Type: `LibreDsc.Databricks/SecretScope`

## Syntax

```json
{
  "scope": "string",
  "_exist": true
}
```

## Properties

| Name | Type | Required | Description |
| ---- | ---- | -------- | ----------- |
| `scope` | string | Yes | A unique name to identify the scope. |
| `backend_type` | string | No | The backend type the scope was created with. Read-only. |
| `_exist` | boolean | No | Whether the instance should exist. Default: `true`. |

## Capabilities

`get`, `set`, `test`, `delete`, `export`, `setWhatIf`.

The resource implements a native `test`. Deleting a scope removes the
secrets and ACLs stored within it.

## Example

Create a secret scope:

```json
{
  "scope": "deployment-credentials"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/SecretScope --input '{"scope":"deployment-credentials"}'
```

## See also

- [Get started with dsc-databricks][01]
- [Manage secrets with a DSC configuration document][02]
- [Exit codes][03]

<!-- Link references -->
[01]: ../../tutorials/get-started.md
[02]: ../../tutorials/manage-secrets-with-a-configuration.md
[03]: ../exit-codes.md
