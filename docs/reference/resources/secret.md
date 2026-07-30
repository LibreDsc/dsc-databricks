# Secret

Manages secrets inside a [SecretScope][01]. A secret is identified by the
combination of `scope` and `key`.

Secret values are write-only. The Databricks API never returns them, so
`get` and `export` report the key without the value, and drift on the value
itself cannot be detected.

Type: `LibreDsc.Databricks/Secret`

## Syntax

```json
{
  "scope": "string",
  "key": "string",
  "string_value": "string",
  "_exist": true
}
```

## Properties

| Name           | Type    | Required | Description                                          |
|----------------|---------|----------|------------------------------------------------------|
| `scope`        | string  | Yes      | Name of the scope the secret belongs to.             |
| `key`          | string  | Yes      | Unique name identifying the secret within the scope. |
| `string_value` | string  | No       | Value stored in UTF-8 (MB4) form. Write-only.        |
| `bytes_value`  | string  | No       | Value stored as bytes. Write-only.                   |
| `_exist`       | boolean | No       | Whether the instance should exist. Default: `true`.  |

`set` requires exactly one of `string_value` or `bytes_value`. Supplying
neither fails with exit code 4.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`. Because the
value is never returned, a `set` writes the value on every run.

## Example

Store a secret in an existing scope. Never put a literal value in a
configuration document you commit; resolve it with the `secret()` function
or pass it in as a `securestring` parameter, as shown in
[Manage secrets with a DSC configuration document][02]:

```yaml
$schema: https://aka.ms/dsc/schemas/v3/bundled/config/document.json
resources:
  - name: api token
    type: LibreDsc.Databricks/Secret
    properties:
      scope: deployment-credentials
      key: api-token
      string_value: "[secret('DatabricksApiToken')]"
```

```powershell
dsc config set -f .\secret.dsc.config.yaml
```

## See also

- [SecretScope][01]
- [SecretAcl][03]
- [Manage secrets with a DSC configuration document][02]

<!-- Link references -->
[01]: secret-scope.md
[02]: ../../tutorials/manage-secrets-with-a-configuration.md
[03]: secret-acl.md
