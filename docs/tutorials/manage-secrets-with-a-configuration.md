# Manage secrets with a DSC configuration document

In this tutorial, you author a [DSC configuration document][00] that manages a
secret scope, a secret, and a secret ACL as one unit. You preview the
changes with what-if, apply the configuration, verify the result, and make
a change to see the configuration converge.

## Prerequisites

- A working installation and the [Basic usage of dsc-databricks][01]
  round trip behind you: DSC engine v3.2 or later, discoverable manifests,
  and working authentication.
- A second user or group in your workspace to grant access to. This
  tutorial uses the built-in `users` group.

## Step 1: Create the configuration document

Create a file named `secrets.dsc.config.yaml` with the following content:

```yaml
$schema: https://aka.ms/dsc/schemas/v3/bundled/config/document.json
resources:
  - name: deployment scope
    type: LibreDsc.Databricks/SecretScope
    properties:
      scope: dsc-deployment
  - name: api token
    type: LibreDsc.Databricks/Secret
    dependsOn:
      - "[resourceId('LibreDsc.Databricks/SecretScope', 'deployment scope')]"
    properties:
      scope: dsc-deployment
      key: api-token
      string_value: tutorial-secret-value
  - name: reader access
    type: LibreDsc.Databricks/SecretAcl
    dependsOn:
      - "[resourceId('LibreDsc.Databricks/SecretScope', 'deployment scope')]"
    properties:
      scope: dsc-deployment
      principal: users
      permission: READ
```

The [`dependsOn`][05] entries make the engine create the scope before the secret
and the ACL that live in it.

!!! danger "Do not commit a secret in plain text"

    `string_value: tutorial-secret-value` is acceptable for a throwaway
    tutorial value and wrong for anything else. A configuration document is
    a file you commit, review in pull requests and hand to CI, so a literal
    written there is readable by everyone who can read the repository, and
    it stays in the history after you delete the line.

Keep the value out of the document instead. DSC gives you two ways to do
that: the [`secret()` function][06], which resolves a name through a
registered secret extension when the configuration runs, and a
[`securestring` parameter][07] referenced with [`parameters()`][08], whose
value you supply at apply time. Both suit this resource, because
`string_value` is write-only. The module never reads it back, so the value
appears in neither `get` output, nor `export` output, nor a what-if
`afterState`.

=== "Resolve with secret()"

    ```yaml
      - name: api token
        type: LibreDsc.Databricks/Secret
        dependsOn:
          - "[resourceId('LibreDsc.Databricks/SecretScope', 'deployment scope')]"
        properties:
          scope: dsc-deployment
          key: api-token
          string_value: "[secret('DatabricksApiToken')]"
    ```

    The value never enters the document. Pass a second argument, as in
    `[secret('DatabricksApiToken', 'MyVault')]`, when more than one
    registered vault holds a secret under that name.

=== "Pass a securestring parameter"

    ```yaml
    parameters:
      apiToken:
        type: securestring
        metadata:
          description: Value stored in dsc-deployment/api-token.
    resources:
      - name: api token
        type: LibreDsc.Databricks/Secret
        properties:
          scope: dsc-deployment
          key: api-token
          string_value: "[parameters('apiToken')]"
    ```

    Supply the value at apply time, from a file your repository ignores or
    from your CI system's secret store:

    ```powershell
    dsc config --parameters-file .\secrets.parameters.yaml set --file .\secrets.dsc.config.yaml
    ```

    Note where the option sits: the parameter options belong to
    `dsc config`, before the subcommand, not to `set` after it.

## Step 2: Preview the changes

Run a what-if operation to see what would happen without changing anything:

```powershell
dsc config set -w -f .\secrets.dsc.config.yaml
```

The metadata reports the execution type as `whatIf`, and each resource
shows its predicted after-state:

```yaml
metadata:
  Microsoft.DSC:
    executionType: whatIf
results:
- name: deployment scope
  result:
    afterState:
      scope: dsc-deployment
      backend_type: DATABRICKS
      _exist: true
```

Nothing has been created yet. Run
`dsc resource get -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-deployment"}'`
to confirm the scope still reports `_exist: false`.

## Step 3: Apply the configuration

Apply the document for real:

```powershell
dsc config set -f .\secrets.dsc.config.yaml
```

The result lists all three resources with their after-states. The summary
reports no errors.

## Step 4: Verify the deployed state

Read the ACL to confirm the grant landed:

```powershell
dsc resource get -r LibreDsc.Databricks/SecretAcl --input '{"scope":"dsc-deployment","principal":"users"}'
```

You should see the `READ` permission:

```yaml
actualState:
  scope: dsc-deployment
  principal: users
  permission: READ
  _exist: true
```

## Step 5: Change and re-apply

Edit `secrets.dsc.config.yaml` and change the ACL permission from `READ` to
`WRITE`. Apply the document again:

```powershell
dsc config set -f .\secrets.dsc.config.yaml
```

Only the ACL reports a change; the scope and the secret are already in the
desired state. Repeat the `get` from step 4. The permission is now `WRITE`.

## Step 6: Clean up

Delete the scope. Deleting the scope removes the secret and the ACL stored
within it:

```powershell
dsc resource delete -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-deployment"}'
```

## What you learned

- A configuration document describes multiple resource instances and their
  ordering with `dependsOn`.
- `dsc config set -w` predicts an apply without changing anything.
- Re-applying a configuration only changes what drifted — the operation
  converges.
- Secret values belong outside the document, resolved by `secret()` or
  passed in as a `securestring` parameter.

## Next steps

- [How to preview changes with what-if][02]
- [About what-if predictions][03]
- [Grant reference][04]

<!-- Link references -->
[00]: https://learn.microsoft.com/en-us/powershell/dsc/concepts/configuration-documents/overview?view=dsc-3.0
[01]: basic-usage-dsc-databricks.md
[02]: ../how-to/preview-changes-with-what-if.md
[03]: ../explanation/about-what-if-predictions.md
[04]: ../reference/resources/grant.md
[05]: https://github.com/PowerShell/DSC/blob/23330fdfc7ced2b087cb7c2e9de1d3a2dd697f69/docs/reference/schemas/config/resource.md#dependson
[06]: https://github.com/PowerShell/DSC/blob/main/docs/reference/schemas/config/functions/secret.md
[07]: https://learn.microsoft.com/en-us/powershell/dsc/reference/schemas/config/parameter?view=dsc-3.0
[08]: https://learn.microsoft.com/en-us/powershell/dsc/reference/schemas/config/functions/parameters?view=dsc-3.0
