# Manage secrets with a DSC configuration document

In this tutorial, you author a DSC configuration document that manages a
secret scope, a secret, and a secret ACL as one unit. You preview the
changes with what-if, apply the configuration, verify the result, and make
a change to see the configuration converge.

## Prerequisites

- A completed [Get started with dsc-databricks][01] setup: DSC engine v3.2
  or later, `DSC_RESOURCE_PATH` set, and working authentication.
- A second user or group in your workspace to grant access to. This
  tutorial uses the built-in `users` group.

## Step 1: Create the configuration document

Create a file named `secrets.dsc.yaml` with the following content:

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

The `dependsOn` entries make the engine create the scope before the secret
and the ACL that live in it.

## Step 2: Preview the changes

Run a what-if operation to see what would happen without changing anything:

```powershell
dsc config set -w -f .\secrets.dsc.yaml
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
dsc config set -f .\secrets.dsc.yaml
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

Edit `secrets.dsc.yaml` and change the ACL permission from `READ` to
`WRITE`. Apply the document again:

```powershell
dsc config set -f .\secrets.dsc.yaml
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

## Next steps

- [How to preview changes with what-if][02]
- [About what-if predictions][03]
- [Grant reference][04]

<!-- Link references -->
[01]: get-started.md
[02]: ../how-to/preview-changes-with-what-if.md
[03]: ../explanation/about-what-if-predictions.md
[04]: ../reference/resources/grant.md
