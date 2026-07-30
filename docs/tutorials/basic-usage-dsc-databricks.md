# Basic usage of dsc-databricks

In this tutorial you complete a full round trip on a single resource: create
a secret scope, read it back, and delete it again. By the end you will
recognise the shape of every operation the resources support.

Working on one instance at a time keeps the moving parts visible. Once the
round trip makes sense, [Manage secrets with a DSC configuration
document][02] does the same work declaratively.

## Prerequisites

- A working installation. Follow [Installation][00] first: the DSC engine,
  the `dsc-databricks` binary, and the generated manifests.
- A Databricks workspace and a personal access token for it. To create a
  token, open your workspace, select your profile, and go to
  **Settings** > **Developer** > **Access tokens**.
- PowerShell 7 or later, or a POSIX shell. The commands below use
  PowerShell.

Confirm the engine can see the resources before you start:

```powershell
dsc resource list LibreDsc.Databricks/*
```

```text
Type                                    Kind      Version  Capabilities
-------------------------------------------------------------------------
LibreDsc.Databricks/AccountUser         resource  0.1.0    gs-t-d---e---
LibreDsc.Databricks/Catalog             resource  0.1.0    gs-t-d---e---
...
LibreDsc.Databricks/WorkspaceSetting    resource  0.1.0    gs---d---e---
```

An empty list means the engine has not found the manifests. Go back to
[Installation][00] before continuing.

## Step 1: Create a secret scope

`set` moves an instance toward the state you describe. Nothing exists yet,
so this creates the scope:

```powershell
dsc resource set -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

The output shows the state before and after, and which properties changed:

```yaml
beforeState:
  scope: dsc-tutorial
  _exist: false
afterState:
  scope: dsc-tutorial
  backend_type: DATABRICKS
  _exist: true
changedProperties:
- backend_type
```

Read that carefully, because it is the shape every `set` returns.
`beforeState` reports `_exist: false`, which is how a missing instance looks
rather than an error. `afterState` comes back fresh from the API, so it
carries `backend_type`, a value the server chose and you never supplied.

## Step 2: Read it back

`get` reports the current state of one instance. It needs only the
properties that identify it:

```powershell
dsc resource get -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

```yaml
actualState:
  scope: dsc-tutorial
  backend_type: DATABRICKS
  _exist: true
```

## Step 3: Run set again

Run the exact command from step 1 a second time:

```powershell
dsc resource set -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

```yaml
beforeState:
  scope: dsc-tutorial
  backend_type: DATABRICKS
  _exist: true
afterState:
  scope: dsc-tutorial
  backend_type: DATABRICKS
  _exist: true
changedProperties: []
```

Nothing happened, and `changedProperties` is empty. That is idempotence. You
describe the end state, and applying it twice costs you nothing the second
time.

## Step 4: Delete it

`delete` removes the instance:

```powershell
dsc resource delete -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

Run the `get` command from step 2 again. The scope now reports as absent
(or not existent):

```yaml
actualState:
  scope: dsc-tutorial
  _exist: false
```

Delete the scope a second time and the command still succeeds. Removing
something that is already gone is not an error, because the outcome you
asked for is the outcome you have.

## What you learned

- `get`, `set` and `delete` each act on one resource instance, taking JSON
  in and returning JSON out.
- `_exist` reports presence as part of state. A missing instance is a
  normal answer, not a failure.
- `set` returns `beforeState`, `afterState` and `changedProperties`, and
  running it twice changes nothing the second time.
- The after-state is read back from the API, so server-computed values such
  as `backend_type` appear without you supplying them.

## Next steps

- [Manage secrets with a DSC configuration document][02] — the same work,
  declared in one document instead of typed one command at a time.
- [How to preview changes with what-if][03] — see what a change would do
  before it does it.
- [SecretScope reference][01] — every property of the resource you just
  used.

<!-- Link references -->
[00]: ../getting-started/index.md
[01]: ../reference/resources/secret-scope.md
[02]: manage-secrets-with-a-configuration.md
[03]: ../how-to/preview-changes-with-what-if.md
