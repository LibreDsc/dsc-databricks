# Get started with dsc-databricks

In this tutorial, you install the Microsoft DSC engine and the
`dsc-databricks` resources, connect them to a Databricks workspace, and
complete a full create, read, and delete round trip on a secret scope.

## Prerequisites

- A Databricks workspace and a personal access token for it. To create a
  token, open your workspace, select your profile, and go to
  **Settings** > **Developer** > **Access tokens**.
- PowerShell 7 or later, or a POSIX shell. The commands in this tutorial use
  PowerShell.

## Step 1: Install the DSC engine

Install the Microsoft DSC engine, version 3.2 or later, with the PSDSC
module:

```powershell
Install-PSResource -Name PSDSC -Repository PSGallery
Install-DscExe -IncludePrerelease
```

Verify the installation:

```powershell
dsc --version
```

The output should look like this:

```text
dsc 3.3.0-preview.4
```

## Step 2: Install dsc-databricks

Download the archive for your platform from the [releases page][00] and
extract it to a directory, for example `C:\dsc-resources\databricks`. The
directory contains the `dsc-databricks` executable and one
`libredsc.databricks.<name>.dsc.resource.json` manifest per resource.

## Step 3: Point DSC at the resources

Set `DSC_RESOURCE_PATH` to the directory you extracted:

```powershell
$env:DSC_RESOURCE_PATH = 'C:\dsc-resources\databricks'
dsc resource list LibreDsc.Databricks/*
```

You should see all 22 resources listed:

```text
Type                                    Kind      Version  Capabilities
-------------------------------------------------------------------------
LibreDsc.Databricks/AccountUser         resource  0.1.0    gs-t-d---e---
LibreDsc.Databricks/Catalog             resource  0.1.0    gs-t-d---e---
...
LibreDsc.Databricks/WorkspaceSetting    resource  0.1.0    gs---d---e---
```

## Step 4: Set authentication

Set the workspace URL and your personal access token:

```powershell
$env:DATABRICKS_HOST = 'https://adb-1234567890123456.7.azuredatabricks.net'
$env:DATABRICKS_TOKEN = '<your-personal-access-token>'
```

Other authentication methods, such as configuration profiles and Microsoft
Entra service principals, are covered in
[How to authenticate to Databricks][01].

## Step 5: Create a secret scope

Create a secret scope named `dsc-tutorial`:

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

## Step 6: Read it back

Retrieve the scope you just created:

```powershell
dsc resource get -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

You should see the scope reported as existing:

```yaml
actualState:
  scope: dsc-tutorial
  backend_type: DATABRICKS
  _exist: true
```

## Step 7: Delete it

Remove the scope again:

```powershell
dsc resource delete -r LibreDsc.Databricks/SecretScope --input '{"scope":"dsc-tutorial"}'
```

Run the `get` command from step 6 once more. The scope is now reported as
absent:

```yaml
actualState:
  scope: dsc-tutorial
  _exist: false
```

## What you learned

- The DSC engine discovers the resources through `DSC_RESOURCE_PATH`.
- Authentication is configured entirely through environment variables.
- `dsc resource set`, `get`, and `delete` operate on single resource
  instances with JSON input, and `_exist` reports whether an instance is
  present.

## Next steps

- [Manage secrets with a DSC configuration document][02]
- [How to authenticate to Databricks][01]
- [SecretScope reference][03]

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-databricks/releases
[01]: ../how-to/authenticate.md
[02]: manage-secrets-with-a-configuration.md
[03]: ../reference/resources/secret-scope.md
