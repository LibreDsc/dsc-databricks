# Installation

`dsc-databricks` is a single executable. Installing it means putting that
executable somewhere, generating the resource manifests the DSC engine reads,
and pointing the engine at them.

You also need the Microsoft DSC engine itself — `dsc-databricks` implements
resources, not the engine that drives them. See
[What the fork keeps and drops][05] for where that line sits.

## Step 1: Install the DSC engine

Install the engine, version 3.2 or later, with the `PSDSC` module:

```powershell
Install-PSResource -Name PSDSC -Repository PSGallery
Install-DscExe -IncludePrerelease
```

Verify the installation:

```powershell
dsc --version
```

```text
dsc 3.3.0-preview.4
```

!!! note "Why 3.2 or later"

    What-if predictions are advertised through the `set` method's
    `whatIfArg`. Engines older than 3.2 do not understand that manifest
    shape and will not offer `dsc config set -w`.

## Step 2: Install dsc-databricks

Windows users can install from winget; everyone else takes an archive from
the [releases page][00], which publishes builds for Windows, Linux and macOS
on both `amd64` and `arm64` alongside a `SHA256SUMS` file.

=== "winget (Windows)"

    ```powershell
    winget install --exact --id LibreDsc.DscDatabricks
    ```

    winget places the executable on your `PATH`. Confirm it resolves:

    ```powershell
    dsc-databricks --version
    ```

=== "Manual (all platforms)"

    Download the archive for your platform and extract it, for example to
    `C:\dsc-resources\databricks` or
    `~/.local/share/dsc-resources/databricks`.

    Add the directory to your `PATH`, or call the executable by its full
    path in the next step.

## Step 3: Generate the resource manifests

The engine does not discover resources from an executable on the `PATH`. It
reads one manifest file per resource type, and `dsc-databricks` writes those
itself:

```powershell
dsc-databricks manifest --out-dir C:\dsc-resources\databricks
```

This produces one `libredsc.databricks.<name>.dsc.resource.json` file per
resource type.

!!! warning "winget installs still need this step"

    A package-managed install puts the binary on your `PATH` but does not
    place any manifests. Run `manifest --out-dir` once against a directory
    of your choosing, then use that directory in step 4. Re-run it after
    upgrading, so the manifests match the binary.

## Step 4: Point the engine at the manifests

Set `DSC_RESOURCE_PATH` to the directory holding the manifests:

=== "PowerShell"

    ```powershell
    $env:DSC_RESOURCE_PATH = 'C:\dsc-resources\databricks'
    ```

=== "Bash"

    ```bash
    export DSC_RESOURCE_PATH=~/.local/share/dsc-resources/databricks
    ```

Set it permanently — through your profile, your shell's rc file, or a
machine-level environment variable — if you do not want to repeat it per
session.

## Step 5: Verify

```powershell
dsc resource list LibreDsc.Databricks/*
```

All 22 resources should be listed:

```text
Type                                    Kind      Version  Capabilities
-------------------------------------------------------------------------
LibreDsc.Databricks/AccountUser         resource  0.1.0    gs-t-d---e---
LibreDsc.Databricks/Catalog             resource  0.1.0    gs-t-d---e---
...
LibreDsc.Databricks/WorkspaceSetting    resource  0.1.0    gs---d---e---
```

An empty list means the engine did not find the manifests — check
`DSC_RESOURCE_PATH` and that step 3 actually wrote files there.

## Step 6: Authenticate

Resources reach the workspace through the Databricks SDK for Go, which reads
its credentials from the environment:

```powershell
$env:DATABRICKS_HOST = 'https://adb-1234567890123456.7.azuredatabricks.net'
$env:DATABRICKS_TOKEN = '<your-personal-access-token>'
```

Configuration profiles, Microsoft Entra service principals and the other
supported methods are covered in
[How to authenticate to Databricks][02].

## Next steps

- [Get started with dsc-databricks][01] — a full create, read and delete
  round trip.
- [Why dsc-databricks is a trimmed Databricks CLI][04] — where the binary
  comes from and why it is shaped this way.
- [Resources][03] — every resource type and its capabilities.

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-databricks/releases
[01]: ../tutorials/get-started.md
[02]: ../how-to/authenticate.md
[03]: ../reference/index.md
[04]: ../explanation/about-the-databricks-cli-fork.md
[05]: ../explanation/what-the-fork-keeps-and-drops.md
