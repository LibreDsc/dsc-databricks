# Installation

`dsc-databricks` is a single executable. It implements the Microsoft DSC
capabilities and follows the contract to talk against the engine.

To install it, put that executable
somewhere on your `PATH`, generate the resource manifests the DSC engine
reads, and point the engine at them.

Whilsts you can call the binary on its own: `get`, `set`, `test`, `delete` and
`export` each act on one resource instance and print JSON, which suits
scripting and debugging a single resource. The Microsoft DSC engine adds the
declarative flavor on top. It reads a whole configuration document, orders
the resources by their dependencies, decides per instance whether to create,
update or delete, reports one diff for the run, and predicts all of it with
what-if before you commit. Most of this documentation assumes you drive the
resources that way.

So install both. `dsc-databricks` implements resources, not the engine that
drives them — see [What the fork keeps and drops][05] for where that line
sits.

## Step 1: Install the Microsoft DSC engine

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

!!! info "Why 3.2 or later"

    Each manifest advertises what-if through the `set` method's
    `whatIfArg`. Engines older than 3.2 do not understand that manifest
    shape and will not offer `dsc config set -w`.

## Step 2: Install Databricks DSC CLI

`dsc-databricks` is a CLI utility available on Windows, Linux, and
macOS on both `amd64` and `arm64`. Windows users can easily install
it using `winget.exe`. For the other operationg systems, it can be taken
from an archive from the [releases page][00].

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

The engine discovers resources from manifest files, not from the executable.
`dsc-databricks` writes those manifests itself, one per resource type:

```powershell
dsc-databricks manifest --out-dir C:\dsc-resources\databricks
```

This produces one `libredsc.databricks.<name>.dsc.resource.json` file per
resource type.

Where you write them decides whether you need step 4. `dsc` searches every
folder on your `PATH`, so manifests that land in a folder already on `PATH`
— next to the executable, after a manual install — need no further
configuration. Write them anywhere else and you point the engine at them
yourself.

!!! warning "winget installs still need this step"

    winget puts the binary on your `PATH` but places no manifests, and its
    shim directory is not somewhere you want to write them. Pick a directory
    of your own and follow step 4. Re-run `manifest --out-dir` after
    upgrading, so the manifests match the binary.

## (Optional) Step 4: Point the engine at the manifests

Skip this step if step 3 wrote the manifests to a folder that is already on
your `PATH`; the engine searches those folders on its own. Otherwise set
`DSC_RESOURCE_PATH` to the directory holding the manifests:

=== "PowerShell"

    ```powershell
    $env:DSC_RESOURCE_PATH = 'C:\dsc-resources\databricks'
    ```

=== "Bash"

    ```bash
    export DSC_RESOURCE_PATH=~/.local/share/dsc-resources/databricks
    ```

Always make sure the above is set permanently if you use DSC's engine.

!!! warning "`DSC_RESOURCE_PATH` replaces `PATH`"

    Once you define it, the engine searches those folders *instead of*
    `PATH`, not in addition to it. List every directory holding manifests
    you want discovered, separated by `;` on Windows and `:` elsewhere, or
    resources you previously relied on will disappear.

## Step 5: Verify

```powershell
dsc resource list LibreDsc.Databricks/*
```

All resources should be listed:

```text
Type                                    Kind      Version  Capabilities
-------------------------------------------------------------------------
LibreDsc.Databricks/AccountUser         resource  0.1.0    gs-t-d---e---
LibreDsc.Databricks/Catalog             resource  0.1.0    gs-t-d---e---
...
LibreDsc.Databricks/WorkspaceSetting    resource  0.1.0    gs---d---e---
```

An empty list means the engine did not find the manifests. Check that step 3
actually wrote files, and that their directory is either on your `PATH` or
listed in `DSC_RESOURCE_PATH`.

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

- [Basic usage of dsc-databricks][01] — a full create, read and delete
  round trip.
- [Why dsc-databricks is a trimmed Databricks CLI][04] — where the binary
  comes from and why it is shaped this way.
- [Resources][03] — every resource type and its capabilities.

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-databricks/releases
[01]: ../tutorials/basic-usage-dsc-databricks.md
[02]: ../how-to/authenticate.md
[03]: ../reference/index.md
[04]: ../explanation/about-the-databricks-cli-fork.md
[05]: ../explanation/what-the-fork-keeps-and-drops.md
