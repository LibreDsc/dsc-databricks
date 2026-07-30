# DSC Databricks CLI

This project is partially forked from the official Databricks CLI and only
implements Microsoft Desired State Configuration (DSC) capabilities.

Documentation is available on the [documentation site][03].

## Installation

On Windows, install with winget:

```powershell
winget install --exact --id LibreDsc.DscDatabricks
```

On other platforms, download the archive for your platform from the
[releases page][00], extract it, and add the directory to the PATH
environment variable.

Either way the DSC engine still needs the resource manifests, which the
binary writes itself:

```powershell
dsc-databricks manifest --out-dir C:\dsc-resources\databricks
$env:DSC_RESOURCE_PATH = 'C:\dsc-resources\databricks'
```

See [Installation][05] for the full walkthrough, and the
[Basic usage tutorial][04] for how the CLI works.

## Releases

For each merge to the `main` branch, build artifacts are produced
automatically. Periodically a release version tag will be pushed which will
create a full [GitHub Release][00]
with binaries for all supported platforms.

## Contributing

Please check out the [Contributing to dsc-databricks][01]
guidelines.

## Change log

A full list of changes in each version can be found in the
[change log][02].

## Documentation

The documentation can be found on the
[dsc-databricks documentation site][03].

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-databricks/releases
[01]: CONTRIBUTING.md
[02]: CHANGELOG.md
[03]: https://libredsc.github.io/dsc-databricks/
[04]: https://libredsc.github.io/dsc-databricks/tutorials/basic-usage-dsc-databricks/
[05]: https://libredsc.github.io/dsc-databricks/getting-started/
