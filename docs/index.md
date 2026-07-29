# dsc-databricks

`dsc-databricks` is a single binary that brings your Databricks workspace
under Microsoft DSC v3: 22 resources covering Unity Catalog, compute,
identities, secrets, and workspace settings, driven by declarative JSON or
YAML configuration documents. It is built on the [dsc-go-rdk][00] library
and the Databricks SDK for Go.

**Install:** download the binary for your platform from the
[releases page][01], then follow [Get started with dsc-databricks][02].

## Tutorials

Learn by doing — guided, verified steps from zero to a working setup.

- [Get started with dsc-databricks][02]
- [Manage secrets with a DSC configuration document][03]

## How-to guides

Task-focused directions for readers who know what they want to accomplish.

- [How to authenticate to Databricks][04]
- [How to preview changes with what-if][05]
- [How to export existing resources][06]

## Reference

Precise descriptions of the machinery: resources, commands, and codes.

- [Resources][07]
- [Command line][08]
- [Environment variables][09]
- [Exit codes][10]

## Explanation

Background and reasoning — how the pieces fit and why they work this way.

- [About DSC v3 resources and this module][11]
- [About what-if predictions][12]
- [About Unity Catalog resource dependencies][13]

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-go-rdk
[01]: https://github.com/LibreDsc/dsc-databricks/releases
[02]: tutorials/get-started.md
[03]: tutorials/manage-secrets-with-a-configuration.md
[04]: how-to/authenticate.md
[05]: how-to/preview-changes-with-what-if.md
[06]: how-to/export-resources.md
[07]: reference/index.md
[08]: reference/cli.md
[09]: reference/environment-variables.md
[10]: reference/exit-codes.md
[11]: explanation/about-dsc-v3-resources.md
[12]: explanation/about-what-if-predictions.md
[13]: explanation/about-unity-catalog-dependencies.md
