# About Microsoft DSC resources and this module

This article explains how Microsoft DSC resources work and how
`dsc-databricks` is built on that model. Understanding the model helps you
predict how the resources behave — why a missing instance is not an error,
why some resources advertise `test` and others do not, and where the
manifests fit in.

## The Microsoft DSC resource model

Microsoft DSC treats a resource as an external command with a simple
contract: JSON state in, JSON state out. A *manifest* file describes the
resource to the engine — its type name, its JSON schema, and which
operations it supports. The engine discovers manifests through the
`DSC_RESOURCE_PATH` environment variable and shells out to the described
command for each operation.

The set of operations a resource supports is called its *capabilities*.
Whatever a resource implements is what its manifest advertises; there is no
separate configuration. This is why capability lists differ between
resources: they reflect what each underlying API can meaningfully do.

## How dsc-databricks is built

`dsc-databricks` is a single Go binary that bundles all the resources. The
DSC protocol plumbing comes from the [dsc-go-rdk][00] library:

- Argument parsing
- Output framing
- Schema generation
- Manifest generation
- Exit codes

This repository only implements the Databricks-specific behavior on top of
the Databricks SDK for Go.

The build generates one manifest per resource rather than one aggregate
file. Per-resource manifests let the engine list, cache, and invoke each
resource independently, and they make additions visible as new files
instead of edits to a shared document.

## The _exist convention

Presence is part of state, not a separate operation. Every state carries an
[`_exist`][04] property: `get` reports a missing instance with
`_exist: false` and exit code 0, and a configuration marks an instance for
removal by declaring `_exist: false`. The engine routes such declarations to
the resource's `delete` operation.

The reason for this design is composability. Because absence is ordinary
state, a single configuration document can create some instances and
remove others in one apply, and a what-if can predict both.

## Synthetic test versus custom test

By default, the engine synthesizes `test` by calling `get` and comparing
properties literally. That is correct for most resources, so most do not
implement `test` themselves.

Ten resources implement a custom `test` because literal comparison would
produce wrong answers. Two examples: [`Grant`][05] compares privilege lists
as sets, because `["SELECT", "MODIFY"]` and `["MODIFY", "SELECT"]` are the
same grant; [`StorageCredential`][06] ignores server-computed nested
identifiers and write-only secrets, because the API never returns them.
The trade-off
is deliberate: custom comparison logic is only written where the domain
requires it, and everywhere else the engine's synthesis keeps behavior
uniform.

## Where to go next

- [Resources][01]
- [About what-if predictions][02]
- [Basic usage of dsc-databricks][03]

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-go-rdk
[01]: ../reference/index.md
[02]: about-what-if-predictions.md
[03]: ../tutorials/basic-usage-dsc-databricks.md
[04]: https://github.com/PowerShell/DSC/blob/23330fdfc7ced2b087cb7c2e9de1d3a2dd697f69/docs/reference/schemas/resource/properties/exist.md
[05]: ../reference/resources/grant.md
[06]: ../reference/resources/storage-credential.md
