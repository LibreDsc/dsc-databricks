# Command line

The resources are operated in two ways: through the Microsoft DSC engine
(`dsc`), which is the primary interface, or by invoking the
`dsc-databricks` binary directly.

## DSC engine commands

The DSC engine discovers the resources through `DSC_RESOURCE_PATH`. The
following commands operate on individual resource instances:

```text
dsc resource list [LibreDsc.Databricks/*]
dsc resource schema -r <type>
dsc resource get    -r <type> --input <json>
dsc resource set    -r <type> --input <json>
dsc resource test   -r <type> --input <json>
dsc resource delete -r <type> --input <json>
dsc resource export -r <type>
```

Configuration documents apply multiple resource instances in one operation:

```text
dsc config set -f <file>
dsc config set -w -f <file>
```

The `-w` flag performs a what-if prediction without changing anything. It is
only available on `dsc config set` and requires DSC v3.2 or later; there is
no `dsc resource set --what-if`.

## dsc-databricks commands

The binary exposes the same operations without the engine:

```text
dsc-databricks get|set|test|delete|export --resource <type> [--input <json>]
dsc-databricks schema   --resource <type>
dsc-databricks manifest [--resource <type>] [--out-dir <dir>]
```

`manifest` regenerates the per-resource manifest files the DSC engine uses
for discovery.

## Input and output

Input is accepted through `--input` (or `-i`) as a JSON string, or piped
through stdin. Results are emitted as compact JSON lines on stdout.
Diagnostics are emitted as JSON lines on stderr; the level is controlled by
the `DSC_TRACE_LEVEL` environment variable.

## See also

- [Environment variables][01]
- [Exit codes][02]
- [Resources][03]

<!-- Link references -->
[01]: environment-variables.md
[02]: exit-codes.md
[03]: index.md
