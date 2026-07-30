# What the fork keeps and drops

[Why dsc-databricks is a trimmed Databricks CLI][01] explains the reasoning.
This article is the inventory: what survived the trim, what did not, and what
was added in its place. Read it when you are trying to work out whether a
Databricks CLI habit carries over.

## Kept

**The Databricks SDK for Go.** Every API call goes through it. Resources
never hand-roll HTTP requests, so pagination, retries, rate-limit handling
and typed errors come for free, and a resource is mostly a mapping between a
state struct and an SDK request.

**The whole authentication chain.** Credentials are resolved by the SDK
exactly as they are for the official CLI: `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` and their siblings, `.databrickscfg` profiles, Microsoft
Entra service principals, and the cloud-specific methods. There is no
custom authentication code in this repository at all — see
[How to authenticate to Databricks][02].

**Account-level and workspace-level clients.** Resources that operate on the
account, such as `AccountUser`, use an account client honouring
`DATABRICKS_ACCOUNT_HOST`; everything else uses a workspace client.

**The Go build and release model.** Static binaries, no runtime dependency,
cross-compiled for Windows, Linux and macOS on `amd64` and `arm64`, published
as archives with checksums.

## Dropped

**Interactive behaviour.** No prompts, no confirmations, no spinners, no
progress bars, no TTY detection, no colour. Anything that cannot be resolved
without asking is an error.

**Human-facing output.** No table renderer, no `--output` formats, no
templating. Results are compact JSON on stdout; diagnostics are JSON lines on
stderr, gated by `DSC_TRACE_LEVEL`.

**The general-purpose command surface.** Bundles, workspace file sync,
filesystem commands, notebook execution, job runs and the per-API command
groups are all absent. The commands that exist are listed in the
[command line reference][03] and there are seven of them.

**Coverage of every API.** The official CLI aims to expose the whole
platform. This binary exposes 22 resource types, chosen because they describe
durable configuration rather than transient activity — see
[Resources][04]. A job *run* is an event, not a state, so it has no resource;
a cluster policy is a state, so it does.

## Added

**The DSC v3 protocol surface**, supplied by the [dsc-go-rdk][05] library:
CLI dispatch, input parsing, output framing, JSON schema generation, manifest
generation, exit codes and logging. Resource handlers implement
`get`, `set`, `test`, `delete`, `export` and `setWhatIf`; the library turns
those into a conforming command-line contract and a manifest per resource.

**The `_exist` convention**, which makes absence part of state rather than a
separate verb — the basis for one document both creating and removing
things. [About DSC v3 resources and this module][06] covers it.

**What-if prediction on every resource**, computed by the resource itself
without calling any mutating API.

## This binary is not the DSC engine

The easiest thing to get wrong. `dsc-databricks` implements the resource side
of the protocol only. The engine, `dsc`, remains a separate tool and owns
everything above the individual resource:

| Concern | Owner |
| ------- | ----- |
| Parsing configuration documents | `dsc` |
| Ordering resources by `dependsOn` | `dsc` |
| Deciding whether to call `set` or `delete` | `dsc` |
| Capturing the before-state and diffing it | `dsc` |
| Orchestrating a what-if run | `dsc` |
| Reading and writing one resource instance | `dsc-databricks` |
| Predicting what one `set` would produce | `dsc-databricks` |

This is why installing `dsc-databricks` on its own is not enough, and why
[Installation][07] starts by installing the engine.

## Where to go next

- [Why dsc-databricks is a trimmed Databricks CLI][01]
- [About DSC v3 resources and this module][06]
- [Resources][04]

<!-- Link references -->
[01]: about-the-databricks-cli-fork.md
[02]: ../how-to/authenticate.md
[03]: ../reference/cli.md
[04]: ../reference/index.md
[05]: https://github.com/LibreDsc/dsc-go-rdk
[06]: about-dsc-v3-resources.md
[07]: ../getting-started/index.md
