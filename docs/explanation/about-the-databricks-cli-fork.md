# Why dsc-databricks is a trimmed Databricks CLI

`dsc-databricks` is partially forked from the official Databricks CLI. It
keeps the parts that talk to Databricks and discards everything built for a
human at a terminal, because a DSC resource provider is a different kind of
program from an interactive command-line tool, even though both are Go
binaries that call the same API.

This article explains why that difference forces a fork rather than a wrapper.

## What the DSC engine actually does with a resource

The engine does not load a library or start a long-lived process. For each
operation on each resource instance, it runs a command, writes JSON to its
stdin or passes it as an argument, reads JSON back from its stdout, and
inspects the exit code. A configuration document with thirty resources means
thirty or more process launches.

Three consequences follow, and each of them is at odds with how a
general-purpose CLI is built.

### Stdout belongs to the protocol

The engine parses stdout. Anything else written there corrupts the result: a
progress spinner, a deprecation notice, a "using profile DEFAULT" hint. In
`dsc-databricks` every diagnostic goes to stderr as JSON lines, gated by
`DSC_TRACE_LEVEL`, and stdout carries nothing but the result document. An
interactive CLI has the opposite instinct: tell the user what is happening.

### Nothing may block on a human

There is no terminal attached, so there is nothing to prompt. A credential
that cannot be resolved from the environment, a profile that does not exist,
a confirmation before a destructive change: each of these must fail with a
diagnostic and an [exit code][01] rather than wait for input that will never
arrive.

### Start-up cost is paid on every operation

Command trees, shell-completion machinery and telemetry initialization are
cheap when they happen once per invocation and a human is waiting anyway.
Multiplied across every resource in a configuration, and again for the
before-state `get` the engine runs around each `set`, they stop being cheap.

## Why fork instead of wrap

The alternative is to keep the Databricks CLI intact and put a DSC-shaped
shim in front of it: shell out to `databricks catalogs create`, parse the
output, translate errors. That approach loses on all three counts above and
adds two more problems. Output formats intended for humans are not a stable
contract, so the shim breaks on cosmetic changes upstream. And the shim would
have to reconstruct structured errors from text, when the SDK already returns
them as typed values.

Forking keeps the useful half, namely the SDK, the authentication chain and
the Go build and release model. It replaces the half that assumes a person
is watching. What is left is a binary whose entire surface is the DSC
protocol.

## Why it is not a `databricks dsc` subcommand

Living inside the official CLI would have avoided the fork entirely, and
that was proposed. [databricks/cli#4349][05] added a `databricks dsc`
command with almost exactly the shape this binary ended up with: `get`,
`set`, `test`, `delete` and `export` behind a `--resource` flag, `--input`
for JSON, a `manifest` subcommand, and the `_exist` convention for
declarative create and delete.

Databricks declined it in January 2026. The maintainers recommend the
Terraform provider or Declarative Automation Bundles for declarative
configuration, stated they were "not invested in DSC at the moment", and
suggested that DSC integration "could merit its own dedicated CLI instead
of being natively integrated".

This project is that dedicated CLI. The cost is worth stating plainly: a
separate binary means a separate release cadence, a separate test suite,
and an SDK dependency tracked by hand rather than moving with upstream. The
technical argument above says the trim was necessary regardless. The PR
history says the separate home was never really on offer.

## What that leaves you with

The result is deliberately unexciting to use directly. `dsc-databricks` has
no interactive mode, no output formatting options and no command for browsing
your workspace; the Databricks CLI remains the right tool for all of that. It
does one job: answer the DSC engine's questions about resources, accurately
and quickly, and change the workspace when told to.

[What the fork keeps and drops][02] lists the specifics.

## Where to go next

- [What the fork keeps and drops][02]
- [About Microsoft DSC resources and this module][03]
- [Command line][04]

<!-- Link references -->
[01]: ../reference/exit-codes.md
[02]: what-the-fork-keeps-and-drops.md
[03]: about-microsoft-dsc-resources.md
[04]: ../reference/cli.md
[05]: https://github.com/databricks/cli/pull/4349
