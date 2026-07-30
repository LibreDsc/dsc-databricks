# Why dsc-databricks is a trimmed Databricks CLI

`dsc-databricks` is partially forked from the official Databricks CLI. It
keeps the parts that talk to Databricks and discards everything built for a
human at a terminal, because a DSC resource provider is a different kind of
program from an interactive command-line tool — even though both are Go
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

The engine parses stdout. Anything else written there — a progress spinner, a
deprecation notice, a "using profile DEFAULT" hint — corrupts the result. In
`dsc-databricks` every diagnostic goes to stderr as JSON lines, gated by
`DSC_TRACE_LEVEL`, and stdout carries nothing but the result document. An
interactive CLI has the opposite instinct: tell the user what is happening.

### Nothing may block on a human

There is no terminal attached, so there is nothing to prompt. A credential
that cannot be resolved from the environment, a profile that does not exist,
a confirmation before a destructive change — each of these must fail with a
diagnostic and an [exit code][01] rather than wait for input that will never
arrive.

### Start-up cost is paid on every operation

Command trees, shell-completion machinery and telemetry initialisation are
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

Forking keeps the useful half — the SDK, the authentication chain, the Go
build and release model — and replaces the half that assumes a person is
watching. What is left is a binary whose entire surface is the DSC protocol.

## What that leaves you with

The result is deliberately unexciting to use directly. `dsc-databricks` has
no interactive mode, no output formatting options and no command for browsing
your workspace; the Databricks CLI remains the right tool for all of that. It
does one job: answer the DSC engine's questions about resources, accurately
and quickly, and change the workspace when told to.

[What the fork keeps and drops][02] lists the specifics.

## Where to go next

- [What the fork keeps and drops][02]
- [About DSC v3 resources and this module][03]
- [Command line][04]

<!-- Link references -->
[01]: ../reference/exit-codes.md
[02]: what-the-fork-keeps-and-drops.md
[03]: about-dsc-v3-resources.md
[04]: ../reference/cli.md
