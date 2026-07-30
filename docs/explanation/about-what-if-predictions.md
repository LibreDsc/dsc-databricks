# About what-if predictions

This article explains what a what-if prediction is, how `dsc-databricks`
computes one, and where its limits are.

## What a prediction is

A what-if operation answers the question "what would `set` do?" without
performing the `set`. The engine runs the resource with a what-if flag, and
the resource returns the state it *would* produce, plus the properties that
would change. Nothing in the workspace is created, updated, or deleted.

## Native projections versus synthetic predictions

There are two ways to produce a prediction. The engine can synthesize one
by comparing the desired state against `get` output — cheap, but blind to
anything the resource itself would compute. Or the resource can produce a
*native projection*: its own simulation of the `set` code path.

Every resource in this module implements native projections. The
projection follows the same branching as the real `set`: for an instance
that would be created, desired values carry over and server-computed values
(IDs, states, locations) stay empty; for an instance that would be updated,
the current state is the base and the desired values overlay it exactly the
way the API request would. The trade-off is maintenance — every change to
`set` behavior must be mirrored in its projection — in exchange for
predictions that match what actually happens, including subtleties such as
fields that are always enforced versus fields that are only sent when
specified.

## The whatIfArg mechanism

Earlier DSC previews modeled what-if as a separate `whatIf` operation with
its own manifest entry. That design was deprecated in favor of a
`whatIfArg`: the manifest tells the engine which extra argument to append
to the ordinary `set` command ([PowerShell/DSC#1361][00]). This module
advertises `--what-if` that way, which is why the capability appears as
`setWhatIf` in `dsc resource list` and requires DSC v3.2 or later. The
engine only exposes the flag at configuration level — `dsc config set -w` —
because predictions across multiple resources need the document's
dependency ordering to be meaningful.

## Limits

A prediction cannot know what only the server decides:

- Server-side defaults for omitted properties are not visible before the
  instance exists, so a would-create prediction leaves them empty.
- Write-only properties, such as secret values, never appear in output —
  neither in real state nor in predictions.
- A prediction validates input the same way `set` does, but it cannot
  foresee API-side rejections such as permission or quota errors.

## Where to go next

- [How to preview changes with what-if][01]
- [About Microsoft DSC resources][02]

<!-- Link references -->
[00]: https://github.com/PowerShell/DSC/issues/1361
[01]: ../how-to/preview-changes-with-what-if.md
[02]: about-microsoft-dsc-resources.md
