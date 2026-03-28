# Contributing to dsc-databricks

Thank you for your interest in contributing to dsc-databricks! This document
provides guidelines and information for contributors.

## Getting started

1. Fork the repository and clone your fork.
2. Create a feature branch from `main`.
3. Make your changes.
4. Submit a pull request.

## Prerequisites

- [Go 1.25+][00]
- [PowerShell 7+][01] (for building and
  testing)
- A Databricks workspace with valid credentials (for integration tests)

## Building

```powershell
.\build.ps1
```

This compiles the Go binary and generates the DSC resource manifest.

## Running tests

Integration tests require Databricks authentication to be configured
(see the [wiki][02] for details).

```powershell
.\build.ps1 -RunTests
```

## Changelog

This project uses a changelog-driven release process inspired by the
[Databricks CLI][03].

- **Do not edit `CHANGELOG.md` directly.** It is managed automatically by the
  release workflow.
- When your PR introduces a user-facing change, add a bullet point to the
  appropriate section in `NEXT_CHANGELOG.md`:

  | Section            | When to use                            |
  |--------------------|----------------------------------------|
  | Notable Changes    | Breaking changes or major new features |
  | Bug Fixes          | Bug fixes                              |
  | Dependency Updates | Dependency version bumps               |

- If no section fits, add a new `### Section Name` heading.

### Example entry

```markdown
### Bug Fixes
* Fix incorrect state comparison for SecretAcl resources ([#42](https://github.com/LibreDsc/dsc-databricks/pull/42))
```

## Pull request guidelines

- Keep PRs focused on a single change.
- Include a clear description of what the PR does and why.
- Add or update tests where applicable.
- Ensure `go vet` and `golangci-lint` pass before submitting.
- Update `NEXT_CHANGELOG.md` for user-facing changes.

## Code style

- Follow idiomatic Go conventions (`gofmt`, `goimports`).
- See [.github/copilot-instructions.md][04] and
  [.github/instructions/dsc-databricks.instructions.md][05]
  for project-specific coding guidelines.

<!-- Link references -->
[00]: https://go.dev/dl/
[01]: https://github.com/PowerShell/PowerShell
[02]: https://github.com/LibreDsc/dsc-databricks/wiki
[03]: https://github.com/databricks/cli
[04]: .github/copilot-instructions.md
[05]: .github/instructions/dsc-databricks.instructions.md
