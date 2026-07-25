---
description: 'Instructions for developing DSC resources in the dsc-databricks project'
applyTo: 'internal/**/*.go,cmd/**/*.go'
---

# dsc-databricks - AI Coding Agent Instructions

Authoring guidance for this repository lives in [/CLAUDE.md](../../CLAUDE.md) — a single source of truth for both Claude and Copilot.

It covers the architecture (single executable built on [dsc-go-rdk](https://github.com/LibreDsc/dsc-go-rdk)), state-struct and `_exist` conventions, the capability-interface handler patterns (Get/Set/Test/Delete/Export), when to implement a custom `Testable`, validation, logging, exit codes, authentication, build, and the Pester/Go testing conventions. Read it before changing any resource code.
