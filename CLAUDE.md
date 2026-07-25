# dsc-databricks — AI Coding Agent Instructions

## Project Overview

This repository contains Microsoft DSC v3 resources for managing Databricks
workspace resources. The project builds a **single Go executable**
(`dsc-databricks`) that bundles all resources. The DSC protocol plumbing (CLI
dispatch, input parsing, output framing, schema generation, manifest
generation, exit codes, logging) is provided by the
[dsc-go-rdk](https://github.com/LibreDsc/dsc-go-rdk) library; this repo only
implements Databricks-specific resource handlers on top of the Databricks SDK
for Go.

**Module:** `github.com/LibreDsc/dsc-databricks`

**Key dependencies:**

- `github.com/LibreDsc/dsc-go-rdk` — the DSC resource development kit
  (package name `dsc`, imported as `dsc "github.com/LibreDsc/dsc-go-rdk"`)
- `github.com/databricks/databricks-sdk-go` — all Databricks API access and
  authentication

**Layout:**

```text
cmd/
└── main.go                     # NewManager + RegisterAll + Main
internal/
└── resources/                  # All resource handlers (package resources)
    ├── doc.go                  # Package documentation
    ├── register.go             # RegisterAll(m) — the registration point
    ├── clients.go              # workspaceClient() / accountClient()
    ├── validate.go             # requireFields / requireAtLeastOne
    ├── messages.go             # Msg* log format constants
    ├── iam_helpers.go          # UserComplexValue, SCIM helpers
    └── <resource>.go           # One file per resource (secret.go holds 3)
tests/                          # Pester E2E suites (one per resource)
tools/Initialize-DatabricksTests.ps1
build.ps1                       # Build + manifest generation into output/
```

**Available resources** (all prefixed `LibreDsc.Databricks/`, PascalCase
after the slash): User, AccountUser, Group, ServicePrincipal, Catalog,
Cluster, ClusterPolicy, Repo, Secret, SecretScope, SecretAcl, SqlWarehouse,
SqlWarehousePermission, WorkspaceConf, WorkspaceSetting.

## Architecture

### Hosting model

`cmd/main.go` creates a multi-resource host and delegates everything to the
rdk:

```go
func main() {
    m := dsc.NewManager("dsc-databricks",
        dsc.WithDescription("Microsoft DSC v3 resources for Databricks"))
    resources.RegisterAll(m)
    m.Main()
}
```

The rdk provides the full CLI surface:

```bash
dsc-databricks get|set|test|delete|export --resource <type> [--input <json>]
dsc-databricks schema   --resource <type>
dsc-databricks manifest [--resource <type>] [--out-dir <dir>]
```

Input arrives via `--input`/`-i` or piped stdin. Results are compact JSON
lines on stdout; diagnostics go to stderr. `--what-if` exists for set but no
resource here implements `WhatIfSettable`.

### Capability model

The rdk detects capabilities by **interface assertion on the handler**,
generic over the resource's state struct `T`:

```go
type Gettable[T any] interface {
    Get(ctx context.Context, instance T) (T, error) // REQUIRED
}
type Settable[T any] interface {
    Set(ctx context.Context, desired T) (T, error)
}
type Testable[T any] interface {
    Test(ctx context.Context, desired T) (TestResult[T], error)
}
type Deletable[T any] interface {
    Delete(ctx context.Context, instance T) error
}
type Exportable[T any] interface {
    Export(ctx context.Context, filter T) ([]T, error)
}
```

Whatever a handler implements is what its manifest advertises — there is no
separate capability configuration. Every resource in this repo implements
Get, Set, Delete, and Export; only some implement Test (see below).

**Method contracts:**

- **Get** — When the instance is missing, return the identifying input with
  `_exist: false` and a `nil` error via `dsc.NotFound(...)`. NEVER return an
  error for not-found.
- **Set** — Create or update toward the desired state, then **end with
  `return h.Get(ctx, desired)`** so the after-state is fresh from the API.
  The rdk captures the before-state itself (via Get) and computes
  `changedProperties` — do not compute diffs in Set. Do NOT check `_exist`
  in Set; the DSC engine routes `_exist: false` to Delete.
- **Test** — Only for resources that need custom comparison semantics (see
  below). Return `dsc.TestResult[T]{ActualState, DifferingProperties}`;
  `inDesiredState` is derived by the rdk from an empty diff.
- **Delete** — Deleting an instance that does not exist MUST return `nil`
  (log it, don't error). Validate identifying fields first.
- **Export** — Enumerate all instances, calling `SetExist(true)` on each
  returned state. The filter argument is the zero value when no input was
  given and is generally ignored.

## State Structs

Each resource defines **one** state struct that serves as input, output, AND
schema source:

```go
type SecretScopeState struct {
    dsc.ExistProperty
    Scope       string `json:"scope" description:"A unique name to identify the scope."`
    BackendType string `json:"backend_type,omitempty" description:"The backend type (read-only)."`
}
```

**Rules:**

- Embed `dsc.ExistProperty` (provides `Exist *bool` serialized as `_exist`,
  plus `ShouldExist()` and `SetExist(bool)`). The generated schema gets an
  `_exist` boolean with `default: true` automatically.
- JSON tags use snake_case matching the Databricks SDK field names.
- Property descriptions live in `description:"..."` struct tags (preferred)
  or `SchemaOptions.Descriptions` for cases a tag can't express.
- `enum:"a,b,c"` tags declare enums on string fields; nested enums (inside
  array items) use `SchemaOptions.Overrides` with the `setNestedEnum`
  helper.
- **Required-field rule:** a field is schema-required when it has a named
  json tag, is NOT `omitempty`, and is NOT a pointer. Override with
  `dsc:"required"` / `dsc:"optional"`. Keep identifying fields (e.g.
  `scope`, `user_name`) non-omitempty; everything else gets `,omitempty`.
- Read-only/computed fields (e.g. cluster `state`, warehouse `num_clusters`,
  setting `etag`) stay in the struct with `,omitempty` and a description
  ending in `(read-only)`.
- Write-only fields (e.g. secret `string_value`) get `,omitempty` and are
  never populated by Get.

### The `_exist` conventions (critical)

- **Every code path that returns a found instance calls
  `state.SetExist(true)` before returning** — Get, the tail Get inside Set,
  and each Export item. Because `Exist` is a pointer with `omitempty`,
  forgetting this silently drops `"_exist": true` from output, which breaks
  consumers and the Pester suites.
- Missing instances return
  `dsc.NotFound(instance, "<ResourceName>", "key=value")` — it sets
  `_exist: false` and returns `nil` error.
- Use `state.ShouldExist()` (true when `_exist` is absent or true) to branch
  create-vs-update in Set.
- `dsc.CompareStates` / `dsc.CompareAllStates` **skip canonical
  (`_`-prefixed) properties** — `changedProperties` and rdk-computed diffs
  never contain `_exist`. Custom Test implementations that need to report
  existence drift must add `"_exist"` to `DifferingProperties` explicitly.

## Resource Configuration

Each resource file defines a config function and the handler:

```go
func secretScopeConfig() dsc.ResourceConfig {
    return dsc.ResourceConfig{
        Type:        "LibreDsc.Databricks/SecretScope",
        Version:     "0.1.0",
        Description: "Manage Databricks secret scopes",
        Tags:        []string{"databricks", "secret", "scope", "workspace"},
        SetReturn:   dsc.SetReturnStateAndDiff,
        SchemaOptions: dsc.SchemaOptions{
            SchemaDescription:         "Schema for managing Databricks secret scopes.",
            AllowAdditionalProperties: true,
        },
    }
}
```

**Repo conventions:**

- `Version: "0.1.0"` for every resource.
- `SetReturn: dsc.SetReturnStateAndDiff` for every resource (manifest
  `set.return: "stateAndDiff"`).
- `TestReturn` left at its default (`state`).
- `SchemaOptions.AllowAdditionalProperties: true` for every resource — the
  rdk default emits `additionalProperties: false`, which we do not want.
- Never set `ResourceConfig.Schema` (the full-replacement escape hatch);
  always use the generated schema.
- Never set `SynthesizeTest`, `HandlesExist`, or `ImplementsPretest`.

### Registration

All resources register explicitly in `internal/resources/register.go` — no
`init()` magic:

```go
func RegisterAll(m *dsc.Manager) {
    dsc.MustRegister(m, dsc.MustResource[SecretScopeState](&SecretScopeHandler{}, secretScopeConfig()))
    // ... one line per resource ...
}
```

**Checklist for a new resource:** state struct (embed `ExistProperty`, tags)
→ handler struct with capability methods → `xConfig()` function → one
`MustRegister` line in `register.go` → Pester suite in `tests/` → unit tests
for any pure helpers → update `NEXT_CHANGELOG.md`.

## When to Implement `Testable`

**Default: do NOT implement Test.** With no `Testable` implementation the
manifest omits `test` and the DSC engine synthesizes it from Get (this is
desired for: Cluster, ClusterPolicy, Repo, SqlWarehouse,
SqlWarehousePermission, Secret, WorkspaceConf, WorkspaceSetting).

Implement `Testable` only when one of these applies:

- **Semantic equivalence** — property values need interpretation rather than
  literal comparison (e.g., `latest` → a resolved version, relative vs.
  absolute paths).
- **Threshold / range comparisons** — desired state expresses a
  minimum/maximum/range rather than an exact value.
- **Case-insensitive or culture-aware comparison** — the domain treats
  values as case-insensitive (DNS names, etc.).
- **Computed or derived properties** — actual state is normalized
  differently from the input representation.
- **Partial collection matching** — subset checking, wildcard matching, or
  ordering-matters semantics on arrays.
- **Performance** — Get is expensive and a dedicated test can determine
  compliance more cheaply.
- **Side-effect validation** — compliance depends on runtime checks beyond
  property comparison (health checks, expiry, etc.).

Resources with a custom Test: User, AccountUser, Group, ServicePrincipal,
Catalog, SecretScope, SecretAcl.

**Canonical Test pattern:**

```go
func (h *SecretScopeHandler) Test(ctx context.Context, desired SecretScopeState) (dsc.TestResult[SecretScopeState], error) {
    actual, err := h.Get(ctx, desired)
    if err != nil {
        return dsc.TestResult[SecretScopeState]{}, err
    }
    result := dsc.TestResult[SecretScopeState]{ActualState: actual}
    if !actual.ShouldExist() {
        // CompareStates skips _ props; report existence drift explicitly.
        result.DifferingProperties = []string{"_exist"}
        return result, nil
    }
    result.DifferingProperties = dsc.CompareStates(desired, actual)
    return result, nil
}
```

`dsc.CompareStates(desired, actual)` compares only keys present in the
marshaled desired state (subset semantics — `,omitempty` fields the user
didn't set are excluded); `dsc.CompareAllStates` is the full symmetric diff.

## Handler Patterns

### Get

```go
func (h *SecretScopeHandler) Get(ctx context.Context, in SecretScopeState) (SecretScopeState, error) {
    if err := requireFields(field{"scope", in.Scope}); err != nil {
        return in, err
    }
    w, err := workspaceClient()
    if err != nil {
        return in, err
    }
    dsc.Logger.Debugf(MsgLookup, "SecretScope", "scope="+in.Scope)
    scopes := w.Secrets.ListScopes(ctx)
    for {
        scope, err := scopes.Next(ctx)
        if err != nil {
            break
        }
        if scope.Name == in.Scope {
            state := SecretScopeState{Scope: scope.Name, BackendType: scope.BackendType.String()}
            state.SetExist(true)
            return state, nil
        }
    }
    dsc.Logger.Infof(MsgNotFound, "SecretScope", "scope="+in.Scope)
    return dsc.NotFound(SecretScopeState{Scope: in.Scope}, "SecretScope", "scope="+in.Scope)
}
```

### Set

```go
func (h *SecretScopeHandler) Set(ctx context.Context, desired SecretScopeState) (SecretScopeState, error) {
    current, err := h.Get(ctx, desired)
    if err != nil {
        return desired, err
    }
    w, err := workspaceClient()
    if err != nil {
        return desired, err
    }
    if !current.ShouldExist() {
        dsc.Logger.Infof(MsgCreate, "SecretScope", "scope="+desired.Scope)
        if err := w.Secrets.CreateScope(ctx, workspace.CreateScope{Scope: desired.Scope}); err != nil {
            return desired, err
        }
    } else {
        dsc.Logger.Debugf(MsgAlreadyExists, "SecretScope", "scope="+desired.Scope)
        // update mutable properties here when the API supports it
    }
    return h.Get(ctx, desired)
}
```

SDK request construction happens inside the handler (or in a pure
`buildXxxRequest(*State)` helper for complex resources like Cluster) — state
structs never unmarshal into SDK types directly.

### Delete

```go
func (h *SecretScopeHandler) Delete(ctx context.Context, in SecretScopeState) error {
    if err := requireFields(field{"scope", in.Scope}); err != nil {
        return err
    }
    current, err := h.Get(ctx, in)
    if err != nil {
        return err
    }
    if !current.ShouldExist() {
        return nil // deleting an absent instance succeeds
    }
    w, err := workspaceClient()
    if err != nil {
        return err
    }
    dsc.Logger.Debugf(MsgDelete, "SecretScope", "scope="+in.Scope)
    return w.Secrets.DeleteScope(ctx, workspace.DeleteScope{Scope: in.Scope})
}
```

If a lookup is needed first and it misses, log and `return nil` — never
error on delete-absent.

### Export

```go
func (h *SecretScopeHandler) Export(ctx context.Context, _ SecretScopeState) ([]SecretScopeState, error) {
    w, err := workspaceClient()
    if err != nil {
        return nil, err
    }
    dsc.Logger.Debugf(MsgListAll, "SecretScope")
    var all []SecretScopeState
    scopes := w.Secrets.ListScopes(ctx)
    for {
        scope, err := scopes.Next(ctx)
        if err != nil {
            break
        }
        state := SecretScopeState{Scope: scope.Name, BackendType: scope.BackendType.String()}
        state.SetExist(true)
        all = append(all, state)
    }
    return all, nil
}
```

## Validation and Errors

Validate identifying fields at the top of Get/Set/Delete using the helpers
in `internal/resources/validate.go`:

```go
if err := requireFields(field{"scope", in.Scope}, field{"key", in.Key}); err != nil { ... }
if err := requireAtLeastOne("string_value or bytes_value", in.StringValue, in.BytesValue); err != nil { ... }
```

Both return `dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, ...)`. For other
typed failures use `dsc.NewExitCodeError(f)`; `*dsc.NotFoundError` maps to
exit code 6 automatically.

| Code | Constant | Description |
| --- | --- | --- |
| 0 | `dsc.ExitSuccess` | Success |
| 1 | `dsc.ExitError` | General error |
| 2 | `dsc.ExitResourceError` | Resource raised an error |
| 3 | `dsc.ExitJSONError` | JSON serialization error |
| 4 | `dsc.ExitInvalidInput` | Invalid input |
| 5 | `dsc.ExitSchemaValidation` | Schema validation error |
| 6 | `dsc.ExitNotFound` | Resource not found |

## Logging

Use the rdk's `dsc.Logger` for ALL diagnostics — JSON lines on stderr
(`{"info": "..."}` etc.), level controlled by `DSC_TRACE_LEVEL`
(OFF/ERROR/WARN/INFO/DEBUG/TRACE, default WARN). Never write diagnostics to
stdout; stdout is reserved for JSON results.

Message format strings are centralized in `internal/resources/messages.go`
(`MsgLookup`, `MsgNotFound`, `MsgCreate`, `MsgUpdate`, `MsgDelete`,
`MsgPut`, `MsgListAll`, `MsgAlreadyExists`, ...). Conventions: `Debugf` for
lookups/deletes/listings, `Infof` for creates/updates/not-found.

## Authentication

Handled entirely by the Databricks SDK — no custom auth logic. Credentials
come from env vars (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, ...),
`.databrickscfg`, or provider-specific methods. Use the helpers in
`internal/resources/clients.go`:

```go
w, err := workspaceClient()   // *databricks.WorkspaceClient
a, err := accountClient()     // honors DATABRICKS_ACCOUNT_HOST
```

The `context.Context` given to handler methods flows into every SDK call.

## Build

```powershell
.\build.ps1        # go build → output/, then manifest generation
```

Manifest generation runs `dsc-databricks manifest --out-dir output/`,
producing one `libredsc.databricks.<name>.dsc.resource.json` per resource.
The DSC engine discovers them via `DSC_RESOURCE_PATH=output`.

## Testing

### Go unit tests

Live next to the code (`internal/resources/*_test.go`). Test **pure logic
only** — no live SDK calls, no client mocking:

- Request builders (`buildCreateRequest` / `buildEditRequest` field mapping)
- State conversion and SCIM complex-value helpers
- Validation helpers (exit-code behavior)
- Principal matching / key computation
- `register_test.go` — the parity net: `RegisterAll` into a Manager, then
  assert resource count, manifest methods and returns per resource, and
  schema invariants (`_exist` default true, expected `required` lists, no
  `additionalProperties: false`).

Use table-driven tests. Run with `go test ./...`.

### E2E tests (Pester)

E2E suites live in `tests/` (Pester v5+), one file per resource, and drive
the real `dsc` CLI against a live workspace.

**Prerequisites:** PowerShell 7+ with Pester, a built binary in `output/`,
and `DATABRICKS_HOST` + `DATABRICKS_TOKEN`. When anything is missing the
suite is **skipped**, not failed.

```powershell
Invoke-Pester -Path ./tests -Output Detailed          # all
Invoke-Pester -Path ./tests/User.Tests.ps1            # one resource
```

Every test file dot-sources `tools/Initialize-DatabricksTests.ps1` in
**both** `BeforeDiscovery` and `BeforeAll`:

```powershell
BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
}

Describe 'Resource' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) { $env:DSC_RESOURCE_PATH = $outputDir }
    }
}
```

`Initialize-DatabricksTests.ps1` also provides unique-name generators
(`New-TestScopeName`, `New-TestUserName`, `New-TestClusterName`, ...) — add
one there when a new resource needs one.

**Context order per Describe:** Discovery (`dsc resource list`,
capabilities) → Schema Validation (`_exist` present, `default: true`) → Get
(missing → `_exist=false`) → Set–Create → Set–Update → Test → Export →
Delete → Idempotency.

**Assertion caveats:**

- `changedProperties` NEVER contains `_exist` (canonical properties are
  skipped by the rdk diff) — assert on a real property or
  `-Not -BeNullOrEmpty` instead.
- `actualState._exist` / `afterState._exist` assertions work because
  handlers always `SetExist(true)` on found instances.

**Cleanup rules:** `AfterAll` deletes everything the suite created, wrapped
in try/catch so cleanup failures never mask results; dependent resources
(Secret, SecretAcl) create a dedicated scope in `BeforeAll`; use the
unique-name generators to avoid collisions.

## Common Pitfalls

- **Forgetting `SetExist(true)`** on found/exported states — `_exist`
  silently disappears from output (pointer + omitempty).
- **Returning errors for not-found in Get** — always `dsc.NotFound(...)`
  (nil error).
- **Erroring on delete-absent** — Delete must return nil when the instance
  is already gone.
- **Checking `_exist` in Set** — the DSC engine routes `_exist: false` to
  Delete; Set only creates/updates.
- **omitempty ↔ required coupling** — removing `,omitempty` from a field
  makes it schema-required; adding it makes it optional. Use
  `dsc:"required"`/`dsc:"optional"` to decouple when needed.
- **Expecting `_exist` in diffs** — `CompareStates`/`CompareAllStates` skip
  `_`-prefixed keys.
- **Writing to stdout** — diagnostics go through `dsc.Logger` (stderr) only.
- **Forgetting registration** — a new resource needs its `MustRegister` line
  in `register.go`.
- **Forgetting `AllowAdditionalProperties: true`** — the rdk default emits
  `additionalProperties: false`.
- **Skipping `NEXT_CHANGELOG.md`** — the changelog-guard workflow fails PRs
  that don't update it.
