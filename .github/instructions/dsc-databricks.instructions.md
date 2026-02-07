---
description: 'Instructions for developing DSC resources in the dsc-databricks project'
applyTo: 'internal/**/*.go,cmd/**/*.go'
---

# dsc-databricks - AI Coding Agent Instructions

## Project Overview

This repository contains Microsoft DSC v3 resources for managing Databricks workspace resources. The project uses a **single Go executable** (`dsc-databricks`) that bundles all resources and implements the standard DSC interface through the Cobra CLI framework and the Databricks SDK for Go.

**Module:** `github.com/LibreDsc/dsc-databricks`

**Major Components:**

1. **DSC Framework** (`internal/dsc/`) - Core framework for building DSC resources
2. **Resource Handlers** (`internal/resources/`) - Databricks-specific resource implementations
3. **CLI Entry Point** (`cmd/main.go`) - Main executable entry point

**Available Resources:**

- `LibreDsc.Databricks/User` - Manage Databricks workspace users
- `LibreDsc.Databricks/Secret` - Manage Databricks secrets
- `LibreDsc.Databricks/SecretScope` - Manage Databricks secret scopes
- `LibreDsc.Databricks/SecretAcl` - Manage Databricks secret ACLs

**Resource Naming Convention:**

- All resources use the prefix `LibreDsc.Databricks/<Name>`
- Resource types use PascalCase after the slash (e.g., `SecretScope`, `SecretAcl`)

## Architecture Pattern

### Single Executable Structure

```
cmd/
└── main.go                     # Entry point: creates root command and executes

internal/
├── dsc/                        # Framework package
│   ├── dsc.go                  # Root command, interfaces, registry, manifest builder
│   ├── commands.go             # CLI commands: get, set, test, delete, export, schema, manifest
│   ├── helpers.go              # Input parsing, validation, metadata builder, state comparison
│   ├── schema.go               # JSON Schema generation from Go reflect types
│   ├── results.go              # GetResult, SetResult, TestResult types
│   ├── logger.go               # Structured JSON logging to stderr
│   └── exitcodes.go            # Exit code constants and error types
└── resources/                  # Resource handlers
    ├── doc.go                  # Package documentation
    ├── user.go                 # LibreDsc.Databricks/User handler
    └── secret.go               # Secret, SecretScope, SecretAcl handlers
```

### Resource Registration

Resources self-register via `init()` functions in the `resources` package. The `cmd/main.go` imports the `resources` package with a blank import (`_`) to trigger registration:

```go
// cmd/main.go
import (
    "github.com/LibreDsc/dsc-databricks/internal/dsc"
    _ "github.com/LibreDsc/dsc-databricks/internal/resources"
)
```

### Critical Interface Pattern

All resources implement `dsc.ResourceHandler`:

```go
type ResourceHandler interface {
    Get(ctx ResourceContext, input json.RawMessage) (*GetResult, error)
    Set(ctx ResourceContext, input json.RawMessage) (*SetResult, error)
    Test(ctx ResourceContext, input json.RawMessage) (*TestResult, error)
    Delete(ctx ResourceContext, input json.RawMessage) error
    Export(ctx ResourceContext) ([]any, error)
}
```

All five methods must be implemented. This corresponds to the DSC v3 operations:

- **Get** - Retrieve the current state of a resource instance
- **Set** - Apply the desired state (create or update)
- **Test** - Compare desired state against actual state, report differences
- **Delete** - Remove the resource instance
- **Export** - Enumerate all instances of the resource type

### Result Types

```go
// Get returns the current actual state
type GetResult struct {
    ActualState any `json:"actualState"`
}

// Set returns before/after state and what changed
type SetResult struct {
    BeforeState       any      `json:"beforeState"`
    AfterState        any      `json:"afterState"`
    ChangedProperties []string `json:"changedProperties,omitempty"`
}

// Test returns whether resource is in desired state and what differs
type TestResult struct {
    DesiredState        any      `json:"desiredState,omitempty"`
    ActualState         any      `json:"actualState"`
    InDesiredState      bool     `json:"inDesiredState"`
    DifferingProperties []string `json:"differingProperties,omitempty"`
}
```

## DSC Canonical Properties

DSC defines canonical properties with shared semantics across all resources. These always start with an underscore (`_`):

### `_exist` (bool)

Controls whether the resource instance should exist. The DSC engine routes operations based on this:

- `_exist=true` (default) → engine calls `Set()`
- `_exist=false` → engine calls `Delete()`

**Implementation rules:**

- Every State struct must include `Exist bool \`json:"_exist"\``
- In `Get()`: set `Exist: false` when the resource is not found; set `Exist: true` when it exists
- In `Get()`: return `_exist: false` with no error when a resource is not found — do NOT return an error for not-found
- In `Set()`: do NOT check `_exist` — the DSC engine handles routing
- In `Test()`: set `DesiredState.Exist = true` as the default desired state. Only implement test under these conditions:
  - Semantic equivalence — Property values need interpretation rather than literal comparison (e.g., latest → a resolved version number, relative paths vs. absolute paths).
  - Threshold / range comparisons — Desired state expresses a minimum, maximum, or range rather than an exact value (e.g., "memory ≥ 4 GB").
  - Case-insensitive or culture-aware comparison — The domain naturally treats values as case-insensitive (e.g., DNS names, registry value names).
  - Computed or derived properties — The actual state is returned in a normalized form that differs from the input representation.
  - Partial collection matching — You need subset checking, wildcard matching, or ordering-matters semantics on arrays.
  - Performance — When get is expensive (e.g., network calls, large queries), a dedicated test command may be able to determine compliance more efficiently without materializing the full state.
  - Side-effect validation — Compliance depends on runtime checks beyond property comparison (e.g., verifying a service responds to health checks, a certificate hasn't expired, etc.).

### State Structs

Each resource defines a State struct that represents the serializable state:

```go
type UserState struct {
    ID          string `json:"id"`
    UserName    string `json:"user_name"`
    DisplayName string `json:"display_name,omitempty"`
    Active      bool   `json:"active"`
    Exist       bool   `json:"_exist"`
}
```

**Rules for State structs:**

- Use JSON tags matching the Databricks SDK field names (snake_case)
- Always include `_exist` as a bool field
- Use `omitempty` for truly optional fields (not for `_exist`)
- State structs are separate from SDK request types — map between them explicitly

## Resource Manifest

The manifest is generated by `buildManifest()` in `dsc.go` and follows the [DSC v3 manifest schema](https://aka.ms/dsc/schemas/v3/bundled/resource/manifest.json):

```json
{
  "$schema": "https://aka.ms/dsc/schemas/v3/bundled/resource/manifest.json",
  "type": "LibreDsc.Databricks/User",
  "version": "0.1.0",
  "get": {
    "executable": "dsc-databricks",
    "args": ["get", "--resource", "LibreDsc.Databricks/User", {"jsonInputArg": "--input", "mandatory": true}]
  },
  "set": {
    "executable": "dsc-databricks",
    "args": ["set", "--resource", "LibreDsc.Databricks/User", {"jsonInputArg": "--input", "mandatory": true}],
    "return": "stateAndDiff"
  },
  "test": {
    "executable": "dsc-databricks",
    "args": ["test", "--resource", "LibreDsc.Databricks/User", {"jsonInputArg": "--input", "mandatory": true}],
    "return": "stateAndDiff"
  },
  "delete": {
    "executable": "dsc-databricks",
    "args": ["delete", "--resource", "LibreDsc.Databricks/User", {"jsonInputArg": "--input", "mandatory": true}]
  },
  "export": {
    "executable": "dsc-databricks",
    "args": ["export", "--resource", "LibreDsc.Databricks/User"]
  },
  "exitCodes": { "0": "Success", "1": "Error", ... },
  "schema": { "embedded": { ... } }
}
```

**Key manifest details:**

- `set.return: "stateAndDiff"` — Set returns before/after state plus changed properties
- `test.return: "stateAndDiff"` — Test returns desired/actual state plus differing properties
- The `executable` is always `dsc-databricks`
- The `jsonInputArg` tells the DSC engine to pass JSON via `--input`

## JSON Schema Generation

Schemas are generated at runtime from Go `reflect.Type` using `GenerateSchemaWithOptions()`. The generator:

- Reads struct field types and JSON tags to produce JSON Schema properties
- Adds `_exist` as a boolean property with `default: true` to every schema
- Applies custom property descriptions from `PropertyDescriptions` maps
- Uses `$schema: "https://json-schema.org/draft/2020-12/schema"`

## Creating a New Resource

### Step-by-Step

1. **Create a new `.go` file** in `internal/resources/` (or add to an existing file for related resources)

2. **Define the State struct** with JSON tags and `_exist`:

    ```go
    type ClusterState struct {
        ClusterID   string `json:"cluster_id"`
        ClusterName string `json:"cluster_name"`
        SparkVersion string `json:"spark_version,omitempty"`
        NumWorkers  int    `json:"num_workers,omitempty"`
        State       string `json:"state,omitempty"`
        Exist       bool   `json:"_exist"`
    }
    ```

3. **Define the handler struct** (stateless — no fields needed):

    ```go
    type ClusterHandler struct{}
    ```

4. **Define property descriptions** for the schema:

    ```go
    var clusterPropertyDescriptions = dsc.PropertyDescriptions{
        "cluster_id":    "The unique identifier for the cluster.",
        "cluster_name":  "The name of the cluster.",
        "spark_version": "The Spark version of the cluster.",
        "num_workers":   "Number of worker nodes in the cluster.",
        "state":         "The current state of the cluster.",
    }
    ```

5. **Define metadata** using `dsc.BuildMetadata`:

    ```go
    func clusterMetadata() dsc.ResourceMetadata {
        return dsc.BuildMetadata(dsc.MetadataConfig{
            ResourceType:      "LibreDsc.Databricks/Cluster",
            Description:       "Manage Databricks clusters",
            SchemaDescription: "Schema for managing Databricks clusters.",
            ResourceName:      "cluster",
            Tags:              []string{"databricks", "cluster", "compute"},
            Descriptions:      clusterPropertyDescriptions,
            SchemaType:        reflect.TypeOf(compute.ClusterDetails{}),
        })
    }
    ```

6. **Register in `init()`**:

    ```go
    func init() {
        dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Cluster", &ClusterHandler{}, clusterMetadata())
    }
    ```

7. **Implement all five ResourceHandler methods** following the patterns below.

### Get Pattern

```go
func (h *ClusterHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
    req, err := dsc.UnmarshalInput[compute.GetClusterRequest](input)
    if err != nil {
        return nil, err
    }
    if err := dsc.ValidateRequired(dsc.RequiredField{Name: "cluster_id", Value: req.ClusterId}); err != nil {
        return nil, err
    }

    cmdCtx, w, err := getWorkspaceClient(ctx)
    if err != nil {
        return nil, err
    }

    cluster, err := w.Clusters.Get(cmdCtx, req)
    if err != nil {
        // Not found → return _exist: false, NOT an error
        return &dsc.GetResult{ActualState: ClusterState{ClusterID: req.ClusterId, Exist: false}}, nil
    }

    return &dsc.GetResult{ActualState: clusterToState(cluster)}, nil
}
```

**Critical:** When a resource is not found, return `Exist: false` with a `nil` error. Do NOT return an error for not-found.

### Set Pattern

```go
func (h *ClusterHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
    req, err := dsc.UnmarshalInput[compute.CreateCluster](input)
    if err != nil {
        return nil, err
    }

    // Capture before state
    beforeState, _ := h.getCurrentState(ctx, req.ClusterName)

    // Perform create or update via Databricks SDK
    cmdCtx, w, err := getWorkspaceClient(ctx)
    if err != nil {
        return nil, err
    }
    // ... create or update logic ...

    // Capture after state
    afterState, _ := h.getCurrentState(ctx, req.ClusterName)
    changedProps := dsc.CompareStates(beforeState, afterState)

    return &dsc.SetResult{
        BeforeState:       beforeState,
        AfterState:        afterState,
        ChangedProperties: changedProps,
    }, nil
}
```

**Key:** Set always returns before/after state and changed properties. The DSC engine uses this for drift reporting.

### Test Pattern

```go
func (h *ClusterHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
    req, err := dsc.UnmarshalInput[compute.CreateCluster](input)
    if err != nil {
        return nil, err
    }

    actualState, err := h.getCurrentState(ctx, req.ClusterName)
    if err != nil {
        return nil, err
    }

    desiredState := ClusterState{ClusterName: req.ClusterName, Exist: true}
    // Map other desired properties from req...

    differing := dsc.CompareStates(desiredState, actualState)
    inDesiredState := len(differing) == 0

    return &dsc.TestResult{
        DesiredState:        desiredState,
        ActualState:         actualState,
        InDesiredState:      inDesiredState,
        DifferingProperties: differing,
    }, nil
}
```

### Delete Pattern

```go
func (h *ClusterHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
    req, err := dsc.UnmarshalInput[compute.DeleteCluster](input)
    if err != nil {
        return err
    }

    cmdCtx, w, err := getWorkspaceClient(ctx)
    if err != nil {
        return err
    }

    return w.Clusters.Delete(cmdCtx, req)
}
```

### Export Pattern

```go
func (h *ClusterHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
    cmdCtx, w, err := getWorkspaceClient(ctx)
    if err != nil {
        return nil, err
    }

    var all []any
    clusters, err := w.Clusters.ListAll(cmdCtx, compute.ListClustersRequest{})
    if err != nil {
        return nil, err
    }
    for _, c := range clusters {
        all = append(all, clusterToState(&c))
    }
    return all, nil
}
```

### Helper Patterns

**State conversion function** — maps SDK types to State structs:

```go
func clusterToState(c *compute.ClusterDetails) ClusterState {
    return ClusterState{
        ClusterID:    c.ClusterId,
        ClusterName:  c.ClusterName,
        SparkVersion: c.SparkVersion,
        NumWorkers:   int(c.NumWorkers),
        State:        c.State.String(),
        Exist:        true,
    }
}
```

**getCurrentState helper** — reusable state fetching for Set/Test:

```go
func (h *ClusterHandler) getCurrentState(ctx dsc.ResourceContext, name string) (ClusterState, error) {
    cmdCtx, w, err := getWorkspaceClient(ctx)
    if err != nil {
        return ClusterState{Exist: false}, err
    }
    // ... lookup logic ...
    // Return Exist: false on not-found (no error)
}
```

## Logging and Exit Codes

### Structured Logging

Use `dsc.Logger` for all diagnostic output. Messages go to stderr in JSON format following the DSC v3 logging protocol:

```go
dsc.Logger.Info("creating user")
dsc.Logger.Errorf("failed to get cluster: %s", err)
dsc.Logger.Trace("API call completed")
```

Output format: `{"info":"creating user"}`, `{"error":"failed to get cluster: ..."}`, etc.

**Rules:**

- Never write diagnostic output to stdout — stdout is reserved for JSON results
- Use `Logger.Errorf` in command handlers before returning errors
- Use `Logger.Info` / `Logger.Trace` for operational visibility

### Exit Codes

| Code | Constant | Description |
| --- | --- | --- |
| 0 | `ExitSuccess` | Success |
| 1 | `ExitError` | General error |
| 2 | `ExitResourceError` | Resource raised an error |
| 3 | `ExitJSONError` | JSON serialization error |
| 4 | `ExitInvalidInput` | Invalid input |
| 5 | `ExitSchemaValidation` | Schema validation error |
| 6 | `ExitNotFound` | Resource not found |

Use `dsc.NewExitCodeError(code, err)` to wrap errors with specific exit codes when precision is needed.

## Input Handling

Resources receive JSON input either via `--input` flag or piped through stdin. The framework handles parsing via `parseInput()`. Resources receive `json.RawMessage` and unmarshal using:

```go
req, err := dsc.UnmarshalInput[SomeSDKType](input)
```

Validate required fields immediately after unmarshaling:

```go
if err := dsc.ValidateRequired(
    dsc.RequiredField{Name: "scope", Value: req.Scope},
    dsc.RequiredField{Name: "key", Value: req.Key},
); err != nil {
    return nil, err
}
```

Use `dsc.ValidateAtLeastOne` when one of several fields must be present:

```go
if err := dsc.ValidateAtLeastOne("string_value or bytes_value", req.StringValue, req.BytesValue); err != nil {
    return nil, err
}
```

## Authentication

Authentication is handled entirely by the Databricks SDK for Go. Do NOT implement custom auth logic. The SDK reads credentials from (in priority order):

1. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.)
2. `.databrickscfg` profile file
3. Azure CLI / GCP auth / other provider-specific methods

The shared `getWorkspaceClient()` helper creates a client per request:

```go
func getWorkspaceClient(ctx dsc.ResourceContext) (context.Context, *databricks.WorkspaceClient, error) {
    cmdCtx := ctx.Cmd.Context()
    if cmdCtx == nil {
        cmdCtx = context.Background()
    }
    w, err := databricks.NewWorkspaceClient()
    if err != nil {
        return nil, nil, fmt.Errorf("failed to create Databricks client: %w", err)
    }
    return cmdCtx, w, nil
}
```

## CLI Commands

```bash
# Get current state
dsc-databricks get --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com"}'

# Set desired state (create or update)
dsc-databricks set --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com","display_name":"New Name"}'

# Test for drift
dsc-databricks test --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com","active":true}'

# Delete
dsc-databricks delete --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com"}'

# Export all
dsc-databricks export --resource LibreDsc.Databricks/User

# Get schema
dsc-databricks schema --resource LibreDsc.Databricks/User

# Get manifests (all or filtered)
dsc-databricks manifest
dsc-databricks manifest --resource LibreDsc.Databricks/User
```

## Testing

### E2E Tests (Pester)

E2E tests live in `tests/` and use [Pester](https://pester.dev/) v5+. They validate every resource by calling the compiled CLI binary directly.

#### Prerequisites

- PowerShell 7+ with Pester v5 installed (`Install-Module Pester -Force`)
- Built binary (`go build -o dsc-databricks.exe ./cmd`)
- Environment variables for a live Databricks workspace:
  - `DATABRICKS_HOST` — workspace URL (e.g., `https://adb-1234567890.12.azuredatabricks.net`)
  - `DATABRICKS_TOKEN` — personal access token

When either variable is missing the entire test suite is **skipped**, not failed.

#### Running Tests

```powershell
# Run all E2E tests
Invoke-Pester -Path ./tests -Output Detailed

# Run a single resource
Invoke-Pester -Path ./tests/User.Tests.ps1 -Output Detailed

# Run by tag
Invoke-Pester -Path ./tests -Tag 'SecretScope' -Output Detailed
```

#### File Layout

| File | Purpose |
|---|---|
| `tests/helpers.ps1` | Shared functions: env-var gating, CLI invocation, unique name generators |
| `tests/User.Tests.ps1` | User resource E2E tests |
| `tests/SecretScope.Tests.ps1` | SecretScope resource E2E tests |
| `tests/Secret.Tests.ps1` | Secret resource E2E tests |
| `tests/SecretAcl.Tests.ps1` | SecretAcl resource E2E tests |

#### Shared Helpers (`tests/helpers.ps1`)

Every test file must dot-source helpers in **both** `BeforeDiscovery` and `BeforeAll`:

```powershell
BeforeDiscovery {
    . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')
    $script:databricksAvailable = Initialize-DatabricksTests -ExeName $ExeName
}

Describe 'Resource' -Skip:(!$script:databricksAvailable) {
    BeforeAll {
        . (Join-Path (Split-Path $PSScriptRoot -Parent) 'tools' 'Initialize-DatabricksTests.ps1')

        $outputDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'output'
        if (Test-Path $outputDir) {
            $env:DSC_RESOURCE_PATH = $outputDir
        }
        # setup code
    }
    # ...
}
```

Key helper functions (in `tools/Initialize-DatabricksTests.ps1`):

| Function | Purpose |
|---|---|
| `Initialize-DatabricksTests` | Returns `$false` when `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, or the built binary is missing |
| `New-TestScopeName` | Returns a unique scope name like `dsc-test-scope-<random>` |
| `New-TestUserName` | Returns a unique user name like `dsc-test-<random>@example.com` |

#### Test Structure (per resource)

Follow this Context order inside each `Describe` block:

1. **Discovery** — validate `dsc resource list` finds the resource and reports correct capabilities
2. **Schema Validation** — validate `dsc resource schema` returns correct JSON Schema with `_exist` property
3. **Get Operation** — verify `_exist=false` for a resource that does not exist
4. **Set Operation – Create** — create the resource, verify `afterState._exist=true`, then confirm via a follow-up `get`
5. **Set Operation – Update** — change a mutable property, verify the change persists
6. **Test Operation** — assert `inDesiredState=true` when state matches, and `inDesiredState=false` with correct `differingProperties` when it does not
7. **Export Operation** — verify the resource appears in the export list
8. **Delete Operation** — delete the resource, verify `_exist=false`
9. **Idempotency** — set the same desired state twice, confirm no errors and state unchanged

#### Cleanup Rules

- **`AfterAll`** at the `Describe` level must delete every resource the test created
- Resources that depend on a scope (Secret, SecretAcl) should create a dedicated scope in `BeforeAll` and delete the scope in `AfterAll` (which cascades)
- Wrap cleanup calls in `try/catch` so cleanup failures never mask test results
- Use unique names (via helpers) to avoid collisions with parallel runs

#### CLI Invocation Pattern

All tests invoke the DSC v3 CLI (`dsc`) directly. The `$env:DSC_RESOURCE_PATH` must point to the `output/` directory so `dsc` can discover the resource manifests.

```powershell
# Discovery
$result = dsc resource list LibreDsc.Databricks/User | ConvertFrom-Json
$result.type | Should -Be 'LibreDsc.Databricks/User'
$result.capabilities | Should -Contain 'get'

# Get
$inputJson = @{ user_name = $testUserName } | ConvertTo-Json -Compress
$result = dsc resource get -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
$result.actualState._exist | Should -Be $true

# Set
$inputJson = @{ user_name = $testUserName; display_name = 'Test' } | ConvertTo-Json -Compress
$result = dsc resource set -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
$result.afterState._exist | Should -Be $true

# Test
$result = dsc resource test -r LibreDsc.Databricks/User --input $inputJson | ConvertFrom-Json
$result.inDesiredState | Should -Be $true

# Delete
$inputJson = @{ user_name = $testUserName } | ConvertTo-Json -Compress
dsc resource delete -r LibreDsc.Databricks/User --input $inputJson | Out-Null
$LASTEXITCODE | Should -Be 0

# Export
$result = dsc resource export -r LibreDsc.Databricks/User | ConvertFrom-Json
$result.resources | Should -Not -BeNullOrEmpty
```

#### Writing Tests for a New Resource

When adding a new resource, create `tests/<ResourceName>.Tests.ps1` by copying the pattern from an existing test file (e.g., `User.Tests.ps1`) and updating:

1. The resource type string (`LibreDsc.Databricks/<Name>`)
2. The state properties tested (matching the resource's State struct JSON tags)
3. The setup/teardown in `BeforeAll`/`AfterAll` (create any prerequisite resources)
4. Add a unique name generator to `helpers.ps1` if the resource needs one

### Unit Tests (Go)

- Place test files next to the code they test (e.g., `user_test.go` in `internal/resources/`)
- Use table-driven tests for multiple scenarios
- Test both success and error cases
- Test not-found returns `_exist: false` without error

## Build

```bash
go build -o dsc-databricks.exe ./cmd    # Windows
go build -o dsc-databricks ./cmd        # Linux/macOS
```

## Common Pitfalls

- **Returning errors for not-found** — always return `Exist: false` with `nil` error instead
- **Checking `_exist` in Set()** — the DSC engine handles routing; Set should always create/update
- **Writing to stdout in resource handlers** — stdout is for JSON results only; use `dsc.Logger` for diagnostics
- **Forgetting to register** — every new resource needs `init()` with `dsc.RegisterResourceWithMetadata`
- **Inconsistent JSON tags** — State struct tags must match the SDK field names for proper schema generation
- **Missing state comparison** — Set must capture before/after state and use `dsc.CompareStates`
- **Not validating required fields** — always validate immediately after `UnmarshalInput`
