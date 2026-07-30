# How to preview changes with what-if

A what-if operation predicts the outcome of applying a configuration
without changing anything in the workspace. Use it to review changes before
they happen. What-if requires DSC v3.2 or later and is available on
`dsc config set` or `dsc resource set`. This guide usage
`dsc config set`

## Run a what-if preview

### 1. Describe the desired state in a configuration document

```yaml
$schema: https://aka.ms/dsc/schemas/v3/bundled/config/document.json
resources:
  - name: engineering catalog
    type: LibreDsc.Databricks/Catalog
    properties:
      name: engineering
      comment: Engineering data
```

### 2. Apply with the what-if flag

```powershell
dsc config set -w -f .\catalog.dsc.yaml
```

## Read the prediction

The result carries the prediction in the same shape as a real apply:

```yaml
metadata:
  Microsoft.DSC:
    executionType: whatIf
results:
- name: engineering catalog
  result:
    beforeState:
      name: engineering
      _exist: false
    afterState:
      name: engineering
      comment: Engineering data
      _exist: true
    changedProperties:
    - comment
```

- `executionType: whatIf` confirms nothing was changed.
- `afterState` is the state `set` would produce. Server-computed values,
  such as IDs, stay empty for instances that would be created.
- `changedProperties` lists the properties that would change.

## Preview a deletion

Set `_exist: false` on the instance to preview its removal:

```yaml
resources:
  - name: engineering catalog
    type: LibreDsc.Databricks/Catalog
    properties:
      name: engineering
      _exist: false
```

## Variations

To run a prediction against the binary directly, without the DSC engine,
pass the what-if flag to `set`:

```powershell
dsc-databricks set --resource LibreDsc.Databricks/Catalog --input '{"name":"engineering"}' --what-if
```

## Related

- [Manage secrets with a DSC configuration document][01]
- [About what-if predictions][02]
- [Command line][03]

<!-- Link references -->
[01]: ../tutorials/manage-secrets-with-a-configuration.md
[02]: ../explanation/about-what-if-predictions.md
[03]: ../reference/cli.md
