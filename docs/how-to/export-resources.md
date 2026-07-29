# How to export existing resources

Export enumerates every instance of a resource type in your workspace as
DSC resource states. Use it to inventory live state or to bootstrap a
configuration document from an existing workspace.

## Export all instances of a resource type

### 1. Run the export

```powershell
dsc resource export -r LibreDsc.Databricks/Cluster
```

The result is a configuration-document-shaped list with one entry per
instance:

```yaml
resources:
- name: LibreDsc.Databricks/Cluster-0
  type: LibreDsc.Databricks/Cluster
  properties:
    cluster_id: 0729-101122-abcdefgh
    cluster_name: etl-nightly
    spark_version: 19.x-scala2.13
    state: TERMINATED
    _exist: true
```

### 2. Save the result

```powershell
dsc resource export -r LibreDsc.Databricks/Cluster | Out-File clusters.yaml
```

## Export into a reusable configuration

Exported states include read-only properties, such as `cluster_id`,
`state`, and `metastore_id`. Before re-applying an exported document to
another workspace, remove the read-only and server-computed properties —
they either conflict or are recomputed on create. The property tables in
the [resource reference][01] mark these properties.

## Per-resource export behavior

| Resource | Behavior |
| -------- | -------- |
| `Grant` | Bounded: exports grants on metastore-level securables only (catalogs, external locations, storage credentials, service credentials, connections). Schema, table, and volume grants are not exported. |
| `Secret` | Secret values are write-only and are never exported. Re-supply `string_value` or `bytes_value` before applying. |
| `StorageCredential`, `ServiceCredential` | The `client_secret` of a service principal block is write-only and is never exported. |
| `Connection` | Secret option values, such as tokens, are returned redacted. |
| `Volume`, `Schema` | Exported by walking the catalog hierarchy; catalogs the caller cannot enumerate are skipped. |

## Variations

To export without the DSC engine, invoke the binary directly:

```powershell
dsc-databricks export --resource LibreDsc.Databricks/Cluster
```

## Related

- [Resources][01]
- [Command line][02]

<!-- Link references -->
[01]: ../reference/index.md
[02]: ../reference/cli.md
