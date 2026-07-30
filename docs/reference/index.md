# Resources

The `dsc-databricks` binary registers Microsoft DSC resources. Every
resource type is prefixed with `LibreDsc.Databricks/`.

## Resource types

| Type                           | Description                                               | Custom test |
|--------------------------------|-----------------------------------------------------------|-------------|
| [`Catalog`][10]                | Manage Unity Catalog catalogs in a Databricks workspace   | Yes         |
| [`Schema`][11]                 | Manage Unity Catalog schemas in a Databricks workspace    | No          |
| [`Volume`][16]                 | Manage Unity Catalog volumes in a Databricks workspace    | No          |
| [`StorageCredential`][14]      | Manage Unity Catalog storage credentials                  | Yes         |
| [`ServiceCredential`][17]      | Manage Unity Catalog service credentials                  | Yes         |
| [`ExternalLocation`][18]       | Manage Unity Catalog external locations                   | No          |
| [`Connection`][19]             | Manage Unity Catalog connections for Lakehouse Federation | No          |
| [`Grant`][13]                  | Manage Unity Catalog privilege grants for a principal     | Yes         |
| [`User`][20]                   | Manage Databricks workspace users                         | Yes         |
| [`AccountUser`][21]            | Manage Databricks account-level users                     | Yes         |
| [`Group`][22]                  | Manage Databricks groups                                  | Yes         |
| [`ServicePrincipal`][23]       | Manage Databricks service principals                      | Yes         |
| [`Cluster`][12]                | Manage Databricks compute clusters                        | No          |
| [`ClusterPolicy`][24]          | Manage Databricks cluster policies                        | No          |
| [`Repo`][25]                   | Manage Databricks Git folders (repos)                     | No          |
| [`Secret`][26]                 | Manage Databricks secrets                                 | No          |
| [`SecretScope`][15]            | Manage Databricks secret scopes                           | Yes         |
| [`SecretAcl`][27]              | Manage Databricks secret ACLs                             | Yes         |
| [`SqlWarehouse`][28]           | Manage Databricks SQL warehouses                          | No          |
| [`SqlWarehousePermission`][29] | Manage Databricks SQL warehouse permissions               | No          |
| [`WorkspaceConf`][30]          | Manage Databricks workspace configuration                 | No          |
| [`WorkspaceSetting`][31]       | Manage Databricks workspace-level settings                | No          |

Every resource implements the `get`, `set`, `delete`, `export`, and
`setWhatIf` capabilities. Resources marked *Yes* in the *Custom test* column
additionally implement a native `test` capability; for all other resources the
DSC engine synthesizes `test` from `get`. See
[About Microsoft DSC resources][01] for the difference.

## Capabilities

`get`
:   Returns the current state of a resource instance. A missing instance is
    reported with `_exist: false` and exit code 0.

`set`
:   Creates the instance when it does not exist, or updates it toward the
    desired state. Returns the after-state and the changed properties.

`test`
:   Compares desired state against actual state and reports the differing
    properties. Only advertised by resources with comparison semantics that
    a literal property comparison cannot express.

`delete`
:   Removes the instance. Deleting an instance that does not exist succeeds.

`export`
:   Enumerates all instances of the resource type as a list of states.

`setWhatIf`
:   Predicts the outcome of `set` without changing anything. Invoked by the
    DSC engine through `dsc config set --what-if`. Requires DSC v3.2 or
    later.

## Manifest discovery

The build produces one manifest per resource, named
`libredsc.databricks.<name>.dsc.resource.json`. The engine finds them by
searching every folder on `PATH`. When `DSC_RESOURCE_PATH` is set, it
searches the folders listed there instead of `PATH`.

Each manifest names its command as plain `dsc-databricks`, so the binary
must be resolvable on `PATH` whichever discovery route you use. Keeping the
manifests alongside the binary, in one directory that is on `PATH`,
satisfies both at once. See [Environment variables][02] and
[Installation][03].

<!-- Link references -->
[01]: ../explanation/about-microsoft-dsc-resources.md
[02]: environment-variables.md
[03]: ../getting-started/index.md
[10]: resources/catalog.md
[11]: resources/schema.md
[12]: resources/cluster.md
[13]: resources/grant.md
[14]: resources/storage-credential.md
[15]: resources/secret-scope.md
[16]: resources/volume.md
[17]: resources/service-credential.md
[18]: resources/external-location.md
[19]: resources/connection.md
[20]: resources/user.md
[21]: resources/account-user.md
[22]: resources/group.md
[23]: resources/service-principal.md
[24]: resources/cluster-policy.md
[25]: resources/repo.md
[26]: resources/secret.md
[27]: resources/secret-acl.md
[28]: resources/sql-warehouse.md
[29]: resources/sql-warehouse-permission.md
[30]: resources/workspace-conf.md
[31]: resources/workspace-setting.md
