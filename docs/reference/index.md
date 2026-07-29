# Resources

The `dsc-databricks` binary registers 22 Microsoft DSC v3 resources. Every
resource type is prefixed with `LibreDsc.Databricks/`.

## Resource types

| Type | Description | Custom test |
| ---- | ----------- | ----------- |
| [`Catalog`][10] | Manage Unity Catalog catalogs in a Databricks workspace | Yes |
| [`Schema`][11] | Manage Unity Catalog schemas in a Databricks workspace | No |
| `Volume` | Manage Unity Catalog volumes in a Databricks workspace | No |
| [`StorageCredential`][14] | Manage Unity Catalog storage credentials | Yes |
| `ServiceCredential` | Manage Unity Catalog service credentials | Yes |
| `ExternalLocation` | Manage Unity Catalog external locations | No |
| `Connection` | Manage Unity Catalog connections for Lakehouse Federation | No |
| [`Grant`][13] | Manage Unity Catalog privilege grants for a principal | Yes |
| `User` | Manage Databricks workspace users | Yes |
| `AccountUser` | Manage Databricks account-level users | Yes |
| `Group` | Manage Databricks groups | Yes |
| `ServicePrincipal` | Manage Databricks service principals | Yes |
| [`Cluster`][12] | Manage Databricks compute clusters | No |
| `ClusterPolicy` | Manage Databricks cluster policies | No |
| `Repo` | Manage Databricks Git folders (repos) | No |
| `Secret` | Manage Databricks secrets | No |
| [`SecretScope`][15] | Manage Databricks secret scopes | Yes |
| `SecretAcl` | Manage Databricks secret ACLs | Yes |
| `SqlWarehouse` | Manage Databricks SQL warehouses | No |
| `SqlWarehousePermission` | Manage Databricks SQL warehouse permissions | No |
| `WorkspaceConf` | Manage Databricks workspace configuration | No |
| `WorkspaceSetting` | Manage Databricks workspace-level settings | No |

Every resource implements the `get`, `set`, `delete`, `export`, and
`setWhatIf` capabilities. Resources marked *Yes* in the *Custom test* column
additionally implement a native `test` capability; for all other resources the
DSC engine synthesizes `test` from `get`. See
[About DSC v3 resources][01] for the difference.

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
`libredsc.databricks.<name>.dsc.resource.json`. The DSC engine discovers the
resources when the `DSC_RESOURCE_PATH` environment variable includes the
directory that contains the manifests and the `dsc-databricks` binary. See
[Environment variables][02].

<!-- Link references -->
[01]: ../explanation/about-dsc-v3-resources.md
[02]: environment-variables.md
[10]: resources/catalog.md
[11]: resources/schema.md
[12]: resources/cluster.md
[13]: resources/grant.md
[14]: resources/storage-credential.md
[15]: resources/secret-scope.md
