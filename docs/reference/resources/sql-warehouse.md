# SqlWarehouse

Manages Databricks SQL warehouses. A warehouse is the compute that serves
SQL queries and BI tools, identified by its `name`.

Type: `LibreDsc.Databricks/SqlWarehouse`

## Syntax

```json
{
  "name": "string",
  "cluster_size": "Small",
  "warehouse_type": "PRO",
  "auto_stop_mins": 30,
  "min_num_clusters": 1,
  "max_num_clusters": 4,
  "enable_photon": true,
  "enable_serverless_compute": false,
  "_exist": true
}
```

## Properties

| Name                        | Type    | Required | Description                                                                                                      |
|-----------------------------|---------|----------|------------------------------------------------------------------------------------------------------------------|
| `name`                      | string  | No       | Logical name of the warehouse. Must be unique and under 100 characters. Identifies the instance.                 |
| `cluster_size`              | string  | No       | `2X-Small`, `X-Small`, `Small`, `Medium`, `Large`, `X-Large`, `2X-Large`, `3X-Large`, `4X-Large`, or `5X-Large`. |
| `warehouse_type`            | string  | No       | `PRO` or `CLASSIC`. Combine `PRO` with `enable_serverless_compute` for serverless.                               |
| `spot_instance_policy`      | string  | No       | `COST_OPTIMIZED` or `RELIABILITY_OPTIMIZED`.                                                                     |
| `channel`                   | string  | No       | `CHANNEL_NAME_CURRENT`, `CHANNEL_NAME_PREVIEW`, or `CHANNEL_NAME_PREVIOUS`.                                      |
| `auto_stop_mins`            | integer | No       | Idle minutes before auto-stop. Must be `0` to disable, or at least `10`. Defaults to `120`.                      |
| `min_num_clusters`          | integer | No       | Minimum available clusters. Greater than 0, at most `min(max_num_clusters, 30)`. Defaults to `1`.                |
| `max_num_clusters`          | integer | No       | Maximum clusters for autoscaling. At least `min_num_clusters`, at most `40`.                                     |
| `enable_photon`             | boolean | No       | Use Photon-optimized clusters. Defaults to `true`.                                                               |
| `enable_serverless_compute` | boolean | No       | Use serverless compute.                                                                                          |
| `id`                        | string  | No       | Unique identifier of the warehouse. Computed on create. Read-only.                                               |
| `state`                     | string  | No       | Lifecycle state: `STARTING`, `RUNNING`, `STOPPING`, `STOPPED`, `DELETING`, `DELETED`. Read-only.                 |
| `num_clusters`              | integer | No       | Clusters currently running for the warehouse. Read-only.                                                         |
| `_exist`                    | boolean | No       | Whether the instance should exist. Default: `true`.                                                              |

`auto_stop_mins` is always sent, so `0` explicitly disables auto-stop rather
than falling back to the server default.

`state` and `num_clusters` describe runtime behavior rather than desired
configuration. They are reported by `get` and ignored when comparing, so a
warehouse that has stopped on its own is not treated as drift.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`.

## Example

Create an autoscaling serverless warehouse:

```json
{
  "name": "analytics",
  "cluster_size": "Small",
  "warehouse_type": "PRO",
  "enable_serverless_compute": true,
  "auto_stop_mins": 30,
  "min_num_clusters": 1,
  "max_num_clusters": 4
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/SqlWarehouse --input (Get-Content .\warehouse.json -Raw)
```

## See also

- [SqlWarehousePermission][01]
- [Cluster][02]
- [How to export existing resources][03]

<!-- Link references -->
[01]: sql-warehouse-permission.md
[02]: cluster.md
[03]: ../../how-to/export-resources.md
