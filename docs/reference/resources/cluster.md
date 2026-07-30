# Cluster

Manages Databricks compute clusters. A cluster is identified by `cluster_id`
(computed on create) or by `cluster_name`; at least one of the two is
required for every operation.

Type: `LibreDsc.Databricks/Cluster`

## Syntax

```json
{
  "cluster_name": "string",
  "spark_version": "string",
  "node_type_id": "string",
  "kind": "CLASSIC_PREVIEW",
  "runtime_engine": "STANDARD | PHOTON",
  "azure_availability": "SPOT_WITH_FALLBACK_AZURE",
  "num_workers": 0,
  "autoscale_min_workers": 1,
  "autoscale_max_workers": 2,
  "autotermination_minutes": 60,
  "spark_conf": { "string": "string" },
  "custom_tags": { "string": "string" },
  "_exist": true
}
```

## Properties

| Name                      | Type    | Required | Description                                                                                                            |
|---------------------------|---------|----------|------------------------------------------------------------------------------------------------------------------------|
| `cluster_id`              | string  | No       | Canonical unique identifier. Computed on create.                                                                       |
| `cluster_name`            | string  | No       | Name of the cluster. Required when creating.                                                                           |
| `spark_version`           | string  | No       | Spark runtime version, for example `19.x-scala2.13`. Required when creating.                                           |
| `node_type_id`            | string  | No       | Node type for worker nodes, for example `Standard_D4ds_v5`.                                                            |
| `driver_node_type_id`     | string  | No       | Node type for the driver node. Defaults to `node_type_id`.                                                             |
| `kind`                    | string  | No       | Compute specification kind. Valid value: `CLASSIC_PREVIEW`. Enables next-generation features such as `is_single_node`. |
| `is_single_node`          | boolean | No       | Create a single-node cluster. Only valid with `kind: CLASSIC_PREVIEW`.                                                 |
| `azure_availability`      | string  | No       | Azure availability type. Valid values: `SPOT_AZURE`, `ON_DEMAND_AZURE`, `SPOT_WITH_FALLBACK_AZURE`.                    |
| `data_security_mode`      | string  | No       | Data security mode, for example `SINGLE_USER` or `USER_ISOLATION`.                                                     |
| `single_user_name`        | string  | No       | User name when `data_security_mode` is `SINGLE_USER`.                                                                  |
| `runtime_engine`          | string  | No       | Runtime engine: `STANDARD` or `PHOTON`.                                                                                |
| `policy_id`               | string  | No       | ID of the cluster policy used to create the cluster.                                                                   |
| `instance_pool_id`        | string  | No       | ID of the instance pool the cluster belongs to.                                                                        |
| `driver_instance_pool_id` | string  | No       | ID of the instance pool for the driver node.                                                                           |
| `num_workers`             | integer | No       | Number of worker nodes. Mutually exclusive with autoscale.                                                             |
| `autoscale_min_workers`   | integer | No       | Minimum workers when autoscaling.                                                                                      |
| `autoscale_max_workers`   | integer | No       | Maximum workers when autoscaling.                                                                                      |
| `autotermination_minutes` | integer | No       | Minutes of inactivity before auto-termination. `0` disables.                                                           |
| `enable_elastic_disk`     | boolean | No       | Enable autoscaling local storage.                                                                                      |
| `state`                   | string  | No       | Current lifecycle state, for example `RUNNING` or `TERMINATED`. Read-only.                                             |
| `state_message`           | string  | No       | Message for the most recent state transition. Read-only.                                                               |
| `spark_conf`              | object  | No       | Spark configuration key-value pairs.                                                                                   |
| `custom_tags`             | object  | No       | Additional tags for cluster resources.                                                                                 |
| `_exist`                  | boolean | No       | Whether the instance should exist. Default: `true`.                                                                    |

`set` waits for the cluster to reach the `RUNNING` state when creating.
Updating a `TERMINATED` cluster does not restart it; the new configuration
applies on the next start. Delete terminates the cluster and then removes it
permanently.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

The DSC engine synthesizes `test` from `get`. `state` and `state_message`
change over time and are never part of desired state.

## Example

Create an autoscaling Photon cluster on next-generation compute:

```json
{
  "cluster_name": "etl-nightly",
  "spark_version": "19.x-scala2.13",
  "node_type_id": "Standard_D4ds_v5",
  "kind": "CLASSIC_PREVIEW",
  "runtime_engine": "PHOTON",
  "azure_availability": "SPOT_WITH_FALLBACK_AZURE",
  "autoscale_min_workers": 1,
  "autoscale_max_workers": 4,
  "autotermination_minutes": 30
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Cluster --input (Get-Content .\cluster.json -Raw)
```

## See also

- [Exit codes][01]
- [How to export existing resources][02]

<!-- Link references -->
[01]: ../exit-codes.md
[02]: ../../how-to/export-resources.md
