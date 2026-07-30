# SqlWarehousePermission

Manages one principal's permission on one Databricks SQL warehouse. An
instance is identified by the warehouse plus exactly one principal.

Type: `LibreDsc.Databricks/SqlWarehousePermission`

## Syntax

```json
{
  "warehouse_name": "string",
  "group_name": "string",
  "permission_level": "CAN_USE",
  "_exist": true
}
```

## Properties

| Name                     | Type    | Required | Description                                                                            |
|--------------------------|---------|----------|----------------------------------------------------------------------------------------|
| `warehouse_id`           | string  | No       | Unique identifier of the SQL warehouse.                                                |
| `warehouse_name`         | string  | No       | Logical name of the warehouse. Usable instead of `warehouse_id` for lookup.            |
| `user_name`              | string  | No       | User the permission applies to.                                                        |
| `group_name`             | string  | No       | Group the permission applies to.                                                       |
| `service_principal_name` | string  | No       | Application ID of the service principal the permission applies to.                     |
| `permission_level`       | string  | No       | `CAN_MANAGE`, `CAN_MONITOR`, `CAN_USE`, `CAN_VIEW`, or `IS_OWNER`. Required for `set`. |
| `_exist`                 | boolean | No       | Whether the instance should exist. Default: `true`.                                    |

Identify the warehouse with either `warehouse_id` or `warehouse_name`, and
the principal with exactly one of `user_name`, `group_name`, or
`service_principal_name`. Use the application ID for a service principal,
not its display name.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`. `set` applies
the permission for the named principal only and leaves other principals'
permissions on the warehouse alone; `delete` removes that principal's entry.

## Example

Give a group query access to a warehouse:

```json
{
  "warehouse_name": "analytics",
  "group_name": "data-analysts",
  "permission_level": "CAN_USE"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/SqlWarehousePermission --input (Get-Content .\permission.json -Raw)
```

## See also

- [SqlWarehouse][01]
- [Group][02]
- [ServicePrincipal][03]

<!-- Link references -->
[01]: sql-warehouse.md
[02]: group.md
[03]: service-principal.md
