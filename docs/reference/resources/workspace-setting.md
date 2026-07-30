# WorkspaceSetting

Manages Databricks workspace-level settings through the typed settings API.
An instance is one named setting and its value.

For the older untyped configuration keys, use [WorkspaceConf][01] instead.

Type: `LibreDsc.Databricks/WorkspaceSetting`

## Syntax

```json
{
  "setting_name": "default_namespace",
  "value": "engineering",
  "_exist": true
}
```

## Properties

| Name           | Type    | Required | Description                                                         |
|----------------|---------|----------|---------------------------------------------------------------------|
| `setting_name` | string  | Yes      | Name of the workspace setting.                                      |
| `value`        | string  | No       | The value, always as a string. Required for `set`.                  |
| `etag`         | string  | No       | Concurrency token. Populated on read and used on update. Read-only. |
| `_exist`       | boolean | No       | Whether the instance should exist. Default: `true`.                 |

Valid `setting_name` values: `aibi_dashboard_embedding_access_policy`,
`automatic_cluster_update`, `compliance_security_profile`,
`dashboard_email_subscriptions`, `default_namespace`,
`default_warehouse_id`, `disable_legacy_access`, `disable_legacy_dbfs`,
`enable_export_notebook`, `enable_notebook_table_clipboard`,
`enable_results_downloading`, `enhanced_security_monitoring`,
`llm_proxy_partner_powered`, `restrict_workspace_admins`,
`sql_results_download`.

Values are always strings. Booleans use `"true"` or `"false"`, and enum
settings use the enum constant, for example `ALLOW_ALL`,
`RESTRICT_TOKENS_AND_JOB_RUN_AS`, or `ALLOW_APPROVED_DOMAINS`.

`value` is always serialized, so a setting that has never been written reads
back as an explicit empty string rather than being absent.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`.

A setting always exists on a workspace, so there is nothing to remove:
`delete` is a no-op that succeeds without changing anything. To undo a
change, `set` the setting back to the value you want rather than declaring
`_exist: false`.

Updates use `etag` for optimistic concurrency. The resource reads the
current etag before writing, so a concurrent change elsewhere fails the
update rather than silently overwriting it.

## Example

Restrict what workspace admins may do:

```json
{
  "setting_name": "restrict_workspace_admins",
  "value": "RESTRICT_TOKENS_AND_JOB_RUN_AS"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/WorkspaceSetting --input (Get-Content .\setting.json -Raw)
```

## See also

- [WorkspaceConf][01]
- [Command line][02]
- [Exit codes][03]

<!-- Link references -->
[01]: workspace-conf.md
[02]: ../cli.md
[03]: ../exit-codes.md
