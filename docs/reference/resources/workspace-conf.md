# WorkspaceConf

Manages individual keys in the Databricks workspace configuration, the
advanced settings exposed through the workspace conf API. An instance is one
key and its value.

For the newer typed settings API, use [WorkspaceSetting][01] instead.

Type: `LibreDsc.Databricks/WorkspaceConf`

## Syntax

```json
{
  "key": "enableTokensConfig",
  "value": "true",
  "_exist": true
}
```

## Properties

| Name     | Type    | Required | Description                                         |
|----------|---------|----------|-----------------------------------------------------|
| `key`    | string  | Yes      | Configuration key name.                             |
| `value`  | string  | No       | The value, always as a string. Required for `set`.  |
| `_exist` | boolean | No       | Whether the instance should exist. Default: `true`. |

Values are always strings, including booleans and integers: use `"true"`
rather than `true`, and `"90"` rather than `90`.

Common keys:

| Key                                       | Description                                     |
|-------------------------------------------|-------------------------------------------------|
| `enableTokensConfig`                      | Enable or disable personal access tokens.       |
| `maxTokenLifetimeDays`                    | Maximum personal access token lifetime in days. |
| `enableIpAccessLists`                     | Enable IP access lists.                         |
| `enableDbfsFileBrowser`                   | Show the DBFS file browser in the UI.           |
| `enableWebTerminal`                       | Enable the web terminal on clusters.            |
| `enableWorkspaceFilesystem`               | Enable the workspace filesystem.                |
| `enableDeprecatedGlobalInitScripts`       | Enable deprecated global init scripts.          |
| `enableDeprecatedClusterNamedInitScripts` | Enable deprecated cluster-named init scripts.   |

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`.

A configuration key always exists on a workspace, so there is nothing to
remove: `delete` is a no-op that succeeds without changing anything. To undo
a change, `set` the key back to the value you want rather than declaring
`_exist: false`.

## Example

Cap the lifetime of personal access tokens:

```json
{
  "key": "maxTokenLifetimeDays",
  "value": "90"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/WorkspaceConf --input '{"key":"maxTokenLifetimeDays","value":"90"}'
```

## See also

- [WorkspaceSetting][01]
- [Command line][02]
- [Exit codes][03]

<!-- Link references -->
[01]: workspace-setting.md
[02]: ../cli.md
[03]: ../exit-codes.md
