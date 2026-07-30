# Environment variables

The resources read configuration exclusively from environment variables and
the Databricks configuration file. No credentials are stored by the module.

## Authentication variables

Authentication is handled by the Databricks SDK unified authentication
model. The most common variables:

| Variable                    | Description                                                                 |
| --------------------------- | --------------------------------------------------------------------------- |
| `DATABRICKS_HOST`           | Workspace URL, for example `https://adb-123.4.azuredatabricks.net`.         |
| `DATABRICKS_TOKEN`          | Personal access token or Microsoft Entra access token.                      |
| `DATABRICKS_ACCOUNT_HOST`   | Account console URL. Used by account-level resources such as `AccountUser`. |
| `DATABRICKS_CONFIG_PROFILE` | Profile name in `~/.databrickscfg` to authenticate with.                    |
| `ARM_CLIENT_ID`             | Microsoft Entra service principal application ID.                           |
| `ARM_CLIENT_SECRET`         | Microsoft Entra service principal client secret.                            |
| `ARM_TENANT_ID`             | Microsoft Entra tenant ID.                                                  |

See [How to authenticate to Databricks][01] for complete procedures.

## Microsoft DSC engine variables

| Variable            | Description                                                                                                                                                                                                                                                                               |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DSC_RESOURCE_PATH` | Directories the DSC engine searches for resource manifests. Optional: when unset, the engine searches the folders on `PATH`. When set, it searches these folders *instead of* `PATH`, so list every directory holding manifests you need — separated by `;` on Windows and `:` elsewhere. |
| `DSC_TRACE_LEVEL`   | Diagnostic level: `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`, or `TRACE`. Default: `WARN`.                                                                                                                                                                                                   |

## Module variables

| Variable              | Description                                                                                                            |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `DSC_DATABRICKS_LANG` | Language for diagnostic messages. Falls back to `LC_ALL`, then `LANG`. English is currently the only shipped language. |

## See also

- [Command line][02]
- [Exit codes][03]

<!-- Link references -->
[01]: ../how-to/authenticate.md
[02]: cli.md
[03]: exit-codes.md
