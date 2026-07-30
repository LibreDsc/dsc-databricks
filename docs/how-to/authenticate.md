# How to authenticate to Databricks

The resources authenticate through the Databricks SDK unified
authentication model. Choose the method that fits your scenario: personal
access tokens for interactive use, configuration profiles for multiple
workspaces, or a Microsoft Entra service principal for automation.

## Use environment variables

To authenticate with a personal access token, set the workspace URL and the
token:

=== "PowerShell"

    ```powershell
    $env:DATABRICKS_HOST = 'https://adb-1234567890123456.7.azuredatabricks.net'
    $env:DATABRICKS_TOKEN = '<personal-access-token>'
    ```

=== "Bash"

    ```bash
    export DATABRICKS_HOST='https://adb-1234567890123456.7.azuredatabricks.net'
    export DATABRICKS_TOKEN='<personal-access-token>'
    ```

Environment variables take precedence over the configuration file.

## Use a configuration profile

To manage multiple workspaces, define profiles in `~/.databrickscfg`:

```ini
[production]
host  = https://adb-1111111111111111.1.azuredatabricks.net
token = <token-for-production>

[staging]
host  = https://adb-2222222222222222.2.azuredatabricks.net
token = <token-for-staging>
```

Select a profile with `DATABRICKS_CONFIG_PROFILE`:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = 'staging'
```

## Use Microsoft Entra authentication

To run unattended, for example in a pipeline, authenticate with a Microsoft
Entra service principal:

```powershell
$env:DATABRICKS_HOST = 'https://adb-1234567890123456.7.azuredatabricks.net'
$env:ARM_CLIENT_ID = '<application-id>'
$env:ARM_CLIENT_SECRET = '<client-secret>'
$env:ARM_TENANT_ID = '<tenant-id>'
```

Alternatively, if you are signed in with the Azure CLI, the SDK can use
that session directly — set only `DATABRICKS_HOST`.

## Authenticate for account-level resources

Account-level resources, such as `LibreDsc.Databricks/AccountUser`, target
the account console instead of a workspace. Set the account host in
addition to your credentials:

```powershell
$env:DATABRICKS_ACCOUNT_HOST = 'https://accounts.azuredatabricks.net'
```

## Troubleshooting

If operations fail with exit code 2, the Databricks API rejected the
request. Raise the diagnostic level to see the underlying error:

```powershell
$env:DSC_TRACE_LEVEL = 'debug'
dsc resource get -r LibreDsc.Databricks/SecretScope --input '{"scope":"probe"}'
```

Common causes are an expired token, a `DATABRICKS_HOST` value pointing at
the wrong workspace, and missing permissions for the operation.

## Related

- [Environment variables][01]
- [Basic usage of dsc-databricks][02]

<!-- Link references -->
[01]: ../reference/environment-variables.md
[02]: ../tutorials/basic-usage-dsc-databricks.md
