# DSC Databricks CLI

This project is partially forked from the official Databricks CLI and only
implements Microsoft Desired State Configuration (DSC) capabilities.

Documentation is available in the wiki.

## Installation

This CLI can be added downloaded as executable in any directory and added to
the PATH environment variable. Check out the latest available releases and
the wiki for more information how the CLI works.

## Authentication

`dsc-databricks` uses [Databricks unified authentication][00] via the Databricks
SDK for Go. Read the documentation to configure credentials **before** running
any commands.

### Option 1: Environment variables

Set the following environment variables:

```bash
export DATABRICKS_HOST="https://<workspace-id>.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

For service principal OAuth (M2M), use:

```bash
export DATABRICKS_HOST="https://<workspace-id>.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="<client-id>"
export DATABRICKS_CLIENT_SECRET="<client-secret>"
```

### Option 2: Configuration profile

Create or edit `~/.databrickscfg`:

```ini
[DEFAULT]
host  = https://<workspace-id>.cloud.databricks.com
token = dapi...
```

To use a named profile, set the `DATABRICKS_CONFIG_PROFILE` environment
variable:

```bash
export DATABRICKS_CONFIG_PROFILE="my-profile"
```

### Option 3: Azure CLI (Cloud-specific)

If you are running on Azure, you can authenticate using the Azure CLI:

```bash
az login
export DATABRICKS_HOST="https://<workspace-id>.azuredatabricks.net"
```

The SDK will automatically pick up the Azure CLI credentials.

### Verify authentication

Run a quick `export` command to confirm your credentials are working:

```bash
dsc-databricks export --resource LibreDsc.Databricks/User
```

## Building

The project has a simple build automation script that can be run using
PowerShell 7+:

```powershell
# Build the project and produce resource manifest
.\build.ps1

# Build and test (requires $env:DATABRICKS_HOST and $env:DATABRICKS_TOKEN set)
\.build.ps1 -RunTests
```

## License

See [LICENSE](LICENSE) for details.

<!-- Link reference definitions -->
[00]: https://docs.databricks.com/en/dev-tools/auth/unified-auth.html
