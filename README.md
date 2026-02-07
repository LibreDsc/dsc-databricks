# dsc-databricks

Microsoft DSC v3 resource provider for Databricks workspace management.

## Overview

`dsc-databricks` is a CLI tool that implements [Microsoft DSC v3](https://learn.microsoft.com/en-us/powershell/dsc/overview?view=dsc-3.0) resource semantics for managing Databricks workspace resources. It provides a single executable that can be discovered and invoked by the DSC engine.

## Available Resources

| Resource Type | Description |
| --- | --- |
| `LibreDsc.Databricks/User` | Manage Databricks workspace users |
| `LibreDsc.Databricks/Secret` | Manage Databricks secrets |
| `LibreDsc.Databricks/SecretScope` | Manage Databricks secret scopes |
| `LibreDsc.Databricks/SecretAcl` | Manage Databricks secret ACLs |
| `LibreDsc.Databricks/Repo` | Manage Databricks Git folders (repos) |

## Usage

```bash
# Get the current state of a user
dsc-databricks get --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com"}'

# Set the desired state
dsc-databricks set --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com","display_name":"New Name"}'

# Test for configuration drift
dsc-databricks test --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com","active":true}'

# Delete a resource
dsc-databricks delete --resource LibreDsc.Databricks/User --input '{"user_name":"user@example.com"}'

# Export all instances
dsc-databricks export --resource LibreDsc.Databricks/User

# Get the JSON schema for a resource
dsc-databricks schema --resource LibreDsc.Databricks/User

# Get DSC v3 manifests
dsc-databricks manifest
```

## Authentication

`dsc-databricks` uses [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth.html) via the Databricks SDK for Go. You must configure credentials **before** running any commands.

### Option 1: Environment Variables (recommended for CI/CD)

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

### Option 2: Configuration Profile (recommended for local development)

Create or edit `~/.databrickscfg`:

```ini
[DEFAULT]
host  = https://<workspace-id>.cloud.databricks.com
token = dapi...
```

To use a named profile, set the `DATABRICKS_CONFIG_PROFILE` environment variable:

```bash
export DATABRICKS_CONFIG_PROFILE="my-profile"
```

### Option 3: Azure CLI / Cloud-Specific Auth

If you are running on Azure, you can authenticate using the Azure CLI:

```bash
az login
export DATABRICKS_HOST="https://<workspace-id>.azuredatabricks.net"
```

The SDK will automatically pick up the Azure CLI credentials.

### Verify Authentication

Run a quick `get` command to confirm your credentials are working:

```bash
dsc-databricks get --resource LibreDsc.Databricks/User --input '{"user_name":"your-email@example.com"}'
```

For the full list of supported authentication methods, see the [Databricks authentication documentation](https://docs.databricks.com/en/dev-tools/auth/index.html).

## Building

```bash
go build -o dsc-databricks ./cmd
```

## License

See [LICENSE](LICENSE) for details.
