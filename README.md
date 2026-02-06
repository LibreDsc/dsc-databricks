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

Authentication is handled by the [Databricks SDK for Go](https://docs.databricks.com/dev-tools/sdk-go.html). Configure via environment variables, `.databrickscfg` profile, or other supported methods.

## Building

```bash
go build -o dsc-databricks ./cmd
```

## License

See [LICENSE](LICENSE) for details.
