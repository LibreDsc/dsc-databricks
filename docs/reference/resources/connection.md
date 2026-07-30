# Connection

Manages Unity Catalog connections for Lakehouse Federation. A connection
describes how to reach an external data source, such as a PostgreSQL server
or a Snowflake account, so that its data can be queried through Unity
Catalog.

Type: `LibreDsc.Databricks/Connection`

## Syntax

```json
{
  "name": "string",
  "connection_type": "POSTGRESQL",
  "options": { "host": "string", "port": "string", "user": "string" },
  "properties": { "key": "value" },
  "comment": "string",
  "owner": "string",
  "read_only": false,
  "_exist": true
}
```

## Properties

| Name              | Type          | Required | Description                                                                                |
|-------------------|---------------|----------|--------------------------------------------------------------------------------------------|
| `name`            | string        | Yes      | Name of the connection.                                                                    |
| `connection_type` | string        | No       | Type of the remote source. Required when creating. Create-only.                            |
| `options`         | map of string | No       | Connection options such as `host`, `port`, `user`, `bearer_token`. Required when creating. |
| `properties`      | map of string | No       | Free-form key-value properties attached to the connection. Create-only.                    |
| `comment`         | string        | No       | Free-form description. Create-only.                                                        |
| `owner`           | string        | No       | Username of the current owner.                                                             |
| `read_only`       | boolean       | No       | Whether the connection is read-only. Create-only.                                          |
| `connection_id`   | string        | No       | Unique identifier of the connection. Read-only.                                            |
| `url`             | string        | No       | URL of the remote data source, derived from `options`. Read-only.                          |
| `metastore_id`    | string        | No       | Unique identifier of the parent metastore. Read-only.                                      |
| `_exist`          | boolean       | No       | Whether the instance should exist. Default: `true`.                                        |

Valid `connection_type` values: `BIGQUERY`, `CONFLUENCE`, `DATABRICKS`,
`DYNAMICS365`, `GA4_RAW_DATA`, `GITHUB`, `GLUE`, `HIVE_METASTORE`, `HTTP`,
`HUBSPOT`, `JDBC`, `META_MARKETING`, `MYSQL`, `ORACLE`, `OUTLOOK`,
`POSTGRESQL`, `POWER_BI`, `REDSHIFT`, `SALESFORCE`,
`SALESFORCE_DATA_CLOUD`, `SERVICENOW`, `SMARTSHEET`, `SNOWFLAKE`, `SQLDW`,
`SQLSERVER`, `TERADATA`, `UNKNOWN_CONNECTION_TYPE`, `WORKDAY_RAAS`,
`ZENDESK`.

Updates resend the whole `options` map rather than merging into it, so an
option left out of the desired state is removed. Secret option values, such
as tokens and passwords, are returned redacted by the API, which means a
configuration that specifies them reports drift on every `test`.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`. Because secret
options come back redacted, expect persistent drift on connections whose
`options` include a credential.

## Example

Create a connection to a PostgreSQL server:

```json
{
  "name": "orders-postgres",
  "connection_type": "POSTGRESQL",
  "options": {
    "host": "orders.example.com",
    "port": "5432",
    "user": "readonly"
  },
  "comment": "Read replica of the orders database"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Connection --input (Get-Content .\connection.json -Raw)
```

## See also

- [ServiceCredential][01]
- [Grant][02]
- [How to export existing resources][03]

<!-- Link references -->
[01]: service-credential.md
[02]: grant.md
[03]: ../../how-to/export-resources.md
