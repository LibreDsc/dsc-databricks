# ClusterPolicy

Manages Databricks cluster policies. A cluster policy constrains the
configurations users may create, either through a policy definition you
write or by inheriting a Databricks policy family.

Type: `LibreDsc.Databricks/ClusterPolicy`

## Syntax

```json
{
  "name": "string",
  "definition": "{\"spark_version\":{\"type\":\"fixed\",\"value\":\"15.4.x-scala2.12\"}}",
  "description": "string",
  "max_clusters_per_user": 5,
  "_exist": true
}
```

## Properties

| Name                                 | Type    | Required | Description                                                                               |
|--------------------------------------|---------|----------|-------------------------------------------------------------------------------------------|
| `name`                               | string  | No       | Policy name. Must be unique, between 1 and 100 characters. Identifies the instance.       |
| `definition`                         | string  | No       | Policy definition in the Databricks Cluster Policy Definition Language, as a JSON string. |
| `description`                        | string  | No       | Human-readable description of the policy.                                                 |
| `policy_family_id`                   | string  | No       | ID of a policy family to inherit from. Cannot be combined with `definition`.              |
| `policy_family_definition_overrides` | string  | No       | JSON document customizing the inherited policy family definition.                         |
| `max_clusters_per_user`              | integer | No       | Maximum active clusters per user under this policy. Omit for no limit.                    |
| `policy_id`                          | string  | No       | Canonical identifier of the policy. Computed on create. Read-only.                        |
| `_exist`                             | boolean | No       | Whether the instance should exist. Default: `true`.                                       |

`definition` is a JSON *string*, not a nested object, so it must be escaped
when embedded in JSON input. In a YAML configuration document a block scalar
keeps it readable.

`definition` and `policy_family_id` are mutually exclusive: supply your own
definition, or inherit a family and adjust it with
`policy_family_definition_overrides`.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`.

## Example

Pin the runtime version for every cluster created under the policy:

```yaml
$schema: https://aka.ms/dsc/schemas/v3/bundled/config/document.json
resources:
  - name: pinned runtime
    type: LibreDsc.Databricks/ClusterPolicy
    properties:
      name: pinned-runtime
      description: Restricts clusters to the supported runtime
      max_clusters_per_user: 2
      definition: |
        {
          "spark_version": { "type": "fixed", "value": "15.4.x-scala2.12" }
        }
```

```powershell
dsc config set -f .\policy.dsc.config.yaml
```

## See also

- [Cluster][01]
- [Command line][02]
- [Exit codes][03]

<!-- Link references -->
[01]: cluster.md
[02]: ../cli.md
[03]: ../exit-codes.md
