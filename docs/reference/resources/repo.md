# Repo

Manages Databricks Git folders, historically called repos. A Git folder
links a workspace path to a remote Git repository and checks out one
branch.

Type: `LibreDsc.Databricks/Repo`

## Syntax

```json
{
  "path": "/Repos/data-platform/pipelines",
  "url": "https://github.com/example/pipelines",
  "provider": "gitHub",
  "branch": "main",
  "_exist": true
}
```

## Properties

| Name             | Type    | Required | Description                                                                                  |
|------------------|---------|----------|----------------------------------------------------------------------------------------------|
| `path`           | string  | Yes      | Workspace path of the Git folder. Under `/Repos`, the form is `/Repos/{folder}/{repo-name}`. |
| `url`            | string  | No       | URL of the remote repository. Required when creating.                                        |
| `provider`       | string  | No       | Git provider, case-insensitive. Required when creating.                                      |
| `branch`         | string  | No       | Branch to check out. Defaults to the repository's default branch on create.                  |
| `head_commit_id` | string  | No       | SHA-1 of the current HEAD commit. Read-only.                                                 |
| `id`             | integer | No       | Numeric ID of the workspace object. Read-only.                                               |
| `_exist`         | boolean | No       | Whether the instance should exist. Default: `true`.                                          |

Valid `provider` values: `gitHub`, `bitbucketCloud`, `gitLab`,
`azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`,
`gitLabEnterpriseEdition`, `awsCodeCommit`.

Once the folder exists, only `branch` is updated. Changing `url` or
`provider` requires deleting and recreating the folder.

Credentials for the remote repository are Git credentials configured
separately in the workspace. This resource does not manage them.

## Capabilities

`get`, `set`, `delete`, `export`, `setWhatIf`.

No native `test`. The DSC engine synthesizes it from `get`. Lookup and
export tolerate workspace shards that report Git folders as directory
objects rather than repo objects.

## Example

Link a repository and track its `main` branch:

```json
{
  "path": "/Repos/data-platform/pipelines",
  "url": "https://github.com/example/pipelines",
  "provider": "gitHub",
  "branch": "main"
}
```

```powershell
dsc resource set -r LibreDsc.Databricks/Repo --input (Get-Content .\repo.json -Raw)
```

Switch the checked-out branch, supplying only the identifying path:

```powershell
dsc resource set -r LibreDsc.Databricks/Repo --input '{"path":"/Repos/data-platform/pipelines","branch":"release"}'
```

## See also

- [Command line][01]
- [How to export existing resources][02]
- [Exit codes][03]

<!-- Link references -->
[01]: ../cli.md
[02]: ../../how-to/export-resources.md
[03]: ../exit-codes.md
