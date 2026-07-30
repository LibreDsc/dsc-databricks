# Exit codes

The `dsc-databricks` binary reports the outcome of every operation through
its exit code. The DSC engine surfaces non-zero exit codes as resource
errors.

## Codes

| Code | Name              | Meaning                                                                        |
|------|-------------------|--------------------------------------------------------------------------------|
| 0    | Success           | The operation completed successfully.                                          |
| 1    | Error             | A general error occurred.                                                      |
| 2    | Resource error    | The resource raised an error, for example a failed Databricks API call.        |
| 3    | JSON error        | Input or output could not be serialized as JSON.                               |
| 4    | Invalid input     | Required fields were missing or invalid. The message lists the missing fields. |
| 5    | Schema validation | The input failed schema validation.                                            |
| 6    | Not found         | The resource was not found where the operation required it to exist.           |

A `get` for a missing instance is not an error: it returns exit code 0 with
`_exist: false` in the state.

## See also

- [Command line][01]
- [Resources][02]

<!-- Link references -->
[01]: cli.md
[02]: index.md
