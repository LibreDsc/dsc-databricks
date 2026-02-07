package dsc

import "fmt"

// ExitCode represents a DSC CLI exit code.
type ExitCode int

const (
	// ExitSuccess indicates the command executed successfully.
	ExitSuccess ExitCode = 0

	// ExitError indicates a general error occurred.
	ExitError ExitCode = 1

	// ExitResourceError indicates a resource raised an error.
	ExitResourceError ExitCode = 2

	// ExitJSONError indicates a JSON serialization/deserialization error.
	ExitJSONError ExitCode = 3

	// ExitInvalidInput indicates the input was not valid JSON.
	ExitInvalidInput ExitCode = 4

	// ExitSchemaValidation indicates a schema validation error.
	ExitSchemaValidation ExitCode = 5

	// ExitNotFound indicates the requested resource was not found.
	ExitNotFound ExitCode = 6
)

// ExitCodeMapping maps a description and optional error type to an exit code.
type ExitCodeMapping struct {
	Description string
	Code        ExitCode
}

// DefaultExitCodeMappings returns the standard exit code mappings for DSC resources.
func DefaultExitCodeMappings() []ExitCodeMapping {
	return []ExitCodeMapping{
		{Code: ExitSuccess, Description: "Success"},
		{Code: ExitError, Description: "Error"},
		{Code: ExitResourceError, Description: "Resource error"},
		{Code: ExitJSONError, Description: "JSON serialization error"},
		{Code: ExitInvalidInput, Description: "Invalid input"},
		{Code: ExitSchemaValidation, Description: "Schema validation error"},
		{Code: ExitNotFound, Description: "Resource not found"},
	}
}

// ExitCodeError wraps an error with a specific exit code for CLI return.
type ExitCodeError struct {
	Err  error
	Code ExitCode
}

func (e *ExitCodeError) Error() string {
	return e.Err.Error()
}

func (e *ExitCodeError) Unwrap() error {
	return e.Err
}

// NewExitCodeError creates an error that carries a specific exit code.
func NewExitCodeError(code ExitCode, err error) *ExitCodeError {
	return &ExitCodeError{Code: code, Err: err}
}

// NewExitCodeErrorf creates a formatted error that carries a specific exit code.
func NewExitCodeErrorf(code ExitCode, format string, args ...any) *ExitCodeError {
	return &ExitCodeError{Code: code, Err: fmt.Errorf(format, args...)}
}
