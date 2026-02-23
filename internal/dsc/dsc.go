package dsc

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// NewRootCommand creates the root command for dsc-databricks CLI.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "dsc-databricks",
		Short:         "Databricks DSC Resource Provider",
		Long:          `Databricks DSC Resource Provider - Manages Databricks resources using Microsoft DSC v3 semantics.`,
		SilenceErrors: true,
	}

	cmd.SetHelpTemplate(`{{.Long}}

Usage:
  dsc-databricks [command]

Available commands:
{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Use "{{.CommandPath}} [command] --help" for more information about a command.
`)

	cmd.AddCommand(newGetCmd())
	cmd.AddCommand(newSetCmd())
	cmd.AddCommand(newTestCmd())
	cmd.AddCommand(newDeleteCmd())
	cmd.AddCommand(newExportCmd())
	cmd.AddCommand(newSchemaCmd())
	cmd.AddCommand(newManifestCmd())

	return cmd
}

// ResourceHandler defines the interface for resource operations.
// All DSC resources must implement Get and Set at minimum.
// Test, Delete, and Export are optional capabilities.
type ResourceHandler interface {
	// Get retrieves the current state of the resource.
	// Returns the actual state or an error.
	Get(ctx ResourceContext, input json.RawMessage) (*GetResult, error)

	// Set applies the desired state to the resource.
	// Returns a SetResult with before/after state and changed properties, or nil if no changes.
	Set(ctx ResourceContext, input json.RawMessage) (*SetResult, error)

	// Test checks whether the resource is in the desired state.
	// Returns a TestResult with the actual state and differing properties.
	Test(ctx ResourceContext, input json.RawMessage) (*TestResult, error)

	// Delete removes the resource.
	Delete(ctx ResourceContext, input json.RawMessage) error

	// Export enumerates all instances of the resource.
	Export(ctx ResourceContext) ([]any, error)
}

// ResourceMetadata contains metadata for resources.
type ResourceMetadata struct {
	ExitCodes   map[string]string `json:"exitCodes"`
	Schema      ResourceSchema    `json:"schema"`
	Type        string            `json:"type"`
	Version     string            `json:"version"`
	Description string            `json:"description"`
	Tags        []string          `json:"tags"`
	OmitTest    bool              `json:"-"`
}

// ResourceSchema represents the embedded schema.
type ResourceSchema struct {
	Embedded any `json:"embedded"`
}

// ResourceManifest represents the resource manifest structure.
type ResourceManifest struct {
	ExitCodes   map[string]string `json:"exitCodes"`
	Schema_     ResourceSchema    `json:"schema"`
	Get         *CommandSpec      `json:"get,omitempty"`
	Set         *SetCommandSpec   `json:"set,omitempty"`
	Test        *TestCommandSpec  `json:"test,omitempty"`
	Delete      *CommandSpec      `json:"delete,omitempty"`
	Export      *ExportSpec       `json:"export,omitempty"`
	Schema      string            `json:"$schema"`
	Type        string            `json:"type"`
	Version     string            `json:"version"`
	Description string            `json:"description"`
	Tags        []string          `json:"tags"`
}

// CommandSpec defines a command specification in the manifest.
type CommandSpec struct {
	Executable string `json:"executable"`
	Args       []any  `json:"args"`
}

// SetCommandSpec defines the Set command specification with return type.
type SetCommandSpec struct {
	Executable string `json:"executable"`
	Return     string `json:"return,omitempty"`
	Args       []any  `json:"args"`
}

// TestCommandSpec defines the Test command specification with return type.
type TestCommandSpec struct {
	Executable string `json:"executable"`
	Return     string `json:"return,omitempty"`
	Args       []any  `json:"args"`
}

// ExportSpec defines the Export command specification.
type ExportSpec struct {
	Executable string `json:"executable"`
	Args       []any  `json:"args"`
}

// JSONInputArg represents the JSON input argument specification.
type JSONInputArg struct {
	JSONInputArg string `json:"jsonInputArg"`
	Mandatory    bool   `json:"mandatory"`
}

// ResourceContext provides context for resource operations.
type ResourceContext struct {
	Cmd *cobra.Command
}

var resourceRegistry = make(map[string]ResourceHandler)
var metadataRegistry = make(map[string]ResourceMetadata)

// RegisterResource registers a resource handler.
func RegisterResource(resourceType string, handler ResourceHandler) {
	resourceRegistry[resourceType] = handler
}

// RegisterResourceWithMetadata registers a resource handler with metadata.
func RegisterResourceWithMetadata(resourceType string, handler ResourceHandler, metadata ResourceMetadata) {
	resourceRegistry[resourceType] = handler
	metadataRegistry[resourceType] = metadata
}

func getResourceHandler(resourceType string) (ResourceHandler, error) {
	handler, ok := resourceRegistry[resourceType]
	if !ok {
		return nil, fmt.Errorf("unknown resource type: %s. Available types: %v", resourceType, listResourceTypes())
	}
	return handler, nil
}

func getResourceMetadata(resourceType string) (ResourceMetadata, error) {
	metadata, ok := metadataRegistry[resourceType]
	if !ok {
		return ResourceMetadata{}, fmt.Errorf("no metadata for resource type: %s", resourceType)
	}
	return metadata, nil
}

func listResourceTypes() []string {
	types := make([]string, 0, len(resourceRegistry))
	for t := range resourceRegistry {
		types = append(types, t)
	}
	return types
}

func buildManifest(resourceType string, metadata ResourceMetadata) ResourceManifest {
	inputArg := JSONInputArg{JSONInputArg: "--input", Mandatory: true}

	var testSpec *TestCommandSpec
	if !metadata.OmitTest {
		testSpec = &TestCommandSpec{
			Executable: "dsc-databricks",
			Args:       []any{"test", "--resource", resourceType, inputArg},
			Return:     "state",
		}
	}

	return ResourceManifest{
		Schema:      "https://aka.ms/dsc/schemas/v3/bundled/resource/manifest.json",
		Type:        resourceType,
		Version:     metadata.Version,
		Description: metadata.Description,
		Tags:        metadata.Tags,
		ExitCodes:   metadata.ExitCodes,
		Schema_:     metadata.Schema,
		Get: &CommandSpec{
			Executable: "dsc-databricks",
			Args:       []any{"get", "--resource", resourceType, inputArg},
		},
		Set: &SetCommandSpec{
			Executable: "dsc-databricks",
			Args:       []any{"set", "--resource", resourceType, inputArg},
			Return:     "stateAndDiff",
		},
		Test: testSpec,
		Delete: &CommandSpec{
			Executable: "dsc-databricks",
			Args:       []any{"delete", "--resource", resourceType, inputArg},
		},
		Export: &ExportSpec{
			Executable: "dsc-databricks",
			Args:       []any{"export", "--resource", resourceType},
		},
	}
}

func parseInput(inputFlag string) (json.RawMessage, error) {
	var jsonData []byte
	var err error

	if inputFlag != "" {
		jsonData = []byte(inputFlag)
	} else {
		// Check if stdin has data
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			jsonData, err = io.ReadAll(os.Stdin)
			if err != nil {
				return nil, fmt.Errorf("failed to read from stdin: %w", err)
			}
		}
	}

	if len(jsonData) == 0 {
		return nil, fmt.Errorf("no JSON input provided. Use --input flag or pipe JSON to stdin")
	}

	var raw json.RawMessage
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON input: %w", err)
	}

	return raw, nil
}
