package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/settings"
)

func init() {
	dsc.RegisterResourceWithMetadata(
		"LibreDsc.Databricks/WorkspaceConf",
		&WorkspaceConfHandler{},
		workspaceConfMetadata(),
	)
}

// Workspace conf property descriptions.
var workspaceConfPropertyDescriptions = dsc.PropertyDescriptions{
	"key": "Configuration key name. Common keys include: " +
		"enableTokensConfig (enable/disable personal access tokens), " +
		"maxTokenLifetimeDays (maximum PAT lifetime in days), " +
		"enableIpAccessLists (enable IP access lists), " +
		"enableDeprecatedClusterNamedInitScripts, " +
		"enableDeprecatedGlobalInitScripts, " +
		"enableDbfsFileBrowser (DBFS file browser in UI), " +
		"enableWebTerminal (web terminal on clusters), " +
		"enableWorkspaceFilesystem (workspace filesystem).",
	"value": "The configuration value. Boolean keys accept 'true' or 'false'. " +
		"Integer keys like maxTokenLifetimeDays accept numeric strings.",
}

// WorkspaceConfSchemaInput defines the desired-state fields.
type WorkspaceConfSchemaInput struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func workspaceConfMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/WorkspaceConf",
		Description:       "Manage Databricks workspace configuration (advanced settings).",
		SchemaDescription: "Schema for managing Databricks workspace configuration keys.",
		ResourceName:      "workspace_conf",
		Tags:              []string{"databricks", "workspace", "configuration", "advanced"},
		Descriptions:      workspaceConfPropertyDescriptions,
		SchemaType:        reflect.TypeFor[WorkspaceConfSchemaInput](),
		OmitTest:          true,
	})
}

// WorkspaceConfState represents the state of a single workspace conf key.
type WorkspaceConfState struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Exist bool   `json:"_exist"`
}

// WorkspaceConfHandler handles WorkspaceConf resource operations.
type WorkspaceConfHandler struct{}

// knownKeys lists the well-known workspace configuration keys used by Export.
// The WorkspaceConf API does not expose a list endpoint, so we maintain a
// static set of commonly documented keys.
var knownKeys = []string{
	"enableTokensConfig",
	"maxTokenLifetimeDays",
	"enableIpAccessLists",
	"enableDeprecatedClusterNamedInitScripts",
	"enableDeprecatedGlobalInitScripts",
	"enableDbfsFileBrowser",
	"enableWebTerminal",
	"enableWorkspaceFilesystem",
}

func (h *WorkspaceConfHandler) getCurrentState(ctx dsc.ResourceContext, key string) (WorkspaceConfState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return WorkspaceConfState{Key: key, Exist: false}, err
	}

	result, err := w.WorkspaceConf.GetStatus(cmdCtx, settings.GetStatusRequest{
		Keys: key,
	})
	if err != nil {
		return WorkspaceConfState{Key: key, Exist: false}, err
	}

	if result == nil {
		return WorkspaceConfState{Key: key, Exist: false}, nil
	}

	value, ok := (*result)[key]
	if !ok {
		return WorkspaceConfState{Key: key, Exist: false}, nil
	}

	return WorkspaceConfState{
		Key:   key,
		Value: value,
		Exist: true,
	}, nil
}

func (h *WorkspaceConfHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[WorkspaceConfSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "key", Value: req.Key}); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *WorkspaceConfHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[WorkspaceConfSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "key", Value: schemaInput.Key},
		dsc.RequiredField{Name: "value", Value: schemaInput.Value},
	); err != nil {
		return nil, err
	}

	// Capture before state.
	beforeState, err := h.getCurrentState(ctx, schemaInput.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to read current configuration: %w", err)
	}

	// Apply the new value.
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if err := w.WorkspaceConf.SetStatus(cmdCtx, settings.WorkspaceConf{
		schemaInput.Key: schemaInput.Value,
	}); err != nil {
		return nil, fmt.Errorf("failed to set configuration %q: %w", schemaInput.Key, err)
	}

	// Capture after state.
	afterState, err := h.getCurrentState(ctx, schemaInput.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to read updated configuration: %w", err)
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *WorkspaceConfHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[WorkspaceConfSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "key", Value: schemaInput.Key}); err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, schemaInput.Key)
	if err != nil {
		return nil, err
	}

	desiredState := WorkspaceConfState{
		Key:   schemaInput.Key,
		Value: schemaInput.Value,
		Exist: true,
	}

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *WorkspaceConfHandler) Delete(_ dsc.ResourceContext, _ json.RawMessage) error {
	// Workspace configuration keys cannot be deleted — they always exist on a
	// workspace.  A Delete call is a no-op.  The user should set the value to
	// the desired default instead.
	return nil
}

func (h *WorkspaceConfHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	// Build a comma-separated list of known keys for a single API call.
	var keysList string
	for i, key := range knownKeys {
		if i > 0 {
			keysList += ","
		}
		keysList += key
	}

	result, err := w.WorkspaceConf.GetStatus(cmdCtx, settings.GetStatusRequest{
		Keys: keysList,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to export workspace configuration: %w", err)
	}

	if result == nil {
		return nil, nil
	}

	var all []any
	for key, value := range *result {
		all = append(all, WorkspaceConfState{
			Key:   key,
			Value: value,
			Exist: true,
		})
	}
	return all, nil
}
