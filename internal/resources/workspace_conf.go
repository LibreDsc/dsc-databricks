package resources

import (
	"context"
	"fmt"
	"strings"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/settings"
)

// WorkspaceConfState represents the state of a single workspace conf key.
type WorkspaceConfState struct {
	dsc.ExistProperty
	Key   string `json:"key" description:"Configuration key name. Common keys include: enableTokensConfig (enable/disable personal access tokens), maxTokenLifetimeDays (maximum PAT lifetime in days), enableIpAccessLists (enable IP access lists), enableDeprecatedClusterNamedInitScripts, enableDeprecatedGlobalInitScripts, enableDbfsFileBrowser (DBFS file browser in UI), enableWebTerminal (web terminal on clusters), enableWorkspaceFilesystem (workspace filesystem)."`
	Value string `json:"value,omitempty" description:"The configuration value. Boolean keys accept 'true' or 'false'. Integer keys like maxTokenLifetimeDays accept numeric strings."`
}

func workspaceConfConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/WorkspaceConf",
		Version:     "0.1.0",
		Description: "Manage Databricks workspace configuration (advanced settings).",
		Tags:        []string{"databricks", "workspace", "configuration", "advanced"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks workspace configuration keys.",
			AllowAdditionalProperties: true,
		},
	}
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

func (h *WorkspaceConfHandler) Get(ctx context.Context, in WorkspaceConfState) (WorkspaceConfState, error) {
	if err := requireFields(field{"key", in.Key}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	dsc.Logger.Debugf(MsgLookup, "WorkspaceConf", "key="+in.Key)
	result, err := w.WorkspaceConf.GetStatus(ctx, settings.GetStatusRequest{Keys: in.Key})
	if err != nil {
		return in, err
	}

	if result != nil {
		if value, ok := (*result)[in.Key]; ok {
			state := WorkspaceConfState{Key: in.Key, Value: value}
			state.SetExist(true)
			return state, nil
		}
	}

	dsc.Logger.Infof(MsgNotFound, "WorkspaceConf", "key="+in.Key)
	return dsc.NotFound(WorkspaceConfState{Key: in.Key}, "WorkspaceConf", "key="+in.Key)
}

func (h *WorkspaceConfHandler) Set(ctx context.Context, desired WorkspaceConfState) (WorkspaceConfState, error) {
	if err := requireFields(field{"key", desired.Key}, field{"value", desired.Value}); err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	dsc.Logger.Infof(MsgPut, "WorkspaceConf", "key="+desired.Key)
	if err := w.WorkspaceConf.SetStatus(ctx, settings.WorkspaceConf{
		desired.Key: desired.Value,
	}); err != nil {
		return desired, fmt.Errorf("failed to set configuration %q: %w", desired.Key, err)
	}

	return h.Get(ctx, desired)
}

func (h *WorkspaceConfHandler) Delete(_ context.Context, _ WorkspaceConfState) error {
	// Workspace configuration keys cannot be deleted — they always exist on a
	// workspace.  A Delete call is a no-op.  The user should set the value to
	// the desired default instead.
	return nil
}

func (h *WorkspaceConfHandler) Export(ctx context.Context, _ WorkspaceConfState) ([]WorkspaceConfState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	// Build a comma-separated list of known keys for a single API call.
	dsc.Logger.Debugf(MsgListAll, "WorkspaceConf")
	result, err := w.WorkspaceConf.GetStatus(ctx, settings.GetStatusRequest{
		Keys: strings.Join(knownKeys, ","),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to export workspace configuration: %w", err)
	}

	if result == nil {
		return nil, nil
	}

	var all []WorkspaceConfState
	for key, value := range *result {
		state := WorkspaceConfState{Key: key, Value: value}
		state.SetExist(true)
		all = append(all, state)
	}
	return all, nil
}
