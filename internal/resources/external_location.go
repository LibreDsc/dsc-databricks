package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ExternalLocationState represents the full state of a Unity Catalog external
// location. Renames are not modeled (rename = delete + create). File event
// and encryption settings are out of scope for this resource version.
type ExternalLocationState struct {
	dsc.ExistProperty
	Name           string `json:"name" description:"Name of the external location."`
	URL            string `json:"url,omitempty" description:"Storage URL of the external location (e.g. abfss://container@account.dfs.core.windows.net/path). Required when creating."`
	CredentialName string `json:"credential_name,omitempty" description:"Name of the storage credential used with this location. Required when creating."`
	Comment        string `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner          string `json:"owner,omitempty" description:"Username of the current owner of the external location."`
	IsolationMode  string `json:"isolation_mode,omitempty" description:"Whether the location is accessible from all workspaces or a specific set. Valid values: ISOLATION_MODE_ISOLATED, ISOLATION_MODE_OPEN." enum:"ISOLATION_MODE_ISOLATED,ISOLATION_MODE_OPEN"`
	CredentialID   string `json:"credential_id,omitempty" description:"Unique identifier of the storage credential used with this location. (read-only)"`
	MetastoreID    string `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
	ReadOnly       bool   `json:"read_only" dsc:"optional" description:"Whether the external location is read-only."`
	Fallback       bool   `json:"fallback" dsc:"optional" description:"Whether to enable fallback mode, serving requests through cluster/warehouse credentials when access fails."`
	SkipValidation bool   `json:"skip_validation,omitempty" description:"Skip validation of the location's storage credential when creating or updating. Write-only behavior toggle: specifying it causes reported drift in test."`
}

func externalLocationConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/ExternalLocation",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog external locations in a Databricks workspace.",
		Tags:        []string{"databricks", "externallocation", "unitycatalog", "storage"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog external locations.",
			AllowAdditionalProperties: true,
		},
	}
}

// ExternalLocationHandler handles ExternalLocation resource operations.
type ExternalLocationHandler struct{}

func externalLocationInfoToState(e *catalog.ExternalLocationInfo) ExternalLocationState {
	state := ExternalLocationState{
		Name:           e.Name,
		URL:            e.Url,
		CredentialName: e.CredentialName,
		Comment:        e.Comment,
		Owner:          e.Owner,
		IsolationMode:  string(e.IsolationMode),
		CredentialID:   e.CredentialId,
		MetastoreID:    e.MetastoreId,
		ReadOnly:       e.ReadOnly,
		Fallback:       e.Fallback,
	}
	state.SetExist(true)
	return state
}

func (h *ExternalLocationHandler) Get(ctx context.Context, in ExternalLocationState) (ExternalLocationState, error) {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "ExternalLocation", "name="+in.Name)
	e, err := w.ExternalLocations.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "ExternalLocation", "name="+in.Name)
		return dsc.NotFound(ExternalLocationState{Name: in.Name}, "ExternalLocation", "name="+in.Name)
	}

	return externalLocationInfoToState(e), nil
}

func (h *ExternalLocationHandler) Set(ctx context.Context, desired ExternalLocationState) (ExternalLocationState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgUpdate, "ExternalLocation", "name="+desired.Name)
		updated, err := w.ExternalLocations.Update(ctx, catalog.UpdateExternalLocation{
			Name:            desired.Name,
			Url:             desired.URL,
			CredentialName:  desired.CredentialName,
			Comment:         desired.Comment,
			Owner:           desired.Owner,
			IsolationMode:   catalog.IsolationMode(desired.IsolationMode),
			ReadOnly:        desired.ReadOnly,
			Fallback:        desired.Fallback,
			SkipValidation:  desired.SkipValidation,
			ForceSendFields: []string{"ReadOnly", "Fallback"},
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update external location: %w", err)
		}
		return externalLocationInfoToState(updated), nil
	}

	if err := requireFields(
		field{"url", desired.URL},
		field{"credential_name", desired.CredentialName},
	); err != nil {
		return desired, err
	}

	logInfof(MsgCreate, "ExternalLocation", "name="+desired.Name)
	if _, err := w.ExternalLocations.Create(ctx, catalog.CreateExternalLocation{
		Name:            desired.Name,
		Url:             desired.URL,
		CredentialName:  desired.CredentialName,
		Comment:         desired.Comment,
		ReadOnly:        desired.ReadOnly,
		Fallback:        desired.Fallback,
		SkipValidation:  desired.SkipValidation,
		ForceSendFields: []string{"ReadOnly", "Fallback"},
	}); err != nil {
		return desired, fmt.Errorf("failed to create external location: %w", err)
	}

	// Owner and isolation mode are not part of the create API; apply them
	// with a follow-up update when specified.
	if desired.Owner != "" || desired.IsolationMode != "" {
		if _, err := w.ExternalLocations.Update(ctx, catalog.UpdateExternalLocation{
			Name:          desired.Name,
			Owner:         desired.Owner,
			IsolationMode: catalog.IsolationMode(desired.IsolationMode),
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create external location settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// projectExternalLocationCreate returns the state Set's create path would
// produce. credential_id and metastore_id are computed by the server and stay
// empty. Owner and isolation_mode are applied by the chained post-create
// update; skip_validation is a write-only toggle and never appears in Get
// output, so it is omitted.
func projectExternalLocationCreate(desired *ExternalLocationState) ExternalLocationState {
	projected := ExternalLocationState{
		Name:           desired.Name,
		URL:            desired.URL,
		CredentialName: desired.CredentialName,
		Comment:        desired.Comment,
		Owner:          desired.Owner,
		IsolationMode:  desired.IsolationMode,
		ReadOnly:       desired.ReadOnly,
		Fallback:       desired.Fallback,
	}
	projected.SetExist(true)
	return projected
}

// projectExternalLocationUpdate mirrors catalog.UpdateExternalLocation:
// read_only and fallback are force-sent (desired always wins); other fields
// follow omit-empty semantics.
func projectExternalLocationUpdate(desired, current *ExternalLocationState) ExternalLocationState {
	projected := *current
	projected.ReadOnly = desired.ReadOnly
	projected.Fallback = desired.Fallback
	if desired.URL != "" {
		projected.URL = desired.URL
	}
	if desired.CredentialName != "" {
		projected.CredentialName = desired.CredentialName
		// The linked credential changes; its id is unknown until re-read.
		if current.CredentialName != desired.CredentialName {
			projected.CredentialID = ""
		}
	}
	if desired.Comment != "" {
		projected.Comment = desired.Comment
	}
	if desired.Owner != "" {
		projected.Owner = desired.Owner
	}
	if desired.IsolationMode != "" {
		projected.IsolationMode = desired.IsolationMode
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the
// external location.
func (h *ExternalLocationHandler) SetWhatIf(ctx context.Context, desired ExternalLocationState) (ExternalLocationState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "ExternalLocation", "name="+desired.Name)
		return projectExternalLocationUpdate(&desired, &current), nil
	}

	if err := requireFields(
		field{"url", desired.URL},
		field{"credential_name", desired.CredentialName},
	); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "ExternalLocation", "name="+desired.Name)
	return projectExternalLocationCreate(&desired), nil
}

func (h *ExternalLocationHandler) Delete(ctx context.Context, in ExternalLocationState) error {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return err
	}

	current, err := h.Get(ctx, in)
	if err != nil {
		return err
	}
	if !current.ShouldExist() {
		return nil
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	logDebugf(MsgDelete, "ExternalLocation", "name="+in.Name)
	return w.ExternalLocations.Delete(ctx, catalog.DeleteExternalLocationRequest{
		Name:  in.Name,
		Force: true,
	})
}

func (h *ExternalLocationHandler) Export(ctx context.Context, _ ExternalLocationState) ([]ExternalLocationState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "ExternalLocation")
	locations, err := w.ExternalLocations.ListAll(ctx, catalog.ListExternalLocationsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list external locations: %w", err)
	}

	var all []ExternalLocationState
	for i := range locations {
		all = append(all, externalLocationInfoToState(&locations[i]))
	}

	return all, nil
}
