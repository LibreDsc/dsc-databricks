package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// CatalogState represents the full state of a Unity Catalog catalog.
type CatalogState struct {
	dsc.ExistProperty
	Properties                   map[string]string `json:"properties,omitempty" description:"A map of key-value properties attached to the catalog."`
	Options                      map[string]string `json:"options,omitempty" description:"A map of key-value options attached to the catalog."`
	Name                         string            `json:"name" description:"Name of the catalog. This is the unique identifier used for lookup."`
	Comment                      string            `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner                        string            `json:"owner,omitempty" description:"Username of the current owner of the catalog."`
	IsolationMode                string            `json:"isolation_mode,omitempty" description:"Whether the catalog is accessible from all workspaces or a specific set. Valid values: ISOLATED, OPEN." enum:"ISOLATED,OPEN"`
	StorageRoot                  string            `json:"storage_root,omitempty" description:"Storage root URL for managed tables within the catalog."`
	StorageLocation              string            `json:"storage_location,omitempty" description:"Storage location URL (full path) for managed tables within the catalog. (read-only)"`
	ConnectionName               string            `json:"connection_name,omitempty" description:"The name of the connection to an external data source."`
	ProviderName                 string            `json:"provider_name,omitempty" description:"The name of the delta sharing provider."`
	ShareName                    string            `json:"share_name,omitempty" description:"The name of the share under the share provider."`
	EnablePredictiveOptimization string            `json:"enable_predictive_optimization,omitempty" description:"Whether predictive optimization should be enabled. Valid values: DISABLE, ENABLE, INHERIT." enum:"DISABLE,ENABLE,INHERIT"`
	CatalogType                  string            `json:"catalog_type,omitempty" description:"Type of the catalog. (read-only)"`
	MetastoreID                  string            `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
}

func catalogConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Catalog",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog catalogs in a Databricks workspace.",
		Tags:        []string{"databricks", "catalog", "unitycatalog"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog catalogs.",
			AllowAdditionalProperties: true,
		},
	}
}

// CatalogHandler handles Catalog resource operations.
type CatalogHandler struct{}

func catalogInfoToState(c *catalog.CatalogInfo) CatalogState {
	state := CatalogState{
		Name:                         c.Name,
		Comment:                      c.Comment,
		Owner:                        c.Owner,
		IsolationMode:                string(c.IsolationMode),
		StorageRoot:                  c.StorageRoot,
		StorageLocation:              c.StorageLocation,
		ConnectionName:               c.ConnectionName,
		ProviderName:                 c.ProviderName,
		ShareName:                    c.ShareName,
		EnablePredictiveOptimization: string(c.EnablePredictiveOptimization),
		CatalogType:                  string(c.CatalogType),
		MetastoreID:                  c.MetastoreId,
		Properties:                   c.Properties,
		Options:                      c.Options,
	}
	state.SetExist(true)
	return state
}

func (h *CatalogHandler) Get(ctx context.Context, in CatalogState) (CatalogState, error) {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "Catalog", "name="+in.Name)
	c, err := w.Catalogs.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "Catalog", "name="+in.Name)
		return dsc.NotFound(CatalogState{Name: in.Name}, "Catalog", "name="+in.Name)
	}

	return catalogInfoToState(c), nil
}

func (h *CatalogHandler) Set(ctx context.Context, desired CatalogState) (CatalogState, error) {
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
		logInfof(MsgUpdate, "Catalog", "name="+desired.Name)
		updated, err := w.Catalogs.Update(ctx, catalog.UpdateCatalog{
			Name:                         desired.Name,
			Comment:                      desired.Comment,
			Owner:                        desired.Owner,
			IsolationMode:                catalog.CatalogIsolationMode(desired.IsolationMode),
			EnablePredictiveOptimization: catalog.EnablePredictiveOptimization(desired.EnablePredictiveOptimization),
			Properties:                   desired.Properties,
			Options:                      desired.Options,
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update catalog: %w", err)
		}
		return catalogInfoToState(updated), nil
	}

	logInfof(MsgCreate, "Catalog", "name="+desired.Name)
	created, err := w.Catalogs.Create(ctx, catalog.CreateCatalog{
		Name:           desired.Name,
		Comment:        desired.Comment,
		StorageRoot:    desired.StorageRoot,
		ConnectionName: desired.ConnectionName,
		ProviderName:   desired.ProviderName,
		ShareName:      desired.ShareName,
		Properties:     desired.Properties,
		Options:        desired.Options,
	})
	if err != nil {
		return desired, fmt.Errorf("failed to create catalog: %w", err)
	}
	return catalogInfoToState(created), nil
}

// projectCatalogCreate returns the state Set's create path would produce.
// Owner, catalog_type, metastore_id, and storage_location are computed by the
// server and stay empty.
func projectCatalogCreate(desired *CatalogState) CatalogState {
	projected := CatalogState{
		Name:           desired.Name,
		Comment:        desired.Comment,
		StorageRoot:    desired.StorageRoot,
		ConnectionName: desired.ConnectionName,
		ProviderName:   desired.ProviderName,
		ShareName:      desired.ShareName,
		Properties:     desired.Properties,
		Options:        desired.Options,
	}
	projected.SetExist(true)
	return projected
}

// projectCatalogUpdate mirrors catalog.UpdateCatalog: the SDK omits empty
// values, so non-empty desired fields win; immutable and computed fields
// carry over from the current state.
func projectCatalogUpdate(desired, current *CatalogState) CatalogState {
	projected := *current
	if desired.Comment != "" {
		projected.Comment = desired.Comment
	}
	if desired.Owner != "" {
		projected.Owner = desired.Owner
	}
	if desired.IsolationMode != "" {
		projected.IsolationMode = desired.IsolationMode
	}
	if desired.EnablePredictiveOptimization != "" {
		projected.EnablePredictiveOptimization = desired.EnablePredictiveOptimization
	}
	if len(desired.Properties) > 0 {
		projected.Properties = desired.Properties
	}
	if len(desired.Options) > 0 {
		projected.Options = desired.Options
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the catalog.
func (h *CatalogHandler) SetWhatIf(ctx context.Context, desired CatalogState) (CatalogState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Catalog", "name="+desired.Name)
		return projectCatalogUpdate(&desired, &current), nil
	}

	logInfof(MsgWhatIfCreate, "Catalog", "name="+desired.Name)
	return projectCatalogCreate(&desired), nil
}

func (h *CatalogHandler) Test(ctx context.Context, desired CatalogState) (dsc.TestResult[CatalogState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[CatalogState]{}, err
	}

	result := dsc.TestResult[CatalogState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *CatalogHandler) Delete(ctx context.Context, in CatalogState) error {
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

	logDebugf(MsgDelete, "Catalog", "name="+in.Name)
	return w.Catalogs.Delete(ctx, catalog.DeleteCatalogRequest{
		Name:  in.Name,
		Force: true,
	})
}

func (h *CatalogHandler) Export(ctx context.Context, _ CatalogState) ([]CatalogState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Catalog")
	catalogs, err := w.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list catalogs: %w", err)
	}

	var all []CatalogState
	for i := range catalogs {
		all = append(all, catalogInfoToState(&catalogs[i]))
	}

	return all, nil
}
