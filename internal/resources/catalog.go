package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Catalog", &CatalogHandler{}, catalogMetadata())
}

// Catalog property descriptions from SDK documentation.
var catalogPropertyDescriptions = dsc.PropertyDescriptions{
	"name":                            "Name of the catalog. This is the unique identifier used for lookup.",
	"comment":                         "User-provided free-form text description.",
	"owner":                           "Username of the current owner of the catalog.",
	"isolation_mode":                  "Whether the catalog is accessible from all workspaces or a specific set. Valid values: ISOLATED, OPEN.",
	"storage_root":                    "Storage root URL for managed tables within the catalog.",
	"storage_location":                "Storage location URL (full path) for managed tables within the catalog. Computed by the server.",
	"connection_name":                 "The name of the connection to an external data source.",
	"provider_name":                   "The name of the delta sharing provider.",
	"share_name":                      "The name of the share under the share provider.",
	"enable_predictive_optimization":  "Whether predictive optimization should be enabled. Valid values: DISABLE, ENABLE, INHERIT.",
	"catalog_type":                    "Type of the catalog. Computed by the server.",
	"metastore_id":                    "Unique identifier of the parent metastore. Computed by the server.",
	"properties":                      "A map of key-value properties attached to the catalog.",
	"options":                         "A map of key-value options attached to the catalog.",
}

// CatalogSchemaInput defines the desired-state fields for the schema
// and for unmarshaling input. name is the primary identifier.
type CatalogSchemaInput struct {
	Name                        string            `json:"name"`
	Comment                     string            `json:"comment,omitempty"`
	Owner                       string            `json:"owner,omitempty"`
	IsolationMode               string            `json:"isolation_mode,omitempty"`
	StorageRoot                 string            `json:"storage_root,omitempty"`
	ConnectionName              string            `json:"connection_name,omitempty"`
	ProviderName                string            `json:"provider_name,omitempty"`
	ShareName                   string            `json:"share_name,omitempty"`
	EnablePredictiveOptimization string           `json:"enable_predictive_optimization,omitempty"`
	Properties                  map[string]string `json:"properties,omitempty"`
	Options                     map[string]string `json:"options,omitempty"`
}

func catalogMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/Catalog",
		Description:       "Manage Unity Catalog catalogs in a Databricks workspace.",
		SchemaDescription: "Schema for managing Unity Catalog catalogs.",
		ResourceName:      "catalog",
		Tags:              []string{"databricks", "catalog", "unitycatalog"},
		Descriptions:      catalogPropertyDescriptions,
		SchemaType:        reflect.TypeFor[CatalogSchemaInput](),
	})
}

// CatalogState represents the full state of a Unity Catalog catalog.
type CatalogState struct {
	Name                        string            `json:"name"`
	Comment                     string            `json:"comment,omitempty"`
	Owner                       string            `json:"owner,omitempty"`
	IsolationMode               string            `json:"isolation_mode,omitempty"`
	StorageRoot                 string            `json:"storage_root,omitempty"`
	StorageLocation             string            `json:"storage_location,omitempty"`
	ConnectionName              string            `json:"connection_name,omitempty"`
	ProviderName                string            `json:"provider_name,omitempty"`
	ShareName                   string            `json:"share_name,omitempty"`
	EnablePredictiveOptimization string           `json:"enable_predictive_optimization,omitempty"`
	CatalogType                 string            `json:"catalog_type,omitempty"`
	MetastoreID                 string            `json:"metastore_id,omitempty"`
	Properties                  map[string]string `json:"properties,omitempty"`
	Options                     map[string]string `json:"options,omitempty"`
	Exist                       bool              `json:"_exist"`
}

// CatalogHandler handles Catalog resource operations.
type CatalogHandler struct{}

func catalogInfoToState(c *catalog.CatalogInfo) CatalogState {
	return CatalogState{
		Name:                        c.Name,
		Comment:                     c.Comment,
		Owner:                       c.Owner,
		IsolationMode:               string(c.IsolationMode),
		StorageRoot:                 c.StorageRoot,
		StorageLocation:             c.StorageLocation,
		ConnectionName:              c.ConnectionName,
		ProviderName:                c.ProviderName,
		ShareName:                   c.ShareName,
		EnablePredictiveOptimization: string(c.EnablePredictiveOptimization),
		CatalogType:                 string(c.CatalogType),
		MetastoreID:                 c.MetastoreId,
		Properties:                  c.Properties,
		Options:                     c.Options,
		Exist:                       true,
	}
}

func (h *CatalogHandler) getCurrentState(ctx dsc.ResourceContext, name string) (CatalogState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return CatalogState{Exist: false}, err
	}

	c, err := w.Catalogs.GetByName(cmdCtx, name)
	if err != nil {
		return CatalogState{Name: name, Exist: false}, nil
	}

	return catalogInfoToState(c), nil
}

func (h *CatalogHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[CatalogSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "name", Value: req.Name}); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *CatalogHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[CatalogSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "name", Value: req.Name}); err != nil {
		return nil, err
	}

	beforeState, _ := h.getCurrentState(ctx, req.Name)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var afterCatalog *catalog.CatalogInfo

	if beforeState.Exist {
		// Catalog exists — update it.
		updated, err := w.Catalogs.Update(cmdCtx, catalog.UpdateCatalog{
			Name:                        req.Name,
			Comment:                     req.Comment,
			Owner:                       req.Owner,
			IsolationMode:               catalog.CatalogIsolationMode(req.IsolationMode),
			EnablePredictiveOptimization: catalog.EnablePredictiveOptimization(req.EnablePredictiveOptimization),
			Properties:                  req.Properties,
			Options:                     req.Options,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update catalog: %w", err)
		}
		afterCatalog = updated
	} else {
		// Catalog does not exist — create it.
		created, err := w.Catalogs.Create(cmdCtx, catalog.CreateCatalog{
			Name:           req.Name,
			Comment:        req.Comment,
			StorageRoot:    req.StorageRoot,
			ConnectionName: req.ConnectionName,
			ProviderName:   req.ProviderName,
			ShareName:      req.ShareName,
			Properties:     req.Properties,
			Options:        req.Options,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create catalog: %w", err)
		}
		afterCatalog = created
	}

	afterState := catalogInfoToState(afterCatalog)
	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *CatalogHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[CatalogSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	desiredState := CatalogState{
		Name:                        req.Name,
		Comment:                     req.Comment,
		Owner:                       req.Owner,
		IsolationMode:               req.IsolationMode,
		StorageRoot:                 req.StorageRoot,
		ConnectionName:              req.ConnectionName,
		ProviderName:                req.ProviderName,
		ShareName:                   req.ShareName,
		EnablePredictiveOptimization: req.EnablePredictiveOptimization,
		Properties:                  req.Properties,
		Options:                     req.Options,
		Exist:                       true,
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

func (h *CatalogHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[CatalogSchemaInput](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "name", Value: req.Name}); err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	return w.Catalogs.Delete(cmdCtx, catalog.DeleteCatalogRequest{
		Name:  req.Name,
		Force: true,
	})
}

func (h *CatalogHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	catalogs, err := w.Catalogs.ListAll(cmdCtx, catalog.ListCatalogsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list catalogs: %w", err)
	}

	var all []any
	for i := range catalogs {
		all = append(all, catalogInfoToState(&catalogs[i]))
	}

	return all, nil
}
