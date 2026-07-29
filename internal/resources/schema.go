package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// SchemaState represents the full state of a Unity Catalog schema. The schema
// is identified by catalog_name + name.
type SchemaState struct {
	dsc.ExistProperty
	Properties                   map[string]string `json:"properties,omitempty" description:"A map of key-value properties attached to the schema."`
	Name                         string            `json:"name" description:"Name of the schema, relative to its parent catalog."`
	CatalogName                  string            `json:"catalog_name" description:"Name of the parent catalog."`
	FullName                     string            `json:"full_name,omitempty" description:"Full name of the schema in the form catalog_name.schema_name. (read-only)"`
	Comment                      string            `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner                        string            `json:"owner,omitempty" description:"Username of the current owner of the schema."`
	StorageRoot                  string            `json:"storage_root,omitempty" description:"Storage root URL for managed tables within the schema. (create-only)"`
	StorageLocation              string            `json:"storage_location,omitempty" description:"Storage location URL (full path) for managed tables within the schema. (read-only)"`
	EnablePredictiveOptimization string            `json:"enable_predictive_optimization,omitempty" description:"Whether predictive optimization should be enabled. Valid values: DISABLE, ENABLE, INHERIT." enum:"DISABLE,ENABLE,INHERIT"`
	SchemaID                     string            `json:"schema_id,omitempty" description:"Unique identifier of the schema. (read-only)"`
	MetastoreID                  string            `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
}

// schemaFullName returns the two-level identifier catalog.schema.
func schemaFullName(catalogName, name string) string {
	return catalogName + "." + name
}

func schemaConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Schema",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog schemas in a Databricks workspace.",
		Tags:        []string{"databricks", "schema", "unitycatalog"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog schemas.",
			AllowAdditionalProperties: true,
		},
	}
}

// SchemaHandler handles Schema resource operations.
type SchemaHandler struct{}

func schemaInfoToState(s *catalog.SchemaInfo) SchemaState {
	state := SchemaState{
		Name:                         s.Name,
		CatalogName:                  s.CatalogName,
		FullName:                     s.FullName,
		Comment:                      s.Comment,
		Owner:                        s.Owner,
		StorageRoot:                  s.StorageRoot,
		StorageLocation:              s.StorageLocation,
		EnablePredictiveOptimization: string(s.EnablePredictiveOptimization),
		SchemaID:                     s.SchemaId,
		MetastoreID:                  s.MetastoreId,
		Properties:                   s.Properties,
	}
	state.SetExist(true)
	return state
}

func (h *SchemaHandler) Get(ctx context.Context, in SchemaState) (SchemaState, error) {
	if err := requireFields(field{"name", in.Name}, field{"catalog_name", in.CatalogName}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	fullName := schemaFullName(in.CatalogName, in.Name)
	logDebugf(MsgLookup, "Schema", "full_name="+fullName)
	s, err := w.Schemas.GetByFullName(ctx, fullName)
	if err != nil {
		logInfof(MsgNotFound, "Schema", "full_name="+fullName)
		return dsc.NotFound(SchemaState{Name: in.Name, CatalogName: in.CatalogName}, "Schema", "full_name="+fullName)
	}

	return schemaInfoToState(s), nil
}

func (h *SchemaHandler) Set(ctx context.Context, desired SchemaState) (SchemaState, error) {
	if err := requireFields(field{"name", desired.Name}, field{"catalog_name", desired.CatalogName}); err != nil {
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

	fullName := schemaFullName(desired.CatalogName, desired.Name)
	if current.ShouldExist() {
		logInfof(MsgUpdate, "Schema", "full_name="+fullName)
		updated, err := w.Schemas.Update(ctx, catalog.UpdateSchema{
			FullName:                     fullName,
			Comment:                      desired.Comment,
			Owner:                        desired.Owner,
			Properties:                   desired.Properties,
			EnablePredictiveOptimization: catalog.EnablePredictiveOptimization(desired.EnablePredictiveOptimization),
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update schema: %w", err)
		}
		return schemaInfoToState(updated), nil
	}

	logInfof(MsgCreate, "Schema", "full_name="+fullName)
	if _, err := w.Schemas.Create(ctx, catalog.CreateSchema{
		CatalogName: desired.CatalogName,
		Name:        desired.Name,
		Comment:     desired.Comment,
		Properties:  desired.Properties,
		StorageRoot: desired.StorageRoot,
	}); err != nil {
		return desired, fmt.Errorf("failed to create schema: %w", err)
	}

	// Owner and predictive optimization are not part of the create API;
	// apply them with a follow-up update when specified.
	if desired.Owner != "" || desired.EnablePredictiveOptimization != "" {
		if _, err := w.Schemas.Update(ctx, catalog.UpdateSchema{
			FullName:                     fullName,
			Owner:                        desired.Owner,
			EnablePredictiveOptimization: catalog.EnablePredictiveOptimization(desired.EnablePredictiveOptimization),
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create schema settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// projectSchemaCreate returns the state Set's create path would produce.
// schema_id, metastore_id, and storage_location are computed by the server
// and stay empty; full_name is deterministic and included. Owner and
// enable_predictive_optimization are applied by the chained post-create
// update, so desired values carry into the projection.
func projectSchemaCreate(desired *SchemaState) SchemaState {
	projected := SchemaState{
		Name:                         desired.Name,
		CatalogName:                  desired.CatalogName,
		FullName:                     schemaFullName(desired.CatalogName, desired.Name),
		Comment:                      desired.Comment,
		Owner:                        desired.Owner,
		StorageRoot:                  desired.StorageRoot,
		EnablePredictiveOptimization: desired.EnablePredictiveOptimization,
		Properties:                   desired.Properties,
	}
	projected.SetExist(true)
	return projected
}

// projectSchemaUpdate mirrors catalog.UpdateSchema: the SDK omits empty
// values, so non-empty desired fields win; create-only and computed fields
// carry over from the current state.
func projectSchemaUpdate(desired, current *SchemaState) SchemaState {
	projected := *current
	if desired.Comment != "" {
		projected.Comment = desired.Comment
	}
	if desired.Owner != "" {
		projected.Owner = desired.Owner
	}
	if desired.EnablePredictiveOptimization != "" {
		projected.EnablePredictiveOptimization = desired.EnablePredictiveOptimization
	}
	if len(desired.Properties) > 0 {
		projected.Properties = desired.Properties
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the schema.
func (h *SchemaHandler) SetWhatIf(ctx context.Context, desired SchemaState) (SchemaState, error) {
	if err := requireFields(field{"name", desired.Name}, field{"catalog_name", desired.CatalogName}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	fullName := schemaFullName(desired.CatalogName, desired.Name)
	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Schema", "full_name="+fullName)
		return projectSchemaUpdate(&desired, &current), nil
	}

	logInfof(MsgWhatIfCreate, "Schema", "full_name="+fullName)
	return projectSchemaCreate(&desired), nil
}

func (h *SchemaHandler) Delete(ctx context.Context, in SchemaState) error {
	if err := requireFields(field{"name", in.Name}, field{"catalog_name", in.CatalogName}); err != nil {
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

	fullName := schemaFullName(in.CatalogName, in.Name)
	logDebugf(MsgDelete, "Schema", "full_name="+fullName)
	return w.Schemas.Delete(ctx, catalog.DeleteSchemaRequest{
		FullName: fullName,
		Force:    true,
	})
}

func (h *SchemaHandler) Export(ctx context.Context, _ SchemaState) ([]SchemaState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Schema")
	catalogs, err := w.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list catalogs: %w", err)
	}

	var all []SchemaState
	for i := range catalogs {
		schemas, err := w.Schemas.ListAll(ctx, catalog.ListSchemasRequest{CatalogName: catalogs[i].Name})
		if err != nil {
			// Skip catalogs the caller cannot enumerate rather than
			// failing the whole export.
			logInfof(MsgSkipping, "Schema", "catalog="+catalogs[i].Name, err)
			continue
		}
		for j := range schemas {
			all = append(all, schemaInfoToState(&schemas[j]))
		}
	}

	return all, nil
}
