package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// VolumeState represents the full state of a Unity Catalog volume.
type VolumeState struct {
	dsc.ExistProperty
	Name            string `json:"name" description:"Name of the volume, relative to its parent schema."`
	CatalogName     string `json:"catalog_name" description:"Name of the parent catalog."`
	SchemaName      string `json:"schema_name" description:"Name of the parent schema."`
	FullName        string `json:"full_name,omitempty" description:"Full name of the volume in the form catalog_name.schema_name.volume_name. (read-only)"`
	VolumeType      string `json:"volume_type,omitempty" description:"The type of the volume: MANAGED or EXTERNAL. Required when creating. (create-only)" enum:"MANAGED,EXTERNAL"`
	StorageLocation string `json:"storage_location,omitempty" description:"Storage location of the volume. Required when creating an EXTERNAL volume; computed by the server for MANAGED volumes. (create-only)"`
	Comment         string `json:"comment,omitempty" description:"User-provided free-form text description."`
	Owner           string `json:"owner,omitempty" description:"Username of the current owner of the volume."`
	VolumeID        string `json:"volume_id,omitempty" description:"Unique identifier of the volume. (read-only)"`
	MetastoreID     string `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
}

// volumeFullName returns the three-level identifier catalog.schema.volume.
func volumeFullName(catalogName, schemaName, name string) string {
	return catalogName + "." + schemaName + "." + name
}

func volumeConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Volume",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog volumes in a Databricks workspace.",
		Tags:        []string{"databricks", "volume", "unitycatalog"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog volumes.",
			AllowAdditionalProperties: true,
		},
	}
}

// VolumeHandler handles Volume resource operations.
type VolumeHandler struct{}

func volumeInfoToState(v *catalog.VolumeInfo) VolumeState {
	state := VolumeState{
		Name:            v.Name,
		CatalogName:     v.CatalogName,
		SchemaName:      v.SchemaName,
		FullName:        v.FullName,
		VolumeType:      string(v.VolumeType),
		StorageLocation: v.StorageLocation,
		Comment:         v.Comment,
		Owner:           v.Owner,
		VolumeID:        v.VolumeId,
		MetastoreID:     v.MetastoreId,
	}
	state.SetExist(true)
	return state
}

func (h *VolumeHandler) Get(ctx context.Context, in VolumeState) (VolumeState, error) {
	if err := requireFields(
		field{"name", in.Name},
		field{"catalog_name", in.CatalogName},
		field{"schema_name", in.SchemaName},
	); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	fullName := volumeFullName(in.CatalogName, in.SchemaName, in.Name)
	logDebugf(MsgLookup, "Volume", "full_name="+fullName)
	v, err := w.Volumes.ReadByName(ctx, fullName)
	if err != nil {
		logInfof(MsgNotFound, "Volume", "full_name="+fullName)
		return dsc.NotFound(VolumeState{
			Name:        in.Name,
			CatalogName: in.CatalogName,
			SchemaName:  in.SchemaName,
		}, "Volume", "full_name="+fullName)
	}

	return volumeInfoToState(v), nil
}

func (h *VolumeHandler) Set(ctx context.Context, desired VolumeState) (VolumeState, error) {
	if err := requireFields(
		field{"name", desired.Name},
		field{"catalog_name", desired.CatalogName},
		field{"schema_name", desired.SchemaName},
	); err != nil {
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

	fullName := volumeFullName(desired.CatalogName, desired.SchemaName, desired.Name)
	if current.ShouldExist() {
		// Only comment and owner are updatable; volume_type and
		// storage_location are create-only and drift is not corrected.
		logInfof(MsgUpdate, "Volume", "full_name="+fullName)
		updated, err := w.Volumes.Update(ctx, catalog.UpdateVolumeRequestContent{
			Name:    fullName,
			Comment: desired.Comment,
			Owner:   desired.Owner,
		})
		if err != nil {
			return desired, fmt.Errorf("failed to update volume: %w", err)
		}
		return volumeInfoToState(updated), nil
	}

	if err := requireFields(field{"volume_type", desired.VolumeType}); err != nil {
		return desired, err
	}

	logInfof(MsgCreate, "Volume", "full_name="+fullName)
	if _, err := w.Volumes.Create(ctx, catalog.CreateVolumeRequestContent{
		CatalogName:     desired.CatalogName,
		SchemaName:      desired.SchemaName,
		Name:            desired.Name,
		VolumeType:      catalog.VolumeType(desired.VolumeType),
		StorageLocation: desired.StorageLocation,
		Comment:         desired.Comment,
	}); err != nil {
		return desired, fmt.Errorf("failed to create volume: %w", err)
	}

	// Owner is not part of the create API; apply with a follow-up update.
	if desired.Owner != "" {
		if _, err := w.Volumes.Update(ctx, catalog.UpdateVolumeRequestContent{
			Name:  fullName,
			Owner: desired.Owner,
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create volume settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// projectVolumeCreate returns the state Set's create path would produce.
// volume_id and metastore_id are computed by the server and stay empty, as
// does storage_location for MANAGED volumes (server-assigned); full_name is
// deterministic. Owner is applied by the chained post-create update.
func projectVolumeCreate(desired *VolumeState) VolumeState {
	projected := VolumeState{
		Name:            desired.Name,
		CatalogName:     desired.CatalogName,
		SchemaName:      desired.SchemaName,
		FullName:        volumeFullName(desired.CatalogName, desired.SchemaName, desired.Name),
		VolumeType:      desired.VolumeType,
		StorageLocation: desired.StorageLocation,
		Comment:         desired.Comment,
		Owner:           desired.Owner,
	}
	projected.SetExist(true)
	return projected
}

// projectVolumeUpdate mirrors catalog.UpdateVolumeRequestContent: only
// comment and owner are sent (omit-empty), everything else carries over from
// the current state.
func projectVolumeUpdate(desired, current *VolumeState) VolumeState {
	projected := *current
	if desired.Comment != "" {
		projected.Comment = desired.Comment
	}
	if desired.Owner != "" {
		projected.Owner = desired.Owner
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the volume.
func (h *VolumeHandler) SetWhatIf(ctx context.Context, desired VolumeState) (VolumeState, error) {
	if err := requireFields(
		field{"name", desired.Name},
		field{"catalog_name", desired.CatalogName},
		field{"schema_name", desired.SchemaName},
	); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	fullName := volumeFullName(desired.CatalogName, desired.SchemaName, desired.Name)
	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Volume", "full_name="+fullName)
		return projectVolumeUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"volume_type", desired.VolumeType}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "Volume", "full_name="+fullName)
	return projectVolumeCreate(&desired), nil
}

func (h *VolumeHandler) Delete(ctx context.Context, in VolumeState) error {
	if err := requireFields(
		field{"name", in.Name},
		field{"catalog_name", in.CatalogName},
		field{"schema_name", in.SchemaName},
	); err != nil {
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

	fullName := volumeFullName(in.CatalogName, in.SchemaName, in.Name)
	logDebugf(MsgDelete, "Volume", "full_name="+fullName)
	return w.Volumes.DeleteByName(ctx, fullName)
}

// Export walks catalog -> schema -> volume, which issues one list call per
// schema; on large metastores this is expensive.
func (h *VolumeHandler) Export(ctx context.Context, _ VolumeState) ([]VolumeState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Volume")
	catalogs, err := w.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list catalogs: %w", err)
	}

	var all []VolumeState
	for i := range catalogs {
		schemas, err := w.Schemas.ListAll(ctx, catalog.ListSchemasRequest{CatalogName: catalogs[i].Name})
		if err != nil {
			logInfof(MsgSkipping, "Volume", "catalog="+catalogs[i].Name, err)
			continue
		}
		for j := range schemas {
			volumes, err := w.Volumes.ListAll(ctx, catalog.ListVolumesRequest{
				CatalogName: schemas[j].CatalogName,
				SchemaName:  schemas[j].Name,
			})
			if err != nil {
				logInfof(MsgSkipping, "Volume", "schema="+schemas[j].FullName, err)
				continue
			}
			for k := range volumes {
				all = append(all, volumeInfoToState(&volumes[k]))
			}
		}
	}

	return all, nil
}
