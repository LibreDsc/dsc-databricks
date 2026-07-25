package resources

import (
	"context"
	"fmt"
	"time"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

// SqlWarehouseState represents the full state of a Databricks SQL warehouse.
// id is computed on create; name is the human-readable identifier — both are
// optional so inputs supplying only one of the two pass schema validation.
type SqlWarehouseState struct {
	dsc.ExistProperty
	ID                      string `json:"id,omitempty" description:"Unique identifier for the SQL warehouse. Computed on create."`
	Name                    string `json:"name,omitempty" description:"Logical name for the warehouse. Must be unique within an org and less than 100 characters."`
	ClusterSize             string `json:"cluster_size,omitempty" description:"Size of clusters allocated (2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large, 5X-Large)."`
	SpotInstancePolicy      string `json:"spot_instance_policy,omitempty" description:"Spot instance policy: COST_OPTIMIZED or RELIABILITY_OPTIMIZED."`
	WarehouseType           string `json:"warehouse_type,omitempty" description:"Warehouse type: PRO or CLASSIC. Set PRO with enable_serverless_compute for serverless."`
	Channel                 string `json:"channel,omitempty" description:"Release channel name: CHANNEL_NAME_CURRENT, CHANNEL_NAME_PREVIEW, or CHANNEL_NAME_PREVIOUS."`
	State                   string `json:"state,omitempty" description:"Current lifecycle state (STARTING, RUNNING, STOPPING, STOPPED, DELETING, DELETED). (read-only)"`
	AutoStopMins            int    `json:"auto_stop_mins,omitempty" description:"Minutes of idle time before auto-stop. Must be 0 (disable) or >= 10. Defaults to 120."`
	MinNumClusters          int    `json:"min_num_clusters,omitempty" description:"Minimum available clusters. Must be > 0 and <= min(max_num_clusters, 30). Defaults to 1."`
	MaxNumClusters          int    `json:"max_num_clusters,omitempty" description:"Maximum clusters for autoscaling. Must be >= min_num_clusters and <= 40."`
	NumClusters             int    `json:"num_clusters,omitempty" description:"Current number of clusters running for the warehouse. (read-only)"`
	EnablePhoton            bool   `json:"enable_photon,omitempty" description:"Whether to use Photon-optimized clusters. Defaults to true."`
	EnableServerlessCompute bool   `json:"enable_serverless_compute,omitempty" description:"Whether to use serverless compute."`
}

func sqlWarehouseConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/SqlWarehouse",
		Version:     "0.1.0",
		Description: "Manage Databricks SQL warehouses.",
		Tags:        []string{"databricks", "sqlwarehouse", "sql"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks SQL warehouses.",
			AllowAdditionalProperties: true,
		},
	}
}

// SqlWarehouseHandler handles SqlWarehouse resource operations. state and
// num_clusters are server-computed and change over time; they are never part
// of desired state. All other configurable properties are plain value
// equality, so no Testable is implemented — the DSC synthetic test suffices.
type SqlWarehouseHandler struct{}

func warehouseResponseToState(w *sql.GetWarehouseResponse) SqlWarehouseState {
	s := SqlWarehouseState{
		ID:                      w.Id,
		Name:                    w.Name,
		ClusterSize:             w.ClusterSize,
		AutoStopMins:            w.AutoStopMins,
		MinNumClusters:          w.MinNumClusters,
		MaxNumClusters:          w.MaxNumClusters,
		EnablePhoton:            w.EnablePhoton,
		EnableServerlessCompute: w.EnableServerlessCompute,
		SpotInstancePolicy:      string(w.SpotInstancePolicy),
		WarehouseType:           string(w.WarehouseType),
		State:                   string(w.State),
		NumClusters:             w.NumClusters,
	}
	if w.Channel != nil {
		s.Channel = string(w.Channel.Name)
	}
	s.SetExist(true)
	return s
}

func endpointInfoToState(w *sql.EndpointInfo) SqlWarehouseState {
	s := SqlWarehouseState{
		ID:                      w.Id,
		Name:                    w.Name,
		ClusterSize:             w.ClusterSize,
		AutoStopMins:            w.AutoStopMins,
		MinNumClusters:          w.MinNumClusters,
		MaxNumClusters:          w.MaxNumClusters,
		EnablePhoton:            w.EnablePhoton,
		EnableServerlessCompute: w.EnableServerlessCompute,
		SpotInstancePolicy:      string(w.SpotInstancePolicy),
		WarehouseType:           string(w.WarehouseType),
		State:                   string(w.State),
		NumClusters:             w.NumClusters,
	}
	if w.Channel != nil {
		s.Channel = string(w.Channel.Name)
	}
	s.SetExist(true)
	return s
}

func (h *SqlWarehouseHandler) Get(ctx context.Context, in SqlWarehouseState) (SqlWarehouseState, error) {
	if err := requireAtLeastOne("id or name", in.ID, in.Name); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.ID != "" {
		logDebugf(MsgLookup, "SqlWarehouse", "id="+in.ID)
		resp, err := w.Warehouses.GetById(ctx, in.ID)
		if err != nil {
			logInfof(MsgNotFound, "SqlWarehouse", "id="+in.ID)
			return dsc.NotFound(SqlWarehouseState{ID: in.ID}, "SqlWarehouse", "id="+in.ID)
		}
		return warehouseResponseToState(resp), nil
	}

	logDebugf(MsgLookup, "SqlWarehouse", "name="+in.Name)
	info, err := w.Warehouses.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "SqlWarehouse", "name="+in.Name)
		return dsc.NotFound(SqlWarehouseState{Name: in.Name}, "SqlWarehouse", "name="+in.Name)
	}
	return endpointInfoToState(info), nil
}

func buildCreateWarehouseRequest(input *SqlWarehouseState) sql.CreateWarehouseRequest {
	req := sql.CreateWarehouseRequest{
		Name:                    input.Name,
		ClusterSize:             input.ClusterSize,
		AutoStopMins:            input.AutoStopMins,
		MinNumClusters:          input.MinNumClusters,
		MaxNumClusters:          input.MaxNumClusters,
		EnablePhoton:            input.EnablePhoton,
		EnableServerlessCompute: input.EnableServerlessCompute,
	}
	if input.SpotInstancePolicy != "" {
		req.SpotInstancePolicy = sql.SpotInstancePolicy(input.SpotInstancePolicy)
	}
	if input.WarehouseType != "" {
		req.WarehouseType = sql.CreateWarehouseRequestWarehouseType(input.WarehouseType)
	}
	if input.Channel != "" {
		req.Channel = &sql.Channel{Name: sql.ChannelName(input.Channel)}
	}
	// auto_stop_mins=0 means disable auto-stop; must be force-sent.
	if input.AutoStopMins == 0 {
		req.ForceSendFields = append(req.ForceSendFields, "AutoStopMins")
	}
	return req
}

func buildEditWarehouseRequest(id string, input *SqlWarehouseState, effectiveName string) sql.EditWarehouseRequest {
	req := sql.EditWarehouseRequest{
		Id:                      id,
		Name:                    effectiveName,
		ClusterSize:             input.ClusterSize,
		AutoStopMins:            input.AutoStopMins,
		MinNumClusters:          input.MinNumClusters,
		MaxNumClusters:          input.MaxNumClusters,
		EnablePhoton:            input.EnablePhoton,
		EnableServerlessCompute: input.EnableServerlessCompute,
	}
	if input.SpotInstancePolicy != "" {
		req.SpotInstancePolicy = sql.SpotInstancePolicy(input.SpotInstancePolicy)
	}
	if input.WarehouseType != "" {
		req.WarehouseType = sql.EditWarehouseRequestWarehouseType(input.WarehouseType)
	}
	if input.Channel != "" {
		req.Channel = &sql.Channel{Name: sql.ChannelName(input.Channel)}
	}
	if input.AutoStopMins == 0 {
		req.ForceSendFields = append(req.ForceSendFields, "AutoStopMins")
	}
	return req
}

func (h *SqlWarehouseHandler) Set(ctx context.Context, desired SqlWarehouseState) (SqlWarehouseState, error) {
	if err := requireAtLeastOne("id or name", desired.ID, desired.Name); err != nil {
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
		logInfof(MsgUpdate, "SqlWarehouse", "id="+current.ID)
		// Warehouse exists — edit it.
		effectiveName := desired.Name
		if effectiveName == "" {
			effectiveName = current.Name
		}

		editReq := buildEditWarehouseRequest(current.ID, &desired, effectiveName)

		// If the warehouse is STOPPED, Edit applies the config for next start
		// without restarting. If RUNNING, wait for the restart to complete.
		wait, err := w.Warehouses.Edit(ctx, editReq)
		if err != nil {
			return desired, fmt.Errorf("failed to update SQL warehouse: %w", err)
		}
		if current.State != "STOPPED" {
			if _, err := wait.Get(); err != nil {
				return desired, fmt.Errorf("failed waiting for SQL warehouse restart: %w", err)
			}
		}

		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.Warehouses.GetById(ctx, current.ID)
		if err != nil {
			return desired, fmt.Errorf("failed to retrieve updated SQL warehouse: %w", err)
		}
		return warehouseResponseToState(updated), nil
	}

	logInfof(MsgCreate, "SqlWarehouse", "name="+desired.Name)
	// Warehouse does not exist — create it.
	if err := requireFields(
		field{"name", desired.Name},
		field{"cluster_size", desired.ClusterSize},
	); err != nil {
		return desired, err
	}

	createReq := buildCreateWarehouseRequest(&desired)
	wait, err := w.Warehouses.Create(ctx, createReq)
	if err != nil {
		return desired, fmt.Errorf("failed to create SQL warehouse: %w", err)
	}
	created, err := wait.Get()
	if err != nil {
		return desired, fmt.Errorf("failed waiting for SQL warehouse to start: %w", err)
	}
	return warehouseResponseToState(created), nil
}

// projectSqlWarehouseCreate returns the state Set's create path would
// produce. Server-computed fields (id, state, num_clusters) stay empty.
func projectSqlWarehouseCreate(desired *SqlWarehouseState) SqlWarehouseState {
	projected := *desired
	projected.ID = ""
	projected.State = ""
	projected.NumClusters = 0
	projected.SetExist(true)
	return projected
}

// projectSqlWarehouseUpdate mirrors buildEditWarehouseRequest: non-zero
// desired values win, auto_stop_mins always comes from desired (Set
// force-sends it even at 0), and read-only fields carry over from current.
func projectSqlWarehouseUpdate(desired, current *SqlWarehouseState) SqlWarehouseState {
	projected := *current
	if desired.Name != "" {
		projected.Name = desired.Name
	}
	if desired.ClusterSize != "" {
		projected.ClusterSize = desired.ClusterSize
	}
	if desired.SpotInstancePolicy != "" {
		projected.SpotInstancePolicy = desired.SpotInstancePolicy
	}
	if desired.WarehouseType != "" {
		projected.WarehouseType = desired.WarehouseType
	}
	if desired.Channel != "" {
		projected.Channel = desired.Channel
	}
	projected.AutoStopMins = desired.AutoStopMins
	if desired.MinNumClusters > 0 {
		projected.MinNumClusters = desired.MinNumClusters
	}
	if desired.MaxNumClusters > 0 {
		projected.MaxNumClusters = desired.MaxNumClusters
	}
	if desired.EnablePhoton {
		projected.EnablePhoton = true
	}
	if desired.EnableServerlessCompute {
		projected.EnableServerlessCompute = true
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the
// warehouse.
func (h *SqlWarehouseHandler) SetWhatIf(ctx context.Context, desired SqlWarehouseState) (SqlWarehouseState, error) {
	if err := requireAtLeastOne("id or name", desired.ID, desired.Name); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "SqlWarehouse", "id="+current.ID)
		return projectSqlWarehouseUpdate(&desired, &current), nil
	}

	if err := requireFields(
		field{"name", desired.Name},
		field{"cluster_size", desired.ClusterSize},
	); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "SqlWarehouse", "name="+desired.Name)
	return projectSqlWarehouseCreate(&desired), nil
}

func (h *SqlWarehouseHandler) Delete(ctx context.Context, in SqlWarehouseState) error {
	if err := requireAtLeastOne("id or name", in.ID, in.Name); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	warehouseID := in.ID
	if warehouseID == "" {
		info, lookupErr := w.Warehouses.GetByName(ctx, in.Name)
		if lookupErr != nil {
			// Already absent — deleting a missing instance succeeds.
			logInfof(MsgNotFound, "SqlWarehouse", "name="+in.Name)
			return nil
		}
		warehouseID = info.Id
	}

	logDebugf(MsgDelete, "SqlWarehouse", "id="+warehouseID)
	// Stop the warehouse first if it is running, then delete it.
	resp, err := w.Warehouses.GetById(ctx, warehouseID)
	if err != nil {
		// Warehouse already gone — nothing to do.
		return nil
	}

	switch resp.State {
	case sql.StateRunning, sql.StateStarting:
		wait, err := w.Warehouses.Stop(ctx, sql.StopRequest{Id: warehouseID})
		if err != nil {
			return fmt.Errorf("failed to stop SQL warehouse: %w", err)
		}
		if _, err := wait.Get(); err != nil {
			return fmt.Errorf("failed waiting for SQL warehouse to stop: %w", err)
		}
	case sql.StateStopping:
		_, err = w.Warehouses.WaitGetWarehouseStopped(ctx, warehouseID, 20*time.Minute, nil)
		if err != nil {
			return fmt.Errorf("failed waiting for SQL warehouse to stop: %w", err)
		}
	}

	return w.Warehouses.DeleteById(ctx, warehouseID)
}

func (h *SqlWarehouseHandler) Export(ctx context.Context, _ SqlWarehouseState) ([]SqlWarehouseState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "SqlWarehouse")
	warehouses, err := w.Warehouses.ListAll(ctx, sql.ListWarehousesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list SQL warehouses: %w", err)
	}

	var all []SqlWarehouseState
	for i := range warehouses {
		all = append(all, endpointInfoToState(&warehouses[i]))
	}

	return all, nil
}
