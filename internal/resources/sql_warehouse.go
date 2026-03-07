package resources

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/SqlWarehouse", &SqlWarehouseHandler{}, sqlWarehouseMetadata())
}

// SQL Warehouse property descriptions from SDK documentation.
var sqlWarehousePropertyDescriptions = dsc.PropertyDescriptions{
	"id":                        "Unique identifier for the SQL warehouse. Computed on create.",
	"name":                      "Logical name for the warehouse. Must be unique within an org and less than 100 characters.",
	"cluster_size":              "Size of clusters allocated (2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large, 5X-Large).",
	"auto_stop_mins":            "Minutes of idle time before auto-stop. Must be 0 (disable) or >= 10. Defaults to 120.",
	"min_num_clusters":          "Minimum available clusters. Must be > 0 and <= min(max_num_clusters, 30). Defaults to 1.",
	"max_num_clusters":          "Maximum clusters for autoscaling. Must be >= min_num_clusters and <= 40.",
	"enable_photon":             "Whether to use Photon-optimized clusters. Defaults to true.",
	"enable_serverless_compute": "Whether to use serverless compute.",
	"spot_instance_policy":      "Spot instance policy: COST_OPTIMIZED or RELIABILITY_OPTIMIZED.",
	"warehouse_type":            "Warehouse type: PRO or CLASSIC. Set PRO with enable_serverless_compute for serverless.",
	"channel":                   "Release channel name: CHANNEL_NAME_CURRENT, CHANNEL_NAME_PREVIEW, or CHANNEL_NAME_PREVIOUS.",
	"state":                     "Current lifecycle state (STARTING, RUNNING, STOPPING, STOPPED, DELETING, DELETED). Read-only.",
	"num_clusters":              "Current number of clusters running for the warehouse. Read-only.",
}

// SqlWarehouseSchemaInput defines the desired-state fields for the schema and
// for unmarshaling input. id is computed on create; name is the human-readable
// identifier — both carry omitempty so inputs supplying only one pass schema
// validation. state and num_clusters are read-only.
type SqlWarehouseSchemaInput struct {
	ID                     string `json:"id,omitempty"`
	Name                   string `json:"name,omitempty"`
	ClusterSize            string `json:"cluster_size,omitempty"`
	AutoStopMins           int    `json:"auto_stop_mins,omitempty"`
	MinNumClusters         int    `json:"min_num_clusters,omitempty"`
	MaxNumClusters         int    `json:"max_num_clusters,omitempty"`
	EnablePhoton           bool   `json:"enable_photon,omitempty"`
	EnableServerlessCompute bool   `json:"enable_serverless_compute,omitempty"`
	SpotInstancePolicy     string `json:"spot_instance_policy,omitempty"`
	WarehouseType          string `json:"warehouse_type,omitempty"`
	Channel                string `json:"channel,omitempty"`
}

func sqlWarehouseMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/SqlWarehouse",
		Description:       "Manage Databricks SQL warehouses.",
		SchemaDescription: "Schema for managing Databricks SQL warehouses.",
		ResourceName:      "sql_warehouse",
		Tags:              []string{"databricks", "sqlwarehouse", "sql"},
		Descriptions:      sqlWarehousePropertyDescriptions,
		SchemaType:        reflect.TypeFor[SqlWarehouseSchemaInput](),
		// state and num_clusters are server-computed and change over time;
		// they are never part of desired state. All other configurable
		// properties are plain value equality, so the DSC synthetic test is
		// sufficient.
		OmitTest: true,
	})
}

// SqlWarehouseState represents the full state of a Databricks SQL warehouse.
type SqlWarehouseState struct {
	ID                     string `json:"id,omitempty"`
	Name                   string `json:"name,omitempty"`
	ClusterSize            string `json:"cluster_size,omitempty"`
	AutoStopMins           int    `json:"auto_stop_mins,omitempty"`
	MinNumClusters         int    `json:"min_num_clusters,omitempty"`
	MaxNumClusters         int    `json:"max_num_clusters,omitempty"`
	EnablePhoton           bool   `json:"enable_photon,omitempty"`
	EnableServerlessCompute bool   `json:"enable_serverless_compute,omitempty"`
	SpotInstancePolicy     string `json:"spot_instance_policy,omitempty"`
	WarehouseType          string `json:"warehouse_type,omitempty"`
	Channel                string `json:"channel,omitempty"`
	State                  string `json:"state,omitempty"`
	NumClusters            int    `json:"num_clusters,omitempty"`
	Exist                  bool   `json:"_exist"`
}

// SqlWarehouseHandler handles SqlWarehouse resource operations.
type SqlWarehouseHandler struct{}

func warehouseResponseToState(w *sql.GetWarehouseResponse) SqlWarehouseState {
	s := SqlWarehouseState{
		ID:                     w.Id,
		Name:                   w.Name,
		ClusterSize:            w.ClusterSize,
		AutoStopMins:           w.AutoStopMins,
		MinNumClusters:         w.MinNumClusters,
		MaxNumClusters:         w.MaxNumClusters,
		EnablePhoton:           w.EnablePhoton,
		EnableServerlessCompute: w.EnableServerlessCompute,
		SpotInstancePolicy:     string(w.SpotInstancePolicy),
		WarehouseType:          string(w.WarehouseType),
		State:                  string(w.State),
		NumClusters:            w.NumClusters,
		Exist:                  true,
	}
	if w.Channel != nil {
		s.Channel = string(w.Channel.Name)
	}
	return s
}

func endpointInfoToState(w *sql.EndpointInfo) SqlWarehouseState {
	s := SqlWarehouseState{
		ID:                     w.Id,
		Name:                   w.Name,
		ClusterSize:            w.ClusterSize,
		AutoStopMins:           w.AutoStopMins,
		MinNumClusters:         w.MinNumClusters,
		MaxNumClusters:         w.MaxNumClusters,
		EnablePhoton:           w.EnablePhoton,
		EnableServerlessCompute: w.EnableServerlessCompute,
		SpotInstancePolicy:     string(w.SpotInstancePolicy),
		WarehouseType:          string(w.WarehouseType),
		State:                  string(w.State),
		NumClusters:            w.NumClusters,
		Exist:                  true,
	}
	if w.Channel != nil {
		s.Channel = string(w.Channel.Name)
	}
	return s
}

func (h *SqlWarehouseHandler) getCurrentState(ctx dsc.ResourceContext, req *SqlWarehouseSchemaInput) (SqlWarehouseState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return SqlWarehouseState{Exist: false}, err
	}

	if req.ID != "" {
		resp, err := w.Warehouses.GetById(cmdCtx, req.ID)
		if err != nil {
			return SqlWarehouseState{ID: req.ID, Exist: false}, nil
		}
		return warehouseResponseToState(resp), nil
	}

	if req.Name != "" {
		info, err := w.Warehouses.GetByName(cmdCtx, req.Name)
		if err != nil {
			return SqlWarehouseState{Name: req.Name, Exist: false}, nil
		}
		return endpointInfoToState(info), nil
	}

	return SqlWarehouseState{Exist: false}, fmt.Errorf("id or name must be provided")
}

func (h *SqlWarehouseHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[SqlWarehouseSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("id or name", req.ID, req.Name); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func buildCreateWarehouseRequest(input *SqlWarehouseSchemaInput) sql.CreateWarehouseRequest {
	req := sql.CreateWarehouseRequest{
		Name:                   input.Name,
		ClusterSize:            input.ClusterSize,
		AutoStopMins:           input.AutoStopMins,
		MinNumClusters:         input.MinNumClusters,
		MaxNumClusters:         input.MaxNumClusters,
		EnablePhoton:           input.EnablePhoton,
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

func buildEditWarehouseRequest(id string, input *SqlWarehouseSchemaInput, effectiveName string) sql.EditWarehouseRequest {
	req := sql.EditWarehouseRequest{
		Id:                     id,
		Name:                   effectiveName,
		ClusterSize:            input.ClusterSize,
		AutoStopMins:           input.AutoStopMins,
		MinNumClusters:         input.MinNumClusters,
		MaxNumClusters:         input.MaxNumClusters,
		EnablePhoton:           input.EnablePhoton,
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

func (h *SqlWarehouseHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[SqlWarehouseSchemaInput](input)
	if err != nil {
		return nil, err
	}
	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var afterState SqlWarehouseState

	if beforeState.Exist {
		// Warehouse exists — edit it.
		effectiveName := schemaInput.Name
		if effectiveName == "" {
			effectiveName = beforeState.Name
		}

		editReq := buildEditWarehouseRequest(beforeState.ID, &schemaInput, effectiveName)

		// If the warehouse is STOPPED, Edit applies the config for next start
		// without restarting. If RUNNING, wait for the restart to complete.
		wait, err := w.Warehouses.Edit(cmdCtx, editReq)
		if err != nil {
			return nil, fmt.Errorf("failed to update SQL warehouse: %w", err)
		}
		if beforeState.State != "STOPPED" {
			if _, err := wait.Get(); err != nil {
				return nil, fmt.Errorf("failed waiting for SQL warehouse restart: %w", err)
			}
		}

		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.Warehouses.GetById(cmdCtx, beforeState.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve updated SQL warehouse: %w", err)
		}
		afterState = warehouseResponseToState(updated)
	} else {
		// Warehouse does not exist — create it.
		if err := dsc.ValidateRequired(
			dsc.RequiredField{Name: "name", Value: schemaInput.Name},
			dsc.RequiredField{Name: "cluster_size", Value: schemaInput.ClusterSize},
		); err != nil {
			return nil, err
		}

		createReq := buildCreateWarehouseRequest(&schemaInput)
		wait, err := w.Warehouses.Create(cmdCtx, createReq)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQL warehouse: %w", err)
		}
		created, err := wait.Get()
		if err != nil {
			return nil, fmt.Errorf("failed waiting for SQL warehouse to start: %w", err)
		}
		afterState = warehouseResponseToState(created)
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *SqlWarehouseHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[SqlWarehouseSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := SqlWarehouseState{
		ID:                     schemaInput.ID,
		Name:                   schemaInput.Name,
		ClusterSize:            schemaInput.ClusterSize,
		AutoStopMins:           schemaInput.AutoStopMins,
		MinNumClusters:         schemaInput.MinNumClusters,
		MaxNumClusters:         schemaInput.MaxNumClusters,
		EnablePhoton:           schemaInput.EnablePhoton,
		EnableServerlessCompute: schemaInput.EnableServerlessCompute,
		SpotInstancePolicy:     schemaInput.SpotInstancePolicy,
		WarehouseType:          schemaInput.WarehouseType,
		Channel:                schemaInput.Channel,
		Exist:                  true,
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

func (h *SqlWarehouseHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[SqlWarehouseSchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	warehouseID := schemaInput.ID
	if warehouseID == "" && schemaInput.Name != "" {
		info, lookupErr := w.Warehouses.GetByName(cmdCtx, schemaInput.Name)
		if lookupErr != nil {
			return dsc.NotFoundError("SQL warehouse", "name="+schemaInput.Name)
		}
		warehouseID = info.Id
	}

	if warehouseID == "" {
		return dsc.ValidateRequired(dsc.RequiredField{Name: "id or name", Value: ""})
	}

	// Stop the warehouse first if it is running, then delete it.
	resp, err := w.Warehouses.GetById(cmdCtx, warehouseID)
	if err != nil {
		// Warehouse already gone — nothing to do.
		return nil
	}

	switch resp.State {
	case sql.StateRunning, sql.StateStarting:
		wait, err := w.Warehouses.Stop(cmdCtx, sql.StopRequest{Id: warehouseID})
		if err != nil {
			return fmt.Errorf("failed to stop SQL warehouse: %w", err)
		}
		if _, err := wait.Get(); err != nil {
			return fmt.Errorf("failed waiting for SQL warehouse to stop: %w", err)
		}
	case sql.StateStopping:
		_, err = w.Warehouses.WaitGetWarehouseStopped(cmdCtx, warehouseID, 20*time.Minute, nil)
		if err != nil {
			return fmt.Errorf("failed waiting for SQL warehouse to stop: %w", err)
		}
	}

	return w.Warehouses.DeleteById(cmdCtx, warehouseID)
}

func (h *SqlWarehouseHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	warehouses, err := w.Warehouses.ListAll(cmdCtx, sql.ListWarehousesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list SQL warehouses: %w", err)
	}

	var all []any
	for i := range warehouses {
		all = append(all, endpointInfoToState(&warehouses[i]))
	}

	return all, nil
}
