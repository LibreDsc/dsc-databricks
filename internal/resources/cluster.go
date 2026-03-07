package resources

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Cluster", &ClusterHandler{}, clusterMetadata())
}

// Cluster property descriptions from SDK documentation.
var clusterPropertyDescriptions = dsc.PropertyDescriptions{
	"cluster_id":              "Canonical unique identifier for the cluster. Retained across restarts and resizes.",
	"cluster_name":            "Name of the cluster. Does not have to be unique.",
	"spark_version":           "The Spark runtime version (e.g. 15.4.x-scala2.12). Required on create.",
	"node_type_id":            "The node type for worker nodes (e.g. Standard_D4ds_v5).",
	"driver_node_type_id":     "The node type for the driver node. Defaults to node_type_id if unset.",
	"num_workers":             "Number of worker nodes. Mutually exclusive with autoscale.",
	"autoscale_min_workers":   "Minimum number of workers when autoscaling is enabled.",
	"autoscale_max_workers":   "Maximum number of workers when autoscaling is enabled.",
	"autotermination_minutes": "Minutes of inactivity before the cluster auto-terminates. 0 disables.",
	"state":                   "Current lifecycle state of the cluster (PENDING, RUNNING, TERMINATED, etc.). Read-only.",
	"state_message":           "Message associated with the most recent state transition. Read-only.",
	"spark_conf":              "An object of Spark configuration key-value pairs.",
	"custom_tags":             "Additional tags for cluster resources.",
	"data_security_mode":      "Data security mode (e.g. SINGLE_USER, USER_ISOLATION).",
	"single_user_name":        "User name when data_security_mode is SINGLE_USER.",
	"policy_id":               "ID of the cluster policy used to create the cluster.",
	"instance_pool_id":        "ID of the instance pool the cluster belongs to.",
	"driver_instance_pool_id": "ID of the instance pool for the driver node.",
	"enable_elastic_disk":     "Enable autoscaling local storage.",
	"runtime_engine":          "Runtime engine: STANDARD or PHOTON.",
}

// ClusterSchemaInput defines the desired-state fields for the schema and for
// unmarshaling input. cluster_id is computed on create; cluster_name is the
// human-readable identifier — both carry omitempty so inputs supplying only
// one pass schema validation. state and state_message are read-only (omitted
// from input).
type ClusterSchemaInput struct {
	ClusterID              string            `json:"cluster_id,omitempty"`
	ClusterName            string            `json:"cluster_name,omitempty"`
	SparkVersion           string            `json:"spark_version,omitempty"`
	NodeTypeID             string            `json:"node_type_id,omitempty"`
	DriverNodeTypeID       string            `json:"driver_node_type_id,omitempty"`
	NumWorkers             int               `json:"num_workers,omitempty"`
	AutoscaleMinWorkers    int               `json:"autoscale_min_workers,omitempty"`
	AutoscaleMaxWorkers    int               `json:"autoscale_max_workers,omitempty"`
	AutoterminationMinutes int               `json:"autotermination_minutes,omitempty"`
	SparkConf              map[string]string  `json:"spark_conf,omitempty"`
	CustomTags             map[string]string  `json:"custom_tags,omitempty"`
	DataSecurityMode       string            `json:"data_security_mode,omitempty"`
	SingleUserName         string            `json:"single_user_name,omitempty"`
	PolicyID               string            `json:"policy_id,omitempty"`
	InstancePoolID         string            `json:"instance_pool_id,omitempty"`
	DriverInstancePoolID   string            `json:"driver_instance_pool_id,omitempty"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk,omitempty"`
	RuntimeEngine          string            `json:"runtime_engine,omitempty"`
}

func clusterMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/Cluster",
		Description:       "Manage Databricks compute clusters.",
		SchemaDescription: "Schema for managing Databricks compute clusters.",
		ResourceName:      "cluster",
		Tags:              []string{"databricks", "cluster", "compute"},
		Descriptions:      clusterPropertyDescriptions,
		SchemaType:        reflect.TypeFor[ClusterSchemaInput](),
		// state and state_message are server-computed and change over time;
		// they are never part of desired state. All other configurable
		// properties are plain value equality, so the DSC synthetic test
		// (which only checks properties present in desired state) is
		// sufficient.
		OmitTest: true,
	})
}

// ClusterState represents the full state of a Databricks cluster.
type ClusterState struct {
	ClusterID              string            `json:"cluster_id,omitempty"`
	ClusterName            string            `json:"cluster_name,omitempty"`
	SparkVersion           string            `json:"spark_version,omitempty"`
	NodeTypeID             string            `json:"node_type_id,omitempty"`
	DriverNodeTypeID       string            `json:"driver_node_type_id,omitempty"`
	NumWorkers             int               `json:"num_workers,omitempty"`
	AutoscaleMinWorkers    int               `json:"autoscale_min_workers,omitempty"`
	AutoscaleMaxWorkers    int               `json:"autoscale_max_workers,omitempty"`
	AutoterminationMinutes int               `json:"autotermination_minutes,omitempty"`
	State                  string            `json:"state,omitempty"`
	StateMessage           string            `json:"state_message,omitempty"`
	SparkConf              map[string]string  `json:"spark_conf,omitempty"`
	CustomTags             map[string]string  `json:"custom_tags,omitempty"`
	DataSecurityMode       string            `json:"data_security_mode,omitempty"`
	SingleUserName         string            `json:"single_user_name,omitempty"`
	PolicyID               string            `json:"policy_id,omitempty"`
	InstancePoolID         string            `json:"instance_pool_id,omitempty"`
	DriverInstancePoolID   string            `json:"driver_instance_pool_id,omitempty"`
	EnableElasticDisk      bool              `json:"enable_elastic_disk,omitempty"`
	RuntimeEngine          string            `json:"runtime_engine,omitempty"`
	Exist                  bool              `json:"_exist"`
}

// ClusterHandler handles Cluster resource operations.
type ClusterHandler struct{}

func clusterToState(c *compute.ClusterDetails) ClusterState {
	s := ClusterState{
		ClusterID:              c.ClusterId,
		ClusterName:            c.ClusterName,
		SparkVersion:           c.SparkVersion,
		NodeTypeID:             c.NodeTypeId,
		DriverNodeTypeID:       c.DriverNodeTypeId,
		NumWorkers:             c.NumWorkers,
		AutoterminationMinutes: c.AutoterminationMinutes,
		State:                  string(c.State),
		StateMessage:           c.StateMessage,
		DataSecurityMode:       string(c.DataSecurityMode),
		SingleUserName:         c.SingleUserName,
		PolicyID:               c.PolicyId,
		InstancePoolID:         c.InstancePoolId,
		DriverInstancePoolID:   c.DriverInstancePoolId,
		EnableElasticDisk:      c.EnableElasticDisk,
		RuntimeEngine:          string(c.RuntimeEngine),
		Exist:                  true,
	}
	if c.Autoscale != nil {
		s.AutoscaleMinWorkers = c.Autoscale.MinWorkers
		s.AutoscaleMaxWorkers = c.Autoscale.MaxWorkers
	}
	if len(c.SparkConf) > 0 {
		s.SparkConf = c.SparkConf
	}
	if len(c.CustomTags) > 0 {
		s.CustomTags = c.CustomTags
	}
	return s
}

func (h *ClusterHandler) getCurrentState(ctx dsc.ResourceContext, req *ClusterSchemaInput) (ClusterState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return ClusterState{Exist: false}, err
	}

	if req.ClusterID != "" {
		c, err := w.Clusters.GetByClusterId(cmdCtx, req.ClusterID)
		if err != nil {
			return ClusterState{ClusterID: req.ClusterID, Exist: false}, nil
		}
		return clusterToState(c), nil
	}

	if req.ClusterName != "" {
		c, err := w.Clusters.GetByClusterName(cmdCtx, req.ClusterName)
		if err != nil {
			return ClusterState{ClusterName: req.ClusterName, Exist: false}, nil
		}
		return clusterToState(c), nil
	}

	return ClusterState{Exist: false}, fmt.Errorf("cluster_id or cluster_name must be provided")
}

func (h *ClusterHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[ClusterSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateAtLeastOne("cluster_id or cluster_name", req.ClusterID, req.ClusterName); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func buildCreateRequest(input *ClusterSchemaInput) compute.CreateCluster {
	req := compute.CreateCluster{
		ClusterName:            input.ClusterName,
		SparkVersion:           input.SparkVersion,
		NodeTypeId:             input.NodeTypeID,
		DriverNodeTypeId:       input.DriverNodeTypeID,
		NumWorkers:             input.NumWorkers,
		AutoterminationMinutes: input.AutoterminationMinutes,
		SparkConf:              input.SparkConf,
		CustomTags:             input.CustomTags,
		SingleUserName:         input.SingleUserName,
		PolicyId:               input.PolicyID,
		InstancePoolId:         input.InstancePoolID,
		DriverInstancePoolId:   input.DriverInstancePoolID,
		EnableElasticDisk:      input.EnableElasticDisk,
	}
	if input.DataSecurityMode != "" {
		req.DataSecurityMode = compute.DataSecurityMode(input.DataSecurityMode)
	}
	if input.RuntimeEngine != "" {
		req.RuntimeEngine = compute.RuntimeEngine(input.RuntimeEngine)
	}
	if input.AutoscaleMinWorkers > 0 || input.AutoscaleMaxWorkers > 0 {
		req.Autoscale = &compute.AutoScale{
			MinWorkers: input.AutoscaleMinWorkers,
			MaxWorkers: input.AutoscaleMaxWorkers,
		}
	} else {
		// When not using autoscale, always send num_workers explicitly
		// (even 0 for single-node) because the SDK omits zero values.
		req.ForceSendFields = append(req.ForceSendFields, "NumWorkers")
	}
	return req
}

func buildEditRequest(clusterID string, input *ClusterSchemaInput, effectiveName string, effectiveSparkVersion string) compute.EditCluster {
	req := compute.EditCluster{
		ClusterId:              clusterID,
		ClusterName:            effectiveName,
		SparkVersion:           effectiveSparkVersion,
		NodeTypeId:             input.NodeTypeID,
		DriverNodeTypeId:       input.DriverNodeTypeID,
		NumWorkers:             input.NumWorkers,
		AutoterminationMinutes: input.AutoterminationMinutes,
		SparkConf:              input.SparkConf,
		CustomTags:             input.CustomTags,
		SingleUserName:         input.SingleUserName,
		PolicyId:               input.PolicyID,
		InstancePoolId:         input.InstancePoolID,
		DriverInstancePoolId:   input.DriverInstancePoolID,
		EnableElasticDisk:      input.EnableElasticDisk,
	}
	if input.DataSecurityMode != "" {
		req.DataSecurityMode = compute.DataSecurityMode(input.DataSecurityMode)
	}
	if input.RuntimeEngine != "" {
		req.RuntimeEngine = compute.RuntimeEngine(input.RuntimeEngine)
	}
	if input.AutoscaleMinWorkers > 0 || input.AutoscaleMaxWorkers > 0 {
		req.Autoscale = &compute.AutoScale{
			MinWorkers: input.AutoscaleMinWorkers,
			MaxWorkers: input.AutoscaleMaxWorkers,
		}
	} else {
		req.ForceSendFields = append(req.ForceSendFields, "NumWorkers")
	}
	return req
}

func (h *ClusterHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ClusterSchemaInput](input)
	if err != nil {
		return nil, err
	}
	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var afterState ClusterState

	if beforeState.Exist {
		// Cluster exists — edit it. Edit requires the cluster to be RUNNING or TERMINATED.
		effectiveName := schemaInput.ClusterName
		if effectiveName == "" {
			effectiveName = beforeState.ClusterName
		}
		effectiveSparkVersion := schemaInput.SparkVersion
		if effectiveSparkVersion == "" {
			effectiveSparkVersion = beforeState.SparkVersion
		}

		editReq := buildEditRequest(beforeState.ClusterID, &schemaInput, effectiveName, effectiveSparkVersion)

		// If the cluster is TERMINATED, Edit won't restart it — the updated
		// config applies on next start. When RUNNING, wait for restart to
		// complete before we return.
		wait, err := w.Clusters.Edit(cmdCtx, editReq)
		if err != nil {
			return nil, fmt.Errorf("failed to update cluster: %w", err)
		}
		if beforeState.State != "TERMINATED" {
			if _, err := wait.Get(); err != nil {
				return nil, fmt.Errorf("failed waiting for cluster restart: %w", err)
			}
		}

		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.Clusters.GetByClusterId(cmdCtx, beforeState.ClusterID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve updated cluster: %w", err)
		}
		afterState = clusterToState(updated)
	} else {
		// Cluster does not exist — create it.
		if err := dsc.ValidateRequired(
			dsc.RequiredField{Name: "cluster_name", Value: schemaInput.ClusterName},
			dsc.RequiredField{Name: "spark_version", Value: schemaInput.SparkVersion},
		); err != nil {
			return nil, err
		}

		createReq := buildCreateRequest(&schemaInput)
		wait, err := w.Clusters.Create(cmdCtx, createReq)
		if err != nil {
			return nil, fmt.Errorf("failed to create cluster: %w", err)
		}
		created, err := wait.Get()
		if err != nil {
			return nil, fmt.Errorf("failed waiting for cluster to start: %w", err)
		}
		afterState = clusterToState(created)
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *ClusterHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[ClusterSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := ClusterState{
		ClusterID:              schemaInput.ClusterID,
		ClusterName:            schemaInput.ClusterName,
		SparkVersion:           schemaInput.SparkVersion,
		NodeTypeID:             schemaInput.NodeTypeID,
		DriverNodeTypeID:       schemaInput.DriverNodeTypeID,
		NumWorkers:             schemaInput.NumWorkers,
		AutoscaleMinWorkers:    schemaInput.AutoscaleMinWorkers,
		AutoscaleMaxWorkers:    schemaInput.AutoscaleMaxWorkers,
		AutoterminationMinutes: schemaInput.AutoterminationMinutes,
		SparkConf:              schemaInput.SparkConf,
		CustomTags:             schemaInput.CustomTags,
		DataSecurityMode:       schemaInput.DataSecurityMode,
		SingleUserName:         schemaInput.SingleUserName,
		PolicyID:               schemaInput.PolicyID,
		InstancePoolID:         schemaInput.InstancePoolID,
		DriverInstancePoolID:   schemaInput.DriverInstancePoolID,
		EnableElasticDisk:      schemaInput.EnableElasticDisk,
		RuntimeEngine:          schemaInput.RuntimeEngine,
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

func (h *ClusterHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[ClusterSchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	clusterID := schemaInput.ClusterID
	if clusterID == "" && schemaInput.ClusterName != "" {
		c, lookupErr := w.Clusters.GetByClusterName(cmdCtx, schemaInput.ClusterName)
		if lookupErr != nil {
			return dsc.NotFoundError("cluster", "cluster_name="+schemaInput.ClusterName)
		}
		clusterID = c.ClusterId
	}

	if clusterID == "" {
		return dsc.ValidateRequired(dsc.RequiredField{Name: "cluster_id or cluster_name", Value: ""})
	}

	// Terminate the cluster first if it is running, then permanently delete
	// it so it no longer appears in the workspace.
	cluster, err := w.Clusters.GetByClusterId(cmdCtx, clusterID)
	if err != nil {
		// Cluster already gone — nothing to do.
		return nil
	}

	if cluster.State != compute.StateTerminated && cluster.State != compute.StateTerminating {
		wait, err := w.Clusters.Delete(cmdCtx, compute.DeleteCluster{ClusterId: clusterID})
		if err != nil {
			return fmt.Errorf("failed to terminate cluster: %w", err)
		}
		if _, err := wait.Get(); err != nil {
			return fmt.Errorf("failed waiting for cluster termination: %w", err)
		}
	} else if cluster.State == compute.StateTerminating {
		// Wait for termination to complete before permanent delete.
		_, err = w.Clusters.WaitGetClusterTerminated(cmdCtx, clusterID, 20*time.Minute, nil)
		if err != nil {
			return fmt.Errorf("failed waiting for cluster termination: %w", err)
		}
	}

	return w.Clusters.PermanentDeleteByClusterId(cmdCtx, clusterID)
}

func (h *ClusterHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	clusters, err := w.Clusters.ListAll(cmdCtx, compute.ListClustersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}

	var all []any
	for i := range clusters {
		all = append(all, clusterToState(&clusters[i]))
	}

	return all, nil
}
