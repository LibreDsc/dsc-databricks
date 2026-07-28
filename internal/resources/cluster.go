package resources

import (
	"context"
	"fmt"
	"time"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

// ClusterState represents the full state of a Databricks cluster. cluster_id
// is computed on create; cluster_name is the human-readable identifier — both
// are optional so inputs supplying only one of the two pass schema validation.
type ClusterState struct {
	dsc.ExistProperty
	SparkConf              map[string]string `json:"spark_conf,omitempty" description:"An object of Spark configuration key-value pairs."`
	CustomTags             map[string]string `json:"custom_tags,omitempty" description:"Additional tags for cluster resources."`
	ClusterID              string            `json:"cluster_id,omitempty" description:"Canonical unique identifier for the cluster. Retained across restarts and resizes."`
	ClusterName            string            `json:"cluster_name,omitempty" description:"Name of the cluster. Does not have to be unique."`
	SparkVersion           string            `json:"spark_version,omitempty" description:"The Spark runtime version (e.g. 15.4.x-scala2.12). Required on create."`
	NodeTypeID             string            `json:"node_type_id,omitempty" description:"The node type for worker nodes (e.g. Standard_D4ds_v5)."`
	DriverNodeTypeID       string            `json:"driver_node_type_id,omitempty" description:"The node type for the driver node. Defaults to node_type_id if unset."`
	State                  string            `json:"state,omitempty" description:"Current lifecycle state of the cluster (PENDING, RUNNING, TERMINATED, etc.). (read-only)"`
	StateMessage           string            `json:"state_message,omitempty" description:"Message associated with the most recent state transition. (read-only)"`
	DataSecurityMode       string            `json:"data_security_mode,omitempty" description:"Data security mode (e.g. SINGLE_USER, USER_ISOLATION)."`
	SingleUserName         string            `json:"single_user_name,omitempty" description:"User name when data_security_mode is SINGLE_USER."`
	PolicyID               string            `json:"policy_id,omitempty" description:"ID of the cluster policy used to create the cluster."`
	InstancePoolID         string            `json:"instance_pool_id,omitempty" description:"ID of the instance pool the cluster belongs to."`
	DriverInstancePoolID   string            `json:"driver_instance_pool_id,omitempty" description:"ID of the instance pool for the driver node."`
	RuntimeEngine          string            `json:"runtime_engine,omitempty" description:"Runtime engine: STANDARD or PHOTON."`
	Kind                   string            `json:"kind,omitempty" enum:"CLASSIC_PREVIEW" description:"The kind of compute described by this specification. CLASSIC_PREVIEW enables next-generation cluster features such as is_single_node."`
	AzureAvailability      string            `json:"azure_availability,omitempty" enum:"SPOT_AZURE,ON_DEMAND_AZURE,SPOT_WITH_FALLBACK_AZURE" description:"Azure availability type for cluster nodes. SPOT_WITH_FALLBACK_AZURE falls back to on-demand when spot capacity is unavailable."`
	NumWorkers             int               `json:"num_workers,omitempty" description:"Number of worker nodes. Mutually exclusive with autoscale."`
	AutoscaleMinWorkers    int               `json:"autoscale_min_workers,omitempty" description:"Minimum number of workers when autoscaling is enabled."`
	AutoscaleMaxWorkers    int               `json:"autoscale_max_workers,omitempty" description:"Maximum number of workers when autoscaling is enabled."`
	AutoterminationMinutes int               `json:"autotermination_minutes,omitempty" description:"Minutes of inactivity before the cluster auto-terminates. 0 disables."`
	EnableElasticDisk      bool              `json:"enable_elastic_disk,omitempty" description:"Enable autoscaling local storage."`
	IsSingleNode           bool              `json:"is_single_node,omitempty" description:"Create a single-node cluster (no workers). Can only be used when kind is CLASSIC_PREVIEW."`
}

func clusterConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Cluster",
		Version:     "0.1.0",
		Description: "Manage Databricks compute clusters.",
		Tags:        []string{"databricks", "cluster", "compute"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks compute clusters.",
			AllowAdditionalProperties: true,
		},
	}
}

// ClusterHandler handles Cluster resource operations. state and state_message
// are server-computed and change over time; they are never part of desired
// state. All other configurable properties are plain value equality, so no
// Testable is implemented — the DSC synthetic test (which only checks
// properties present in desired state) is sufficient.
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
		Kind:                   string(c.Kind),
		IsSingleNode:           c.IsSingleNode,
	}
	if c.AzureAttributes != nil {
		s.AzureAvailability = string(c.AzureAttributes.Availability)
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
	s.SetExist(true)
	return s
}

func (h *ClusterHandler) Get(ctx context.Context, in ClusterState) (ClusterState, error) {
	if err := requireAtLeastOne("cluster_id or cluster_name", in.ClusterID, in.ClusterName); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.ClusterID != "" {
		logDebugf(MsgLookup, "Cluster", "cluster_id="+in.ClusterID)
		c, err := w.Clusters.GetByClusterId(ctx, in.ClusterID)
		if err != nil {
			logInfof(MsgNotFound, "Cluster", "cluster_id="+in.ClusterID)
			return dsc.NotFound(ClusterState{ClusterID: in.ClusterID}, "Cluster", "cluster_id="+in.ClusterID)
		}
		return clusterToState(c), nil
	}

	logDebugf(MsgLookup, "Cluster", "cluster_name="+in.ClusterName)
	c, err := w.Clusters.GetByClusterName(ctx, in.ClusterName)
	if err != nil {
		logInfof(MsgNotFound, "Cluster", "cluster_name="+in.ClusterName)
		return dsc.NotFound(ClusterState{ClusterName: in.ClusterName}, "Cluster", "cluster_name="+in.ClusterName)
	}
	return clusterToState(c), nil
}

func buildCreateRequest(input *ClusterState) compute.CreateCluster {
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
		IsSingleNode:           input.IsSingleNode,
	}
	if input.DataSecurityMode != "" {
		req.DataSecurityMode = compute.DataSecurityMode(input.DataSecurityMode)
	}
	if input.RuntimeEngine != "" {
		req.RuntimeEngine = compute.RuntimeEngine(input.RuntimeEngine)
	}
	if input.Kind != "" {
		req.Kind = compute.Kind(input.Kind)
	}
	if input.AzureAvailability != "" {
		req.AzureAttributes = &compute.AzureAttributes{
			Availability: compute.AzureAvailability(input.AzureAvailability),
		}
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

func buildEditRequest(clusterID string, input *ClusterState, effectiveName string, effectiveSparkVersion string) compute.EditCluster {
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
		IsSingleNode:           input.IsSingleNode,
	}
	if input.DataSecurityMode != "" {
		req.DataSecurityMode = compute.DataSecurityMode(input.DataSecurityMode)
	}
	if input.RuntimeEngine != "" {
		req.RuntimeEngine = compute.RuntimeEngine(input.RuntimeEngine)
	}
	if input.Kind != "" {
		req.Kind = compute.Kind(input.Kind)
	}
	if input.AzureAvailability != "" {
		req.AzureAttributes = &compute.AzureAttributes{
			Availability: compute.AzureAvailability(input.AzureAvailability),
		}
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

func (h *ClusterHandler) Set(ctx context.Context, desired ClusterState) (ClusterState, error) {
	if err := requireAtLeastOne("cluster_id or cluster_name", desired.ClusterID, desired.ClusterName); err != nil {
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
		logInfof(MsgUpdate, "Cluster", "cluster_id="+current.ClusterID)
		// Cluster exists — edit it. Edit requires the cluster to be RUNNING or TERMINATED.
		effectiveName := desired.ClusterName
		if effectiveName == "" {
			effectiveName = current.ClusterName
		}
		effectiveSparkVersion := desired.SparkVersion
		if effectiveSparkVersion == "" {
			effectiveSparkVersion = current.SparkVersion
		}

		editReq := buildEditRequest(current.ClusterID, &desired, effectiveName, effectiveSparkVersion)

		// If the cluster is TERMINATED, Edit won't restart it — the updated
		// config applies on next start. When RUNNING, wait for restart to
		// complete before we return.
		wait, err := w.Clusters.Edit(ctx, editReq)
		if err != nil {
			return desired, fmt.Errorf("failed to update cluster: %w", err)
		}
		if current.State != "TERMINATED" {
			if _, err := wait.Get(); err != nil {
				return desired, fmt.Errorf("failed waiting for cluster restart: %w", err)
			}
		}

		// Re-fetch by ID (direct GET — strongly consistent).
		updated, err := w.Clusters.GetByClusterId(ctx, current.ClusterID)
		if err != nil {
			return desired, fmt.Errorf("failed to retrieve updated cluster: %w", err)
		}
		return clusterToState(updated), nil
	}

	logInfof(MsgCreate, "Cluster", "cluster_name="+desired.ClusterName)
	// Cluster does not exist — create it.
	if err := requireFields(
		field{"cluster_name", desired.ClusterName},
		field{"spark_version", desired.SparkVersion},
	); err != nil {
		return desired, err
	}

	createReq := buildCreateRequest(&desired)
	wait, err := w.Clusters.Create(ctx, createReq)
	if err != nil {
		return desired, fmt.Errorf("failed to create cluster: %w", err)
	}
	created, err := wait.Get()
	if err != nil {
		return desired, fmt.Errorf("failed waiting for cluster to start: %w", err)
	}
	return clusterToState(created), nil
}

// projectClusterCreate returns the state Set's create path would produce.
// Server-computed fields (cluster_id, state, state_message) are unknown
// before creation and stay empty.
func projectClusterCreate(desired *ClusterState) ClusterState {
	projected := *desired
	projected.ClusterID = ""
	projected.State = ""
	projected.StateMessage = ""
	projected.SetExist(true)
	return projected
}

// projectClusterUpdate mirrors buildEditRequest: non-zero desired values win
// (the SDK omits zero values from the edit request), read-only fields carry
// over from the current state, and num_workers follows the autoscale branch.
func projectClusterUpdate(desired, current *ClusterState) ClusterState {
	projected := *current
	if desired.ClusterName != "" {
		projected.ClusterName = desired.ClusterName
	}
	if desired.SparkVersion != "" {
		projected.SparkVersion = desired.SparkVersion
	}
	if desired.NodeTypeID != "" {
		projected.NodeTypeID = desired.NodeTypeID
	}
	if desired.DriverNodeTypeID != "" {
		projected.DriverNodeTypeID = desired.DriverNodeTypeID
	}
	if desired.DataSecurityMode != "" {
		projected.DataSecurityMode = desired.DataSecurityMode
	}
	if desired.SingleUserName != "" {
		projected.SingleUserName = desired.SingleUserName
	}
	if desired.PolicyID != "" {
		projected.PolicyID = desired.PolicyID
	}
	if desired.InstancePoolID != "" {
		projected.InstancePoolID = desired.InstancePoolID
	}
	if desired.DriverInstancePoolID != "" {
		projected.DriverInstancePoolID = desired.DriverInstancePoolID
	}
	if desired.RuntimeEngine != "" {
		projected.RuntimeEngine = desired.RuntimeEngine
	}
	if desired.Kind != "" {
		projected.Kind = desired.Kind
	}
	if desired.AzureAvailability != "" {
		projected.AzureAvailability = desired.AzureAvailability
	}
	if desired.IsSingleNode {
		projected.IsSingleNode = true
	}
	if desired.AutoterminationMinutes > 0 {
		projected.AutoterminationMinutes = desired.AutoterminationMinutes
	}
	if desired.EnableElasticDisk {
		projected.EnableElasticDisk = true
	}
	if desired.AutoscaleMinWorkers > 0 || desired.AutoscaleMaxWorkers > 0 {
		projected.AutoscaleMinWorkers = desired.AutoscaleMinWorkers
		projected.AutoscaleMaxWorkers = desired.AutoscaleMaxWorkers
		// num_workers is server-managed under autoscale; keep current.
	} else {
		// buildEditRequest force-sends NumWorkers (0 included).
		projected.NumWorkers = desired.NumWorkers
		projected.AutoscaleMinWorkers = 0
		projected.AutoscaleMaxWorkers = 0
	}
	if len(desired.SparkConf) > 0 {
		projected.SparkConf = desired.SparkConf
	}
	if len(desired.CustomTags) > 0 {
		projected.CustomTags = desired.CustomTags
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the cluster.
func (h *ClusterHandler) SetWhatIf(ctx context.Context, desired ClusterState) (ClusterState, error) {
	if err := requireAtLeastOne("cluster_id or cluster_name", desired.ClusterID, desired.ClusterName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Cluster", "cluster_id="+current.ClusterID)
		return projectClusterUpdate(&desired, &current), nil
	}

	if err := requireFields(
		field{"cluster_name", desired.ClusterName},
		field{"spark_version", desired.SparkVersion},
	); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "Cluster", "cluster_name="+desired.ClusterName)
	return projectClusterCreate(&desired), nil
}

func (h *ClusterHandler) Delete(ctx context.Context, in ClusterState) error {
	if err := requireAtLeastOne("cluster_id or cluster_name", in.ClusterID, in.ClusterName); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	clusterID := in.ClusterID
	if clusterID == "" {
		logDebugf(MsgDelete, "Cluster", "cluster_name="+in.ClusterName)
		c, lookupErr := w.Clusters.GetByClusterName(ctx, in.ClusterName)
		if lookupErr != nil {
			// Already absent — deleting a missing instance succeeds.
			logInfof(MsgNotFound, "Cluster", "cluster_name="+in.ClusterName)
			return nil
		}
		clusterID = c.ClusterId
	}

	logDebugf(MsgDelete, "Cluster", "cluster_id="+clusterID)
	// Terminate the cluster first if it is running, then permanently delete
	// it so it no longer appears in the workspace.
	cluster, err := w.Clusters.GetByClusterId(ctx, clusterID)
	if err != nil {
		// Cluster already gone — nothing to do.
		return nil
	}

	if cluster.State != compute.StateTerminated && cluster.State != compute.StateTerminating {
		wait, err := w.Clusters.Delete(ctx, compute.DeleteCluster{ClusterId: clusterID})
		if err != nil {
			return fmt.Errorf("failed to terminate cluster: %w", err)
		}
		if _, err := wait.Get(); err != nil {
			return fmt.Errorf("failed waiting for cluster termination: %w", err)
		}
	} else if cluster.State == compute.StateTerminating {
		// Wait for termination to complete before permanent delete.
		_, err = w.Clusters.WaitGetClusterTerminated(ctx, clusterID, 20*time.Minute, nil)
		if err != nil {
			return fmt.Errorf("failed waiting for cluster termination: %w", err)
		}
	}

	return w.Clusters.PermanentDeleteByClusterId(ctx, clusterID)
}

func (h *ClusterHandler) Export(ctx context.Context, _ ClusterState) ([]ClusterState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Cluster")
	clusters, err := w.Clusters.ListAll(ctx, compute.ListClustersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}

	var all []ClusterState
	for i := range clusters {
		all = append(all, clusterToState(&clusters[i]))
	}

	return all, nil
}
