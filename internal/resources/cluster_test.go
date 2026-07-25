package resources

import (
	"slices"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/compute"
)

func TestBuildCreateRequestFixedSize(t *testing.T) {
	input := &ClusterState{
		ClusterName:            "test",
		SparkVersion:           "15.4.x-scala2.12",
		NodeTypeID:             "Standard_D4ds_v5",
		NumWorkers:             2,
		AutoterminationMinutes: 30,
		DataSecurityMode:       "SINGLE_USER",
		RuntimeEngine:          "PHOTON",
		SparkConf:              map[string]string{"spark.speculation": "true"},
	}

	req := buildCreateRequest(input)

	if req.ClusterName != "test" || req.SparkVersion != "15.4.x-scala2.12" {
		t.Errorf("identity fields not mapped: %+v", req)
	}
	if req.NumWorkers != 2 {
		t.Errorf("NumWorkers = %d, want 2", req.NumWorkers)
	}
	if req.Autoscale != nil {
		t.Errorf("Autoscale should be nil for fixed-size cluster")
	}
	if !slices.Contains(req.ForceSendFields, "NumWorkers") {
		t.Errorf("ForceSendFields = %v, want to contain NumWorkers", req.ForceSendFields)
	}
	if req.DataSecurityMode != compute.DataSecurityMode("SINGLE_USER") {
		t.Errorf("DataSecurityMode = %q", req.DataSecurityMode)
	}
	if req.RuntimeEngine != compute.RuntimeEngine("PHOTON") {
		t.Errorf("RuntimeEngine = %q", req.RuntimeEngine)
	}
	if req.SparkConf["spark.speculation"] != "true" {
		t.Errorf("SparkConf not mapped")
	}
}

func TestBuildCreateRequestAutoscale(t *testing.T) {
	input := &ClusterState{
		ClusterName:         "test",
		SparkVersion:        "15.4.x-scala2.12",
		AutoscaleMinWorkers: 1,
		AutoscaleMaxWorkers: 4,
	}

	req := buildCreateRequest(input)

	if req.Autoscale == nil {
		t.Fatalf("Autoscale is nil, want min=1 max=4")
	}
	if req.Autoscale.MinWorkers != 1 || req.Autoscale.MaxWorkers != 4 {
		t.Errorf("Autoscale = %+v, want min=1 max=4", req.Autoscale)
	}
	if slices.Contains(req.ForceSendFields, "NumWorkers") {
		t.Errorf("NumWorkers must not be force-sent when autoscaling")
	}
}

func TestBuildEditRequestUsesEffectiveValues(t *testing.T) {
	input := &ClusterState{
		NodeTypeID: "Standard_D4ds_v5",
		NumWorkers: 3,
	}

	req := buildEditRequest("cluster-1", input, "existing-name", "14.3.x-scala2.12")

	if req.ClusterId != "cluster-1" {
		t.Errorf("ClusterId = %q, want cluster-1", req.ClusterId)
	}
	if req.ClusterName != "existing-name" {
		t.Errorf("ClusterName = %q, want existing-name (fallback)", req.ClusterName)
	}
	if req.SparkVersion != "14.3.x-scala2.12" {
		t.Errorf("SparkVersion = %q, want fallback value", req.SparkVersion)
	}
	if req.NumWorkers != 3 {
		t.Errorf("NumWorkers = %d, want 3", req.NumWorkers)
	}
	if !slices.Contains(req.ForceSendFields, "NumWorkers") {
		t.Errorf("ForceSendFields = %v, want to contain NumWorkers", req.ForceSendFields)
	}
}

func TestClusterToState(t *testing.T) {
	details := &compute.ClusterDetails{
		ClusterId:    "abc",
		ClusterName:  "test",
		SparkVersion: "15.4.x-scala2.12",
		State:        compute.StateRunning,
		Autoscale:    &compute.AutoScale{MinWorkers: 2, MaxWorkers: 8},
	}

	state := clusterToState(details)

	if state.ClusterID != "abc" || state.ClusterName != "test" {
		t.Errorf("identity fields not mapped: %+v", state)
	}
	if state.State != "RUNNING" {
		t.Errorf("State = %q, want RUNNING", state.State)
	}
	if state.AutoscaleMinWorkers != 2 || state.AutoscaleMaxWorkers != 8 {
		t.Errorf("autoscale not mapped: %+v", state)
	}
	if state.Exist == nil || !*state.Exist {
		t.Errorf("_exist must be explicitly true on found instances")
	}
}
