package resources

import (
	"slices"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/sql"
)

func TestBuildCreateWarehouseRequest(t *testing.T) {
	input := &SqlWarehouseState{
		Name:                    "wh",
		ClusterSize:             "2X-Small",
		AutoStopMins:            15,
		MinNumClusters:          1,
		MaxNumClusters:          2,
		EnablePhoton:            true,
		EnableServerlessCompute: true,
		SpotInstancePolicy:      "COST_OPTIMIZED",
		WarehouseType:           "PRO",
		Channel:                 "CHANNEL_NAME_CURRENT",
	}

	req := buildCreateWarehouseRequest(input)

	if req.Name != "wh" || req.ClusterSize != "2X-Small" {
		t.Errorf("identity fields not mapped: %+v", req)
	}
	if req.SpotInstancePolicy != sql.SpotInstancePolicy("COST_OPTIMIZED") {
		t.Errorf("SpotInstancePolicy = %q", req.SpotInstancePolicy)
	}
	if req.WarehouseType != sql.CreateWarehouseRequestWarehouseType("PRO") {
		t.Errorf("WarehouseType = %q", req.WarehouseType)
	}
	if req.Channel == nil || req.Channel.Name != sql.ChannelName("CHANNEL_NAME_CURRENT") {
		t.Errorf("Channel = %+v", req.Channel)
	}
	if slices.Contains(req.ForceSendFields, "AutoStopMins") {
		t.Errorf("AutoStopMins must not be force-sent when non-zero")
	}
}

func TestBuildCreateWarehouseRequestZeroAutoStop(t *testing.T) {
	req := buildCreateWarehouseRequest(&SqlWarehouseState{Name: "wh", AutoStopMins: 0})

	// auto_stop_mins=0 disables auto-stop and must be sent explicitly.
	if !slices.Contains(req.ForceSendFields, "AutoStopMins") {
		t.Errorf("ForceSendFields = %v, want to contain AutoStopMins", req.ForceSendFields)
	}
	if req.Channel != nil {
		t.Errorf("Channel should be nil when unset")
	}
}

func TestBuildEditWarehouseRequest(t *testing.T) {
	input := &SqlWarehouseState{
		ClusterSize:  "Small",
		AutoStopMins: 20,
	}

	req := buildEditWarehouseRequest("wh-1", input, "kept-name")

	if req.Id != "wh-1" {
		t.Errorf("Id = %q, want wh-1", req.Id)
	}
	if req.Name != "kept-name" {
		t.Errorf("Name = %q, want kept-name (fallback)", req.Name)
	}
	if req.ClusterSize != "Small" {
		t.Errorf("ClusterSize = %q", req.ClusterSize)
	}
	if slices.Contains(req.ForceSendFields, "AutoStopMins") {
		t.Errorf("AutoStopMins must not be force-sent when non-zero")
	}
}

func TestWarehouseResponseToState(t *testing.T) {
	resp := &sql.GetWarehouseResponse{
		Id:          "wh-1",
		Name:        "wh",
		ClusterSize: "Small",
		State:       sql.StateRunning,
		NumClusters: 1,
		Channel:     &sql.Channel{Name: sql.ChannelNameChannelNameCurrent},
	}

	state := warehouseResponseToState(resp)

	if state.ID != "wh-1" || state.Name != "wh" {
		t.Errorf("identity fields not mapped: %+v", state)
	}
	if state.State != "RUNNING" || state.NumClusters != 1 {
		t.Errorf("read-only fields not mapped: %+v", state)
	}
	if state.Channel != "CHANNEL_NAME_CURRENT" {
		t.Errorf("Channel = %q", state.Channel)
	}
	if state.Exist == nil || !*state.Exist {
		t.Errorf("_exist must be explicitly true on found instances")
	}
}
