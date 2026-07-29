package resources

import "testing"

// TestBuildConnectionUpdate guards the options null-out hazard: the SDK
// serializes options without omitempty, so an update with an empty desired
// map would clear the connection's options on the server. No options → no
// update request at all.
func TestBuildConnectionUpdate(t *testing.T) {
	t.Run("nil when options empty", func(t *testing.T) {
		if req := buildConnectionUpdate(&ConnectionState{Name: "c", Owner: "o"}); req != nil {
			t.Errorf("buildConnectionUpdate = %+v, want nil for empty options", req)
		}
	})

	t.Run("full map resend with owner", func(t *testing.T) {
		req := buildConnectionUpdate(&ConnectionState{
			Name:    "c",
			Owner:   "o",
			Options: map[string]string{"host": "h", "port": "443"},
		})
		if req == nil {
			t.Fatal("expected update request")
		}
		if req.Name != "c" || req.Owner != "o" {
			t.Errorf("identity fields not mapped: %+v", req)
		}
		if len(req.Options) != 2 || req.Options["host"] != "h" {
			t.Errorf("Options not mapped: %+v", req.Options)
		}
	})
}
