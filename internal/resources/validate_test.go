package resources

import (
	"errors"
	"strings"
	"testing"

	dsc "github.com/LibreDsc/dsc-go-rdk"
)

func TestRequireFields(t *testing.T) {
	tests := []struct {
		name        string
		fields      []field
		wantErr     bool
		wantMessage string
	}{
		{
			name:    "all present",
			fields:  []field{{"scope", "a"}, {"key", "b"}},
			wantErr: false,
		},
		{
			name:        "one missing",
			fields:      []field{{"scope", ""}, {"key", "b"}},
			wantErr:     true,
			wantMessage: "scope",
		},
		{
			name:        "multiple missing listed together",
			fields:      []field{{"scope", ""}, {"key", ""}},
			wantErr:     true,
			wantMessage: "scope, key",
		},
		{
			name:    "no fields",
			fields:  nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireFields(tt.fields...)
			if tt.wantErr != (err != nil) {
				t.Fatalf("requireFields() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				return
			}
			var exitErr *dsc.ExitCodeError
			if !errors.As(err, &exitErr) {
				t.Fatalf("expected *dsc.ExitCodeError, got %T", err)
			}
			if exitErr.Code != dsc.ExitInvalidInput {
				t.Errorf("exit code = %d, want %d", exitErr.Code, dsc.ExitInvalidInput)
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantMessage)
			}
		})
	}
}

func TestRequireAtLeastOne(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		wantErr bool
	}{
		{name: "first set", values: []string{"a", ""}, wantErr: false},
		{name: "second set", values: []string{"", "b"}, wantErr: false},
		{name: "all empty", values: []string{"", ""}, wantErr: true},
		{name: "no values", values: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireAtLeastOne("id or name", tt.values...)
			if tt.wantErr != (err != nil) {
				t.Fatalf("requireAtLeastOne() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				return
			}
			var exitErr *dsc.ExitCodeError
			if !errors.As(err, &exitErr) {
				t.Fatalf("expected *dsc.ExitCodeError, got %T", err)
			}
			if exitErr.Code != dsc.ExitInvalidInput {
				t.Errorf("exit code = %d, want %d", exitErr.Code, dsc.ExitInvalidInput)
			}
			if !strings.Contains(err.Error(), "id or name") {
				t.Errorf("error %q does not contain field description", err.Error())
			}
		})
	}
}
