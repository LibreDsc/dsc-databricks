package resources

import "testing"

func TestRepoAliasPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/Repos/user@example.com/my-repo", "/Workspace/Repos/user@example.com/my-repo"},
		{"/Workspace/Repos/user@example.com/my-repo", "/Repos/user@example.com/my-repo"},
		{"/Workspace/Users/user@example.com/repo", "/Users/user@example.com/repo"},
	}

	for _, tt := range tests {
		if got := repoAliasPath(tt.path); got != tt.want {
			t.Errorf("repoAliasPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}
