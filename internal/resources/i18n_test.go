package resources

import (
	"testing"

	"golang.org/x/text/language"
)

func fakeGetenv(env map[string]string) func(string) string {
	return func(name string) string { return env[name] }
}

func TestLanguageFromEnv(t *testing.T) {
	tests := []struct {
		want language.Tag
		env  map[string]string
		name string
	}{
		{
			name: "empty environment defaults to English",
			env:  map[string]string{},
			want: language.English,
		},
		{
			name: "explicit English",
			env:  map[string]string{"DSC_DATABRICKS_LANG": "en"},
			want: language.English,
		},
		{
			name: "POSIX locale value is normalized",
			env:  map[string]string{"LANG": "en_US.UTF-8"},
			want: language.English,
		},
		{
			name: "unsupported language falls back to English",
			env:  map[string]string{"DSC_DATABRICKS_LANG": "fr"},
			want: language.English,
		},
		{
			name: "garbage value falls through to next variable",
			env:  map[string]string{"DSC_DATABRICKS_LANG": "not a locale!!", "LANG": "en"},
			want: language.English,
		},
		{
			name: "DSC_DATABRICKS_LANG takes precedence over LC_ALL and LANG",
			env:  map[string]string{"DSC_DATABRICKS_LANG": "en", "LC_ALL": "fr_FR", "LANG": "de_DE"},
			want: language.English,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := languageFromEnv(fakeGetenv(tt.env))
			if got != tt.want {
				t.Errorf("languageFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalizeEnglishPassThrough(t *testing.T) {
	got := localize(MsgCreate, "Catalog", "name=x")
	want := "Catalog: creating name=x"
	if got != want {
		t.Errorf("localize(MsgCreate) = %q, want %q", got, want)
	}
}

// TestCatalogCompleteness guards that every Msg* constant is registered as a
// localization key — a new message added to messages.go must also be added
// to localizedMessages in i18n.go.
func TestCatalogCompleteness(t *testing.T) {
	allMessages := []string{
		MsgLookup, MsgNotFound, MsgUpdate, MsgCreate, MsgDelete, MsgListAll,
		MsgAlreadyExists, MsgPut, MsgSkipping,
		MsgWhatIfCreate, MsgWhatIfUpdate, MsgWhatIfPut,
		MsgCreatingWorkspaceClient, MsgCreatingAccountClient,
	}
	for _, msg := range allMessages {
		if _, ok := localizedMessages[msg]; !ok {
			t.Errorf("message %q missing from localizedMessages", msg)
		}
	}
	if len(localizedMessages) != len(allMessages) {
		t.Errorf("localizedMessages has %d entries, test knows %d — keep both in sync",
			len(localizedMessages), len(allMessages))
	}
}
