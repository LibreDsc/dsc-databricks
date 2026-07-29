package resources

import (
	"os"
	"strings"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// supportedLanguages lists the languages with registered catalogs. English
// (index 0) is the matcher fallback. Adding a language means appending its
// tag here and registering a translation for every key in localizedMessages
// via message.SetString in newPrinter.
var supportedLanguages = []language.Tag{language.English}

// localizedMessages maps every catalog key (the Msg* constants) to its
// English format string. The key IS the English string, so an untranslated
// language falls back to readable English output.
var localizedMessages = map[string]string{
	MsgLookup:                  MsgLookup,
	MsgNotFound:                MsgNotFound,
	MsgUpdate:                  MsgUpdate,
	MsgCreate:                  MsgCreate,
	MsgDelete:                  MsgDelete,
	MsgListAll:                 MsgListAll,
	MsgAlreadyExists:           MsgAlreadyExists,
	MsgPut:                     MsgPut,
	MsgSkipping:                MsgSkipping,
	MsgSettingUnset:            MsgSettingUnset,
	MsgWhatIfCreate:            MsgWhatIfCreate,
	MsgWhatIfUpdate:            MsgWhatIfUpdate,
	MsgWhatIfPut:               MsgWhatIfPut,
	MsgCreatingWorkspaceClient: MsgCreatingWorkspaceClient,
	MsgCreatingAccountClient:   MsgCreatingAccountClient,
}

var printer = newPrinter(languageFromEnv(os.Getenv))

func newPrinter(tag language.Tag) *message.Printer {
	for key, val := range localizedMessages {
		_ = message.SetString(language.English, key, val)
	}
	return message.NewPrinter(tag)
}

// languageFromEnv resolves the log message language from the environment:
// DSC_DATABRICKS_LANG, then LC_ALL, then LANG, defaulting to English.
// POSIX-style values like "en_US.UTF-8" are normalized before parsing. The
// getenv function is injected so tests can exercise the chain without
// touching the process environment.
func languageFromEnv(getenv func(string) string) language.Tag {
	matcher := language.NewMatcher(supportedLanguages)
	for _, name := range []string{"DSC_DATABRICKS_LANG", "LC_ALL", "LANG"} {
		raw := getenv(name)
		if raw == "" {
			continue
		}
		raw = strings.ReplaceAll(strings.SplitN(raw, ".", 2)[0], "_", "-")
		tag, err := language.Parse(raw)
		if err != nil {
			continue
		}
		// Match returns a region-preserving tag (e.g. en-US); index into
		// supportedLanguages to land on the exact catalog language.
		_, idx, _ := matcher.Match(tag)
		return supportedLanguages[idx]
	}
	return language.English
}

// localize renders a catalog message in the selected language, falling back
// to the English format string itself when no translation exists.
func localize(format string, args ...any) string {
	return printer.Sprintf(format, args...)
}

func logDebug(msg string) {
	dsc.Logger.Debug(localize(msg))
}

func logDebugf(format string, args ...any) {
	dsc.Logger.Debug(localize(format, args...))
}

func logInfof(format string, args ...any) {
	dsc.Logger.Info(localize(format, args...))
}
