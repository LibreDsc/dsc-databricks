package resources

// Log message format strings.
// Each constant doubles as a localization catalog key (see i18n.go): the
// constant value is the canonical English format string, and translations are
// registered against it per language. Every new constant must also be added
// to localizedMessages in i18n.go.

// Resource-level messages used across all resource handlers.
const (
	MsgLookup        = "%s: looking up by %s"
	MsgNotFound      = "%s: not found by %s"
	MsgUpdate        = "%s: updating %s"
	MsgCreate        = "%s: creating %s"
	MsgDelete        = "%s: deleting %s"
	MsgListAll       = "%s: listing all"
	MsgAlreadyExists = "%s: %s already exists"
	MsgPut           = "%s: setting %s"
	MsgSkipping      = "%s: skipping %s: %s"
	MsgSettingUnset  = "%s: %s has never been written; using its server-side default"
)

// What-if messages: emitted by SetWhatIf instead of the mutating message.
const (
	MsgWhatIfCreate = "%s: would create %s"
	MsgWhatIfUpdate = "%s: would update %s"
	MsgWhatIfPut    = "%s: would set %s"
)

// Client initialization messages.
const (
	MsgCreatingWorkspaceClient = "Creating Databricks workspace client"
	MsgCreatingAccountClient   = "Creating Databricks account client"
)
