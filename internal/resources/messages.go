package resources

// Log message format strings.
// Centralizing messages enables future localization and ensures consistent wording
// across all DSC resource handlers.

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
)

// Client initialization messages.
const (
	MsgCreatingWorkspaceClient = "Creating Databricks workspace client"
	MsgCreatingAccountClient   = "Creating Databricks account client"
)
