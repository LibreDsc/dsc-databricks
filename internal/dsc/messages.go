package dsc

// Log message format strings.
// Centralizing messages enables future localization and ensures consistent wording
// across all DSC resource handlers and commands.

// Command-level messages used in CLI command handlers.
const (
	MsgCmdStarting     = "%s: starting for %s"
	MsgCmdInput        = "%s: input for %s: %s"
	MsgCmdFailed       = "%s failed for %s: %s"
	MsgCmdCompleted    = "%s: completed for %s"
	MsgSetCompleted    = "set: completed for %s, changed properties: %v"
	MsgSetNoChanges    = "set: completed for %s, no changes"
	MsgTestCompleted   = "test: completed for %s, inDesiredState=%t"
	MsgExportCompleted = "export: completed for %s, returned %d instances"
)

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
	MsgCreatingWorkspaceClient = "creating Databricks workspace client"
	MsgCreatingAccountClient   = "creating Databricks account client"
)
