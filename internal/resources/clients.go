package resources

import (
	"fmt"
	"os"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go"
)

// workspaceClient creates a new Databricks workspace client. Authentication is
// resolved by the SDK (environment variables, .databrickscfg, or provider auth).
func workspaceClient() (*databricks.WorkspaceClient, error) {
	dsc.Logger.Debug(MsgCreatingWorkspaceClient)
	w, err := databricks.NewWorkspaceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Databricks client: %w", err)
	}
	return w, nil
}

// accountClient creates a new Databricks account client, honoring
// DATABRICKS_ACCOUNT_HOST when set.
func accountClient() (*databricks.AccountClient, error) {
	dsc.Logger.Debug(MsgCreatingAccountClient)
	var a *databricks.AccountClient
	var err error
	if host := os.Getenv("DATABRICKS_ACCOUNT_HOST"); host != "" {
		a, err = databricks.NewAccountClient(&databricks.Config{Host: host})
	} else {
		a, err = databricks.NewAccountClient()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create Databricks account client: %w", err)
	}
	return a, nil
}
