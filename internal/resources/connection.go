package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// ConnectionState represents the full state of a Unity Catalog connection
// (Lakehouse Federation).
type ConnectionState struct {
	dsc.ExistProperty
	Options        map[string]string `json:"options,omitempty" description:"Connection options as key-value pairs (e.g. host, port, bearer_token). Required when creating; updates replace the full map."`
	Properties     map[string]string `json:"properties,omitempty" description:"A map of key-value properties attached to the connection. (create-only)"`
	Name           string            `json:"name" description:"Name of the connection."`
	ConnectionType string            `json:"connection_type,omitempty" description:"The type of the connection. Required when creating. (create-only)" enum:"BIGQUERY,CONFLUENCE,DATABRICKS,DYNAMICS365,GA4_RAW_DATA,GITHUB,GLUE,HIVE_METASTORE,HTTP,HUBSPOT,JDBC,META_MARKETING,MYSQL,ORACLE,OUTLOOK,POSTGRESQL,POWER_BI,REDSHIFT,SALESFORCE,SALESFORCE_DATA_CLOUD,SERVICENOW,SMARTSHEET,SNOWFLAKE,SQLDW,SQLSERVER,TERADATA,UNKNOWN_CONNECTION_TYPE,WORKDAY_RAAS,ZENDESK"`
	Comment        string            `json:"comment,omitempty" description:"User-provided free-form text description. (create-only)"`
	Owner          string            `json:"owner,omitempty" description:"Username of the current owner of the connection."`
	ConnectionID   string            `json:"connection_id,omitempty" description:"Unique identifier of the connection. (read-only)"`
	URL            string            `json:"url,omitempty" description:"URL of the remote data source, extracted from the options. (read-only)"`
	MetastoreID    string            `json:"metastore_id,omitempty" description:"Unique identifier of the parent metastore. (read-only)"`
	ReadOnly       bool              `json:"read_only,omitempty" description:"Whether the connection is read-only. (create-only)"`
}

func connectionConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Connection",
		Version:     "0.1.0",
		Description: "Manage Unity Catalog connections for Lakehouse Federation in a Databricks workspace.",
		Tags:        []string{"databricks", "connection", "unitycatalog", "federation"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Unity Catalog connections.",
			AllowAdditionalProperties: true,
		},
	}
}

// ConnectionHandler handles Connection resource operations.
type ConnectionHandler struct{}

func connectionInfoToState(c *catalog.ConnectionInfo) ConnectionState {
	state := ConnectionState{
		Name:           c.Name,
		ConnectionType: string(c.ConnectionType),
		Comment:        c.Comment,
		Owner:          c.Owner,
		ConnectionID:   c.ConnectionId,
		URL:            c.Url,
		MetastoreID:    c.MetastoreId,
		ReadOnly:       c.ReadOnly,
	}
	if len(c.Options) > 0 {
		state.Options = c.Options
	}
	if len(c.Properties) > 0 {
		state.Properties = c.Properties
	}
	state.SetExist(true)
	return state
}

// buildConnectionUpdate returns the update request for the desired state, or
// nil when no update may be sent: the SDK serializes options without
// omitempty, so an update with an empty desired map would null out the
// connection's options on the server.
func buildConnectionUpdate(desired *ConnectionState) *catalog.UpdateConnection {
	if len(desired.Options) == 0 {
		return nil
	}
	return &catalog.UpdateConnection{
		Name:    desired.Name,
		Options: desired.Options,
		Owner:   desired.Owner,
	}
}

func (h *ConnectionHandler) Get(ctx context.Context, in ConnectionState) (ConnectionState, error) {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "Connection", "name="+in.Name)
	c, err := w.Connections.GetByName(ctx, in.Name)
	if err != nil {
		logInfof(MsgNotFound, "Connection", "name="+in.Name)
		return dsc.NotFound(ConnectionState{Name: in.Name}, "Connection", "name="+in.Name)
	}

	return connectionInfoToState(c), nil
}

func (h *ConnectionHandler) Set(ctx context.Context, desired ConnectionState) (ConnectionState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		req := buildConnectionUpdate(&desired)
		if req == nil {
			// Nothing updatable was specified; sending an empty options
			// map would clear the connection's options.
			logDebugf(MsgAlreadyExists, "Connection", "name="+desired.Name)
			return current, nil
		}
		logInfof(MsgUpdate, "Connection", "name="+desired.Name)
		updated, err := w.Connections.Update(ctx, *req)
		if err != nil {
			return desired, fmt.Errorf("failed to update connection: %w", err)
		}
		return connectionInfoToState(updated), nil
	}

	if err := requireFields(field{"connection_type", desired.ConnectionType}); err != nil {
		return desired, err
	}
	if len(desired.Options) == 0 {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "missing required field(s): options")
	}

	logInfof(MsgCreate, "Connection", "name="+desired.Name)
	if _, err := w.Connections.Create(ctx, catalog.CreateConnection{
		Name:           desired.Name,
		ConnectionType: catalog.ConnectionType(desired.ConnectionType),
		Options:        desired.Options,
		Comment:        desired.Comment,
		Properties:     desired.Properties,
		ReadOnly:       desired.ReadOnly,
	}); err != nil {
		return desired, fmt.Errorf("failed to create connection: %w", err)
	}

	// Owner is not part of the create API; apply with a follow-up update
	// (options ride along — the SDK always serializes them).
	if desired.Owner != "" {
		if _, err := w.Connections.Update(ctx, catalog.UpdateConnection{
			Name:    desired.Name,
			Options: desired.Options,
			Owner:   desired.Owner,
		}); err != nil {
			return desired, fmt.Errorf("failed to apply post-create connection settings: %w", err)
		}
	}

	return h.Get(ctx, desired)
}

// projectConnectionCreate returns the state Set's create path would produce.
// connection_id, url, and metastore_id are computed by the server and stay
// empty. Owner is applied by the chained post-create update.
func projectConnectionCreate(desired *ConnectionState) ConnectionState {
	projected := ConnectionState{
		Name:           desired.Name,
		ConnectionType: desired.ConnectionType,
		Comment:        desired.Comment,
		Owner:          desired.Owner,
		ReadOnly:       desired.ReadOnly,
		Options:        desired.Options,
		Properties:     desired.Properties,
	}
	projected.SetExist(true)
	return projected
}

// projectConnectionUpdate mirrors buildConnectionUpdate: options and owner
// are the only updatable fields, and no update happens at all when the
// desired options map is empty.
func projectConnectionUpdate(desired, current *ConnectionState) ConnectionState {
	projected := *current
	if len(desired.Options) > 0 {
		projected.Options = desired.Options
		if desired.Owner != "" {
			projected.Owner = desired.Owner
		}
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the
// connection.
func (h *ConnectionHandler) SetWhatIf(ctx context.Context, desired ConnectionState) (ConnectionState, error) {
	if err := requireFields(field{"name", desired.Name}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "Connection", "name="+desired.Name)
		return projectConnectionUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"connection_type", desired.ConnectionType}); err != nil {
		return desired, err
	}
	if len(desired.Options) == 0 {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "missing required field(s): options")
	}
	logInfof(MsgWhatIfCreate, "Connection", "name="+desired.Name)
	return projectConnectionCreate(&desired), nil
}

func (h *ConnectionHandler) Delete(ctx context.Context, in ConnectionState) error {
	if err := requireFields(field{"name", in.Name}); err != nil {
		return err
	}

	current, err := h.Get(ctx, in)
	if err != nil {
		return err
	}
	if !current.ShouldExist() {
		return nil
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	logDebugf(MsgDelete, "Connection", "name="+in.Name)
	return w.Connections.DeleteByName(ctx, in.Name)
}

func (h *ConnectionHandler) Export(ctx context.Context, _ ConnectionState) ([]ConnectionState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Connection")
	connections, err := w.Connections.ListAll(ctx, catalog.ListConnectionsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list connections: %w", err)
	}

	var all []ConnectionState
	for i := range connections {
		all = append(all, connectionInfoToState(&connections[i]))
	}

	return all, nil
}
