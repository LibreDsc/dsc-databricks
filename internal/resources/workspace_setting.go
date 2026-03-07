package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/settings"
)

func init() {
	dsc.RegisterResourceWithMetadata(
		"LibreDsc.Databricks/WorkspaceSetting",
		&WorkspaceSettingHandler{},
		workspaceSettingMetadata(),
	)
}

// Workspace setting property descriptions.
var workspaceSettingPropertyDescriptions = dsc.PropertyDescriptions{
	"setting_name": "Name of the workspace setting. Valid values: " +
		"aibi_dashboard_embedding_access_policy, " +
		"automatic_cluster_update, compliance_security_profile, " +
		"dashboard_email_subscriptions, default_namespace, default_warehouse_id, " +
		"disable_legacy_access, disable_legacy_dbfs, " +
		"enable_export_notebook, enable_notebook_table_clipboard, enable_results_downloading, " +
		"enhanced_security_monitoring, llm_proxy_partner_powered, " +
		"restrict_workspace_admins, sql_results_download.",
	"value": "The setting value. For boolean settings use 'true' or 'false'. " +
		"For string settings use the string value. For enum settings use the enum constant " +
		"(e.g. ALLOW_ALL, RESTRICT_TOKENS_AND_JOB_RUN_AS, ALLOW_APPROVED_DOMAINS).",
	"etag": "Etag for optimistic concurrency control. Populated on read, used for updates. Read-only.",
}

// WorkspaceSettingSchemaInput defines the desired-state fields.
type WorkspaceSettingSchemaInput struct {
	SettingName string `json:"setting_name"`
	Value       string `json:"value,omitempty"`
}

func workspaceSettingMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/WorkspaceSetting",
		Description:       "Manage Databricks workspace-level settings.",
		SchemaDescription: "Schema for managing Databricks workspace-level settings.",
		ResourceName:      "workspace_setting",
		Tags:              []string{"databricks", "settings", "workspace"},
		Descriptions:      workspaceSettingPropertyDescriptions,
		SchemaType:        reflect.TypeFor[WorkspaceSettingSchemaInput](),
		OmitTest:          true,
	})
}

// WorkspaceSettingState represents the state of a single workspace setting.
type WorkspaceSettingState struct {
	SettingName string `json:"setting_name"`
	Value       string `json:"value,omitempty"`
	Etag        string `json:"etag,omitempty"`
	Exist       bool   `json:"_exist"`
}

// WorkspaceSettingHandler handles WorkspaceSetting resource operations.
type WorkspaceSettingHandler struct{}

// settingDef describes how to get and update a single workspace setting via
// the SDK. The get function reads current value+etag; the update function
// applies a new value using the supplied etag for optimistic concurrency.
type settingDef struct {
	get    func(ctx context.Context, w *databricks.WorkspaceClient) (value string, etag string, err error)
	update func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error
}

// settingRegistry maps setting_name values to their SDK operations.
var settingRegistry = map[string]settingDef{
	"default_namespace": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.DefaultNamespace().Get(ctx, settings.GetDefaultNamespaceSettingRequest{})
			if err != nil {
				return "", "", err
			}
			return resp.Namespace.Value, resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			_, err := w.Settings.DefaultNamespace().Update(ctx, settings.UpdateDefaultNamespaceSettingRequest{
				AllowMissing: true,
				FieldMask:    "namespace.value",
				Setting: settings.DefaultNamespaceSetting{
					Etag:        etag,
					SettingName: "default",
					Namespace:   settings.StringMessage{Value: value},
				},
			})
			return err
		},
	},
	"default_warehouse_id": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.DefaultWarehouseId().Get(ctx, settings.GetDefaultWarehouseIdRequest{})
			if err != nil {
				return "", "", err
			}
			return resp.StringVal.Value, resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			_, err := w.Settings.DefaultWarehouseId().Update(ctx, settings.UpdateDefaultWarehouseIdRequest{
				AllowMissing: true,
				FieldMask:    "string_val.value",
				Setting: settings.DefaultWarehouseId{
					Etag:        etag,
					SettingName: "default",
					StringVal:   settings.StringMessage{Value: value},
				},
			})
			return err
		},
	},
	"restrict_workspace_admins": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.RestrictWorkspaceAdmins().Get(ctx, settings.GetRestrictWorkspaceAdminsSettingRequest{})
			if err != nil {
				return "", "", err
			}
			return string(resp.RestrictWorkspaceAdmins.Status), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			_, err := w.Settings.RestrictWorkspaceAdmins().Update(ctx, settings.UpdateRestrictWorkspaceAdminsSettingRequest{
				AllowMissing: true,
				FieldMask:    "restrict_workspace_admins.status",
				Setting: settings.RestrictWorkspaceAdminsSetting{
					Etag:        etag,
					SettingName: "default",
					RestrictWorkspaceAdmins: settings.RestrictWorkspaceAdminsMessage{
						Status: settings.RestrictWorkspaceAdminsMessageStatus(value),
					},
				},
			})
			return err
		},
	},
	"enhanced_security_monitoring": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.EnhancedSecurityMonitoring().Get(ctx, settings.GetEnhancedSecurityMonitoringSettingRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.EnhancedSecurityMonitoringWorkspace.IsEnabled), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("enhanced_security_monitoring value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.EnhancedSecurityMonitoring().Update(ctx, settings.UpdateEnhancedSecurityMonitoringSettingRequest{
				AllowMissing: true,
				FieldMask:    "enhanced_security_monitoring_workspace.is_enabled",
				Setting: settings.EnhancedSecurityMonitoringSetting{
					Etag:        etag,
					SettingName: "default",
					EnhancedSecurityMonitoringWorkspace: settings.EnhancedSecurityMonitoring{
						IsEnabled:       b,
						ForceSendFields: []string{"IsEnabled"},
					},
				},
			})
			return err
		},
	},
	"compliance_security_profile": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.ComplianceSecurityProfile().Get(ctx, settings.GetComplianceSecurityProfileSettingRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.ComplianceSecurityProfileWorkspace.IsEnabled), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("compliance_security_profile value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.ComplianceSecurityProfile().Update(ctx, settings.UpdateComplianceSecurityProfileSettingRequest{
				AllowMissing: true,
				FieldMask:    "compliance_security_profile_workspace.is_enabled",
				Setting: settings.ComplianceSecurityProfileSetting{
					Etag:        etag,
					SettingName: "default",
					ComplianceSecurityProfileWorkspace: settings.ComplianceSecurityProfile{
						IsEnabled:       b,
						ForceSendFields: []string{"IsEnabled"},
					},
				},
			})
			return err
		},
	},
	"automatic_cluster_update": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.AutomaticClusterUpdate().Get(ctx, settings.GetAutomaticClusterUpdateSettingRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.AutomaticClusterUpdateWorkspace.Enabled), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("automatic_cluster_update value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.AutomaticClusterUpdate().Update(ctx, settings.UpdateAutomaticClusterUpdateSettingRequest{
				AllowMissing: true,
				FieldMask:    "automatic_cluster_update_workspace.enabled",
				Setting: settings.AutomaticClusterUpdateSetting{
					Etag:        etag,
					SettingName: "default",
					AutomaticClusterUpdateWorkspace: settings.ClusterAutoRestartMessage{
						Enabled:         b,
						ForceSendFields: []string{"Enabled"},
					},
				},
			})
			return err
		},
	},
	"dashboard_email_subscriptions": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.DashboardEmailSubscriptions().Get(ctx, settings.GetDashboardEmailSubscriptionsRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.BooleanVal.Value), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("dashboard_email_subscriptions value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.DashboardEmailSubscriptions().Update(ctx, settings.UpdateDashboardEmailSubscriptionsRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.DashboardEmailSubscriptions{
					Etag:        etag,
					SettingName: "default",
					BooleanVal: settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"disable_legacy_access": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.DisableLegacyAccess().Get(ctx, settings.GetDisableLegacyAccessRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.DisableLegacyAccess.Value), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("disable_legacy_access value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.DisableLegacyAccess().Update(ctx, settings.UpdateDisableLegacyAccessRequest{
				AllowMissing: true,
				FieldMask:    "disable_legacy_access.value",
				Setting: settings.DisableLegacyAccess{
					Etag:        etag,
					SettingName: "default",
					DisableLegacyAccess: settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"disable_legacy_dbfs": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.DisableLegacyDbfs().Get(ctx, settings.GetDisableLegacyDbfsRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.DisableLegacyDbfs.Value), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("disable_legacy_dbfs value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.DisableLegacyDbfs().Update(ctx, settings.UpdateDisableLegacyDbfsRequest{
				AllowMissing: true,
				FieldMask:    "disable_legacy_dbfs.value",
				Setting: settings.DisableLegacyDbfs{
					Etag:        etag,
					SettingName: "default",
					DisableLegacyDbfs: settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"llm_proxy_partner_powered": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.LlmProxyPartnerPoweredWorkspace().Get(ctx, settings.GetLlmProxyPartnerPoweredWorkspaceRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.BooleanVal.Value), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("llm_proxy_partner_powered value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.LlmProxyPartnerPoweredWorkspace().Update(ctx, settings.UpdateLlmProxyPartnerPoweredWorkspaceRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.LlmProxyPartnerPoweredWorkspace{
					Etag:        etag,
					SettingName: "default",
					BooleanVal: settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"sql_results_download": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.SqlResultsDownload().Get(ctx, settings.GetSqlResultsDownloadRequest{})
			if err != nil {
				return "", "", err
			}
			return strconv.FormatBool(resp.BooleanVal.Value), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("sql_results_download value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.SqlResultsDownload().Update(ctx, settings.UpdateSqlResultsDownloadRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.SqlResultsDownload{
					Etag:        etag,
					SettingName: "default",
					BooleanVal: settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"aibi_dashboard_embedding_access_policy": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.AibiDashboardEmbeddingAccessPolicy().Get(ctx, settings.GetAibiDashboardEmbeddingAccessPolicySettingRequest{})
			if err != nil {
				return "", "", err
			}
			return string(resp.AibiDashboardEmbeddingAccessPolicy.AccessPolicyType), resp.Etag, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, etag string) error {
			_, err := w.Settings.AibiDashboardEmbeddingAccessPolicy().Update(ctx, settings.UpdateAibiDashboardEmbeddingAccessPolicySettingRequest{
				AllowMissing: true,
				FieldMask:    "aibi_dashboard_embedding_access_policy.access_policy_type",
				Setting: settings.AibiDashboardEmbeddingAccessPolicySetting{
					Etag:        etag,
					SettingName: "default",
					AibiDashboardEmbeddingAccessPolicy: settings.AibiDashboardEmbeddingAccessPolicy{
						AccessPolicyType: settings.AibiDashboardEmbeddingAccessPolicyAccessPolicyType(value),
					},
				},
			})
			return err
		},
	},
	"enable_export_notebook": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.EnableExportNotebook().GetEnableExportNotebook(ctx)
			if err != nil {
				return "", "", err
			}
			if resp.BooleanVal != nil {
				return strconv.FormatBool(resp.BooleanVal.Value), resp.SettingName, nil
			}
			return "", resp.SettingName, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, _ string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("enable_export_notebook value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.EnableExportNotebook().PatchEnableExportNotebook(ctx, settings.UpdateEnableExportNotebookRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.EnableExportNotebook{
					SettingName: "default",
					BooleanVal: &settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"enable_notebook_table_clipboard": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.EnableNotebookTableClipboard().GetEnableNotebookTableClipboard(ctx)
			if err != nil {
				return "", "", err
			}
			if resp.BooleanVal != nil {
				return strconv.FormatBool(resp.BooleanVal.Value), resp.SettingName, nil
			}
			return "", resp.SettingName, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, _ string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("enable_notebook_table_clipboard value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.EnableNotebookTableClipboard().PatchEnableNotebookTableClipboard(ctx, settings.UpdateEnableNotebookTableClipboardRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.EnableNotebookTableClipboard{
					SettingName: "default",
					BooleanVal: &settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
	"enable_results_downloading": {
		get: func(ctx context.Context, w *databricks.WorkspaceClient) (string, string, error) {
			resp, err := w.Settings.EnableResultsDownloading().GetEnableResultsDownloading(ctx)
			if err != nil {
				return "", "", err
			}
			if resp.BooleanVal != nil {
				return strconv.FormatBool(resp.BooleanVal.Value), resp.SettingName, nil
			}
			return "", resp.SettingName, nil
		},
		update: func(ctx context.Context, w *databricks.WorkspaceClient, value, _ string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("enable_results_downloading value must be 'true' or 'false': %w", err)
			}
			_, err = w.Settings.EnableResultsDownloading().PatchEnableResultsDownloading(ctx, settings.UpdateEnableResultsDownloadingRequest{
				AllowMissing: true,
				FieldMask:    "boolean_val.value",
				Setting: settings.EnableResultsDownloading{
					SettingName: "default",
					BooleanVal: &settings.BooleanMessage{
						Value:           b,
						ForceSendFields: []string{"Value"},
					},
				},
			})
			return err
		},
	},
}

// allSettingNames returns the list of supported setting names for export.
func allSettingNames() []string {
	names := make([]string, 0, len(settingRegistry))
	for name := range settingRegistry {
		names = append(names, name)
	}
	return names
}

func (h *WorkspaceSettingHandler) getCurrentState(ctx dsc.ResourceContext, settingName string) (WorkspaceSettingState, error) {
	def, ok := settingRegistry[settingName]
	if !ok {
		return WorkspaceSettingState{
			SettingName: settingName,
			Exist:       false,
		}, fmt.Errorf("unsupported setting_name %q", settingName)
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return WorkspaceSettingState{SettingName: settingName, Exist: false}, err
	}

	value, etag, err := def.get(cmdCtx, w)
	if err != nil {
		// Settings always exist on a workspace; an error here is a real error
		// (e.g. permission denied), not a not-found.
		return WorkspaceSettingState{SettingName: settingName, Exist: false}, err
	}

	return WorkspaceSettingState{
		SettingName: settingName,
		Value:       value,
		Etag:        etag,
		Exist:       true,
	}, nil
}

func (h *WorkspaceSettingHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[WorkspaceSettingSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "setting_name", Value: req.SettingName}); err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, req.SettingName)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *WorkspaceSettingHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[WorkspaceSettingSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(
		dsc.RequiredField{Name: "setting_name", Value: schemaInput.SettingName},
		dsc.RequiredField{Name: "value", Value: schemaInput.Value},
	); err != nil {
		return nil, err
	}

	def, ok := settingRegistry[schemaInput.SettingName]
	if !ok {
		return nil, fmt.Errorf("unsupported setting_name %q", schemaInput.SettingName)
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	// Read current value and etag.
	currentValue, etag, getErr := def.get(cmdCtx, w)
	beforeState := WorkspaceSettingState{
		SettingName: schemaInput.SettingName,
		Value:       currentValue,
		Etag:        etag,
		Exist:       getErr == nil,
	}

	if getErr != nil {
		return nil, fmt.Errorf("failed to read current setting: %w", getErr)
	}

	// Update the setting value using the etag for optimistic concurrency.
	if err := def.update(cmdCtx, w, schemaInput.Value, etag); err != nil {
		return nil, fmt.Errorf("failed to update setting %q: %w", schemaInput.SettingName, err)
	}

	// Re-read to capture the after state.
	afterValue, afterEtag, err := def.get(cmdCtx, w)
	if err != nil {
		return nil, fmt.Errorf("failed to read updated setting: %w", err)
	}

	afterState := WorkspaceSettingState{
		SettingName: schemaInput.SettingName,
		Value:       afterValue,
		Etag:        afterEtag,
		Exist:       true,
	}

	changedProps := dsc.CompareAllStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *WorkspaceSettingHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[WorkspaceSettingSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "setting_name", Value: schemaInput.SettingName}); err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, schemaInput.SettingName)
	if err != nil {
		return nil, err
	}

	desiredState := WorkspaceSettingState{
		SettingName: schemaInput.SettingName,
		Value:       schemaInput.Value,
		Exist:       true,
	}

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *WorkspaceSettingHandler) Delete(_ dsc.ResourceContext, _ json.RawMessage) error {
	// Workspace settings cannot be deleted — they always exist on a workspace.
	// A Delete call is a no-op.  The user should set the value to the desired
	// default instead.
	return nil
}

func (h *WorkspaceSettingHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	var all []any
	for _, name := range allSettingNames() {
		state, err := h.getCurrentState(ctx, name)
		if err != nil {
			// Skip settings where we don't have permission.
			dsc.Logger.Infof("skipping setting %s: %s", name, err)
			continue
		}
		all = append(all, state)
	}
	return all, nil
}
