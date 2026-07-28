package resources

import (
	"context"
	"fmt"
	"strconv"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/settings"
)

// WorkspaceSettingState represents the state of a single workspace setting.
type WorkspaceSettingState struct {
	dsc.ExistProperty
	SettingName string `json:"setting_name" description:"Name of the workspace setting. Valid values: aibi_dashboard_embedding_access_policy, automatic_cluster_update, compliance_security_profile, dashboard_email_subscriptions, default_namespace, default_warehouse_id, disable_legacy_access, disable_legacy_dbfs, enable_export_notebook, enable_notebook_table_clipboard, enable_results_downloading, enhanced_security_monitoring, llm_proxy_partner_powered, restrict_workspace_admins, sql_results_download."`
	// value is always serialized (no omitempty) so a never-written setting
	// reads back as an explicit empty string
	Value string `json:"value" dsc:"optional" description:"The setting value. For boolean settings use 'true' or 'false'. For string settings use the string value. For enum settings use the enum constant (e.g. ALLOW_ALL, RESTRICT_TOKENS_AND_JOB_RUN_AS, ALLOW_APPROVED_DOMAINS)."`
	Etag        string `json:"etag,omitempty" description:"Etag for optimistic concurrency control. Populated on read, used for updates. (read-only)"`
}

func workspaceSettingConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/WorkspaceSetting",
		Version:     "0.1.0",
		Description: "Manage Databricks workspace-level settings.",
		Tags:        []string{"databricks", "settings", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks workspace-level settings.",
			AllowAdditionalProperties: true,
		},
	}
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

// settingDefaultState returns the state of a setting that has never been
// written on the workspace. The setting still exists (settings cannot be
// absent, only unset), so _exist is true with an empty value and no etag.
func settingDefaultState(name string) WorkspaceSettingState {
	state := WorkspaceSettingState{SettingName: name}
	state.SetExist(true)
	return state
}

// allSettingNames returns the list of supported setting names for export.
func allSettingNames() []string {
	names := make([]string, 0, len(settingRegistry))
	for name := range settingRegistry {
		names = append(names, name)
	}
	return names
}

func (h *WorkspaceSettingHandler) Get(ctx context.Context, in WorkspaceSettingState) (WorkspaceSettingState, error) {
	if err := requireFields(field{"setting_name", in.SettingName}); err != nil {
		return in, err
	}

	def, ok := settingRegistry[in.SettingName]
	if !ok {
		return in, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "unsupported setting_name %q", in.SettingName)
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "WorkspaceSetting", "setting_name="+in.SettingName)
	value, etag, err := def.get(ctx, w)
	if err != nil {
		if apierr.IsMissing(err) {
			// Settings that were never written (e.g. default_namespace on a
			// fresh workspace) return 404 from the settings API. The setting
			// still exists at its server-side default.
			logInfof(MsgSettingUnset, "WorkspaceSetting", "setting_name="+in.SettingName)
			return settingDefaultState(in.SettingName), nil
		}
		// Any other error (e.g. permission denied) is a real error.
		return in, err
	}

	state := WorkspaceSettingState{SettingName: in.SettingName, Value: value, Etag: etag}
	state.SetExist(true)
	return state, nil
}

func (h *WorkspaceSettingHandler) Set(ctx context.Context, desired WorkspaceSettingState) (WorkspaceSettingState, error) {
	if err := requireFields(
		field{"setting_name", desired.SettingName},
		field{"value", desired.Value},
	); err != nil {
		return desired, err
	}

	def, ok := settingRegistry[desired.SettingName]
	if !ok {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "unsupported setting_name %q", desired.SettingName)
	}

	w, err := workspaceClient()
	if err != nil {
		return desired, err
	}

	// Read the current etag for optimistic concurrency.
	_, etag, err := def.get(ctx, w)
	if err != nil {
		if !apierr.IsMissing(err) {
			return desired, fmt.Errorf("failed to read current setting: %w", err)
		}
		// Never-written setting: no etag exists yet. Updates send
		// AllowMissing, so an empty etag performs the first write.
		etag = ""
	}

	logInfof(MsgUpdate, "WorkspaceSetting", "setting_name="+desired.SettingName)
	if err := def.update(ctx, w, desired.Value, etag); err != nil {
		return desired, fmt.Errorf("failed to update setting %q: %w", desired.SettingName, err)
	}

	return h.Get(ctx, desired)
}

// projectWorkspaceSettingUpdate returns the state Set would produce. The
// post-update etag cannot be predicted, so the current etag is carried over.
func projectWorkspaceSettingUpdate(desired, current *WorkspaceSettingState) WorkspaceSettingState {
	projected := WorkspaceSettingState{
		SettingName: desired.SettingName,
		Value:       desired.Value,
		Etag:        current.Etag,
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without updating the setting.
func (h *WorkspaceSettingHandler) SetWhatIf(ctx context.Context, desired WorkspaceSettingState) (WorkspaceSettingState, error) {
	if err := requireFields(
		field{"setting_name", desired.SettingName},
		field{"value", desired.Value},
	); err != nil {
		return desired, err
	}
	if _, ok := settingRegistry[desired.SettingName]; !ok {
		return desired, dsc.NewExitCodeErrorf(dsc.ExitInvalidInput, "unsupported setting_name %q", desired.SettingName)
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	logInfof(MsgWhatIfUpdate, "WorkspaceSetting", "setting_name="+desired.SettingName)
	return projectWorkspaceSettingUpdate(&desired, &current), nil
}

func (h *WorkspaceSettingHandler) Delete(_ context.Context, _ WorkspaceSettingState) error {
	// Workspace settings cannot be deleted — they always exist on a workspace.
	// A Delete call is a no-op.  The user should set the value to the desired
	// default instead.
	return nil
}

func (h *WorkspaceSettingHandler) Export(ctx context.Context, _ WorkspaceSettingState) ([]WorkspaceSettingState, error) {
	var all []WorkspaceSettingState
	for _, name := range allSettingNames() {
		state, err := h.Get(ctx, WorkspaceSettingState{SettingName: name})
		if err != nil {
			// Skip settings where we don't have permission.
			logInfof(MsgSkipping, "WorkspaceSetting", name, err)
			continue
		}
		all = append(all, state)
	}
	return all, nil
}
