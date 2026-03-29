package dsc

import (
	"strings"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// helpTemplateWithRequiredFlags generates a help template that separates required and optional flags.
func helpTemplateWithRequiredFlags(hasRequiredFlags bool) string {
	var tmpl string
	if hasRequiredFlags {
		tmpl = `
Description:
  {{.Long}}

Usage:
  {{.UseLine}}

Required flags:
{{requiredFlags .LocalFlags | trimTrailingWhitespaces}}

Optional flags:
{{optionalFlags .LocalFlags | trimTrailingWhitespaces}}
`
	} else {
		tmpl = `
Description:
  {{.Long}}

Usage:
  {{.UseLine}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}
`
	}
	return tmpl
}

// setupHelpFuncs adds custom template functions for flag filtering.
func setupHelpFuncs(cmd *cobra.Command) {
	cobra.AddTemplateFunc("requiredFlags", func(flags *pflag.FlagSet) string {
		var b strings.Builder
		flags.VisitAll(func(f *pflag.Flag) {
			if _, ok := f.Annotations[cobra.BashCompOneRequiredFlag]; ok {
				b.WriteString(formatFlag(f))
			}
		})
		return b.String()
	})

	cobra.AddTemplateFunc("optionalFlags", func(flags *pflag.FlagSet) string {
		var b strings.Builder
		flags.VisitAll(func(f *pflag.Flag) {
			if _, ok := f.Annotations[cobra.BashCompOneRequiredFlag]; !ok {
				b.WriteString(formatFlag(f))
			}
		})
		return b.String()
	})
}

func formatFlag(f *pflag.Flag) string {
	var b strings.Builder
	if f.Shorthand != "" {
		b.WriteString("  -")
		b.WriteString(f.Shorthand)
		b.WriteString(", --")
	} else {
		b.WriteString("      --")
	}
	b.WriteString(f.Name)
	if f.Value.Type() != "bool" {
		b.WriteString(" string")
	}
	b.WriteString("\n        ")
	b.WriteString(f.Usage)
	b.WriteString("\n")
	return b.String()
}

func newGetCmd() *cobra.Command {
	var resourceType string
	var input string

	cmd := &cobra.Command{
		Use:          "get",
		Short:        "Get the current state of a resource",
		Long:         `Retrieves and returns the current state of a specified resource.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			Logger.Infof(MsgCmdStarting, "get", resourceType)
			handler, err := getResourceHandler(resourceType)
			if err != nil {
				return err
			}

			jsonInput, err := parseInput(input)
			if err != nil {
				return err
			}
			Logger.Tracef(MsgCmdInput, "get", resourceType, string(jsonInput))

			ctx := ResourceContext{Cmd: cmd}
			result, err := handler.Get(ctx, jsonInput)
			if err != nil {
				Logger.Errorf(MsgCmdFailed, "get", resourceType, err)
				return err
			}

			Logger.Debugf(MsgCmdCompleted, "get", resourceType)
			return renderJSON(ctx, result.ActualState)
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON input for the resource instance")
	_ = cmd.MarkFlagRequired("resource")
	_ = cmd.MarkFlagRequired("input")

	return cmd
}

func newSetCmd() *cobra.Command {
	var resourceType string
	var input string

	cmd := &cobra.Command{
		Use:          "set",
		Short:        "Create or update a resource to match desired state",
		Long:         `Creates a new resource or updates an existing one to match the specified desired state.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			Logger.Infof(MsgCmdStarting, "set", resourceType)
			handler, err := getResourceHandler(resourceType)
			if err != nil {
				return err
			}

			jsonInput, err := parseInput(input)
			if err != nil {
				return err
			}
			Logger.Tracef(MsgCmdInput, "set", resourceType, string(jsonInput))

			ctx := ResourceContext{Cmd: cmd}
			result, err := handler.Set(ctx, jsonInput)
			if err != nil {
				Logger.Errorf(MsgCmdFailed, "set", resourceType, err)
				return err
			}

			if result != nil {
				Logger.Debugf(MsgSetCompleted, resourceType, result.ChangedProperties)
				// DSC with stateAndDiff expects two JSON lines on stdout:
				// Line 1: after state (resource instance)
				// Line 2: changed properties array
				if err := renderJSON(ctx, result.AfterState); err != nil {
					return err
				}
				changedProps := result.ChangedProperties
				if changedProps == nil {
					changedProps = []string{}
				}
				return renderJSON(ctx, changedProps)
			}
			Logger.Debugf(MsgSetNoChanges, resourceType)
			return nil
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON input for the resource instance")
	_ = cmd.MarkFlagRequired("resource")
	_ = cmd.MarkFlagRequired("input")

	return cmd
}

func newDeleteCmd() *cobra.Command {
	var resourceType string
	var input string

	cmd := &cobra.Command{
		Use:          "delete",
		Short:        "Delete a resource",
		Long:         `Removes the specified resource.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			Logger.Infof(MsgCmdStarting, "delete", resourceType)
			handler, err := getResourceHandler(resourceType)
			if err != nil {
				return err
			}

			jsonInput, err := parseInput(input)
			if err != nil {
				return err
			}
			Logger.Tracef(MsgCmdInput, "delete", resourceType, string(jsonInput))

			ctx := ResourceContext{Cmd: cmd}
			if err := handler.Delete(ctx, jsonInput); err != nil {
				Logger.Errorf(MsgCmdFailed, "delete", resourceType, err)
				return err
			}
			Logger.Debugf(MsgCmdCompleted, "delete", resourceType)
			return nil
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON input for the resource instance")
	_ = cmd.MarkFlagRequired("resource")
	_ = cmd.MarkFlagRequired("input")

	return cmd
}

func newTestCmd() *cobra.Command {
	var resourceType string
	var input string

	cmd := &cobra.Command{
		Use:          "test",
		Short:        "Test if a resource is in the desired state",
		Long:         `Tests whether a resource matches the specified desired state and reports any differences.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			Logger.Infof(MsgCmdStarting, "test", resourceType)
			handler, err := getResourceHandler(resourceType)
			if err != nil {
				return err
			}

			jsonInput, err := parseInput(input)
			if err != nil {
				return err
			}
			Logger.Tracef(MsgCmdInput, "test", resourceType, string(jsonInput))

			ctx := ResourceContext{Cmd: cmd}
			result, err := handler.Test(ctx, jsonInput)
			if err != nil {
				Logger.Errorf(MsgCmdFailed, "test", resourceType, err)
				return err
			}

			Logger.Debugf(MsgTestCompleted, resourceType, result.InDesiredState)
			// DSC with stateAndDiff expects two JSON lines on stdout:
			// Line 1: actual state with _inDesiredState property
			// Line 2: differing properties array
			stateWithDesired, err := withInDesiredState(result.ActualState, result.InDesiredState)
			if err != nil {
				return err
			}
			return renderJSON(ctx, stateWithDesired)
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON input for the resource instance")
	_ = cmd.MarkFlagRequired("resource")
	_ = cmd.MarkFlagRequired("input")

	return cmd
}

func newExportCmd() *cobra.Command {
	var resourceType string

	cmd := &cobra.Command{
		Use:          "export",
		Short:        "Export all resources of a type",
		Long:         `Lists and exports all resources of the specified type.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			Logger.Infof(MsgCmdStarting, "export", resourceType)
			handler, err := getResourceHandler(resourceType)
			if err != nil {
				return err
			}

			ctx := ResourceContext{Cmd: cmd}
			result, err := handler.Export(ctx)
			if err != nil {
				Logger.Errorf(MsgCmdFailed, "export", resourceType, err)
				return err
			}

			Logger.Debugf(MsgExportCompleted, resourceType, len(result))
			// DSC expects one JSON object per line; it wraps each into
			// {"type": ..., "properties": ...} for the export result.
			for _, item := range result {
				if err := renderJSON(ctx, item); err != nil {
					return err
				}
			}
			return nil
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	_ = cmd.MarkFlagRequired("resource")

	return cmd
}

func newSchemaCmd() *cobra.Command {
	var resourceType string

	cmd := &cobra.Command{
		Use:          "schema",
		Short:        "Get the JSON schema for a resource type",
		Long:         `Returns the JSON schema that describes the input format for a resource type.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			metadata, err := getResourceMetadata(resourceType)
			if err != nil {
				return err
			}

			ctx := ResourceContext{Cmd: cmd}
			return renderJSON(ctx, metadata.Schema.Embedded)
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(true))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "The DSC resource type")
	_ = cmd.MarkFlagRequired("resource")

	return cmd
}

// ManifestOutput represents the full manifest output.
type ManifestOutput struct {
	Resources []ResourceManifest `json:"resources"`
}

func newManifestCmd() *cobra.Command {
	var resourceType string

	cmd := &cobra.Command{
		Use:          "manifest",
		Short:        "Get Microsoft DSC resource manifests",
		Long:         `Returns the Microsoft DSC resource manifest(s) in JSON format.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ResourceContext{Cmd: cmd}
			if resourceType != "" {
				// Return single resource manifest
				metadata, err := getResourceMetadata(resourceType)
				if err != nil {
					return err
				}
				manifest := buildManifest(resourceType, metadata)
				return renderJSON(ctx, manifest)
			}

			// Return all manifests
			var manifests []ResourceManifest
			for _, rt := range listResourceTypes() {
				metadata, err := getResourceMetadata(rt)
				if err != nil {
					continue
				}
				manifests = append(manifests, buildManifest(rt, metadata))
			}

			return renderJSON(ctx, ManifestOutput{Resources: manifests})
		},
	}

	setupHelpFuncs(cmd)
	cmd.SetHelpTemplate(helpTemplateWithRequiredFlags(false))
	cmd.Flags().StringVarP(&resourceType, "resource", "r", "", "Filter to a specific DSC resource type")

	return cmd
}
