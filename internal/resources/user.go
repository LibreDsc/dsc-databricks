package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/User", &UserHandler{}, userMetadata())
}

// User property descriptions from SDK documentation.
var userPropertyDescriptions = dsc.PropertyDescriptions{
	"id":           "Databricks user ID. This is the unique identifier for the user.",
	"user_name":    "Email address of the Databricks user. This is used as the primary identifier.",
	"display_name": "String that represents a concatenation of given and family names.",
	"active":       "If this user is active.",
	"external_id":  "External ID is reserved for future use.",
	"emails":       "All the emails associated with the Databricks user.",
	"entitlements": "Entitlements assigned to the user.",
	"groups":       "Groups the user belongs to.",
	"roles":        "Corresponds to AWS instance profile/arn role.",
	"name":         "The name of the user (given name and family name).",
}

func userMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/User",
		Description:       "Manage Databricks workspace users",
		SchemaDescription: "Schema for managing Databricks workspace users.",
		ResourceName:      "user",
		Tags:              []string{"databricks", "user", "iam", "workspace"},
		Descriptions:      userPropertyDescriptions,
		SchemaType:        reflect.TypeOf(iam.User{}),
	})
}

// UserState represents the state of a Databricks user.
type UserState struct {
	ID          string `json:"id"`
	UserName    string `json:"user_name"`
	DisplayName string `json:"display_name,omitempty"`
	Active      bool   `json:"active"`
	Exist       bool   `json:"_exist"`
}

// UserHandler handles User resource operations.
type UserHandler struct{}

func (h *UserHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[iam.User](input)
	if err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

// getCurrentState retrieves the current state for a user from the API.
func (h *UserHandler) getCurrentState(ctx dsc.ResourceContext, req *iam.User) (UserState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return UserState{Exist: false}, err
	}

	if req.Id != "" {
		user, err := w.Users.GetById(cmdCtx, req.Id)
		if err != nil {
			return UserState{Exist: false}, nil
		}
		return userToState(user), nil
	}

	if req.UserName != "" {
		users := w.Users.List(cmdCtx, iam.ListUsersRequest{
			Filter: "userName eq \"" + req.UserName + "\"",
		})

		user, err := users.Next(cmdCtx)
		if err != nil {
			return UserState{UserName: req.UserName, Exist: false}, nil
		}
		return userToState(&user), nil
	}

	return UserState{Exist: false}, fmt.Errorf("id or user_name must be provided")
}

func (h *UserHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[iam.User](input)
	if err != nil {
		return nil, err
	}

	beforeState, _ := h.getCurrentState(ctx, &req)

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if req.Id != "" {
		if err := w.Users.Update(cmdCtx, req); err != nil {
			return nil, err
		}
	} else if req.UserName != "" {
		users := w.Users.List(cmdCtx, iam.ListUsersRequest{
			Filter: "userName eq \"" + req.UserName + "\"",
		})

		existingUser, err := users.Next(cmdCtx)
		if err == nil && existingUser.Id != "" {
			req.Id = existingUser.Id
			if err := w.Users.Update(cmdCtx, req); err != nil {
				return nil, err
			}
		} else {
			if _, err := w.Users.Create(cmdCtx, req); err != nil {
				return nil, err
			}
		}
	} else {
		return nil, dsc.ValidateRequired(dsc.RequiredField{Name: "user_name", Value: ""})
	}

	afterState, _ := h.getCurrentState(ctx, &req)
	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *UserHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	req, err := dsc.UnmarshalInput[iam.User](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	desiredState := userToState(&req)
	desiredState.Exist = true

	differing := dsc.CompareStates(desiredState, actualState)
	inDesiredState := len(differing) == 0

	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         actualState,
		InDesiredState:      inDesiredState,
		DifferingProperties: differing,
	}, nil
}

func (h *UserHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[iam.User](input)
	if err != nil {
		return err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	if req.Id != "" {
		return w.Users.DeleteById(cmdCtx, req.Id)
	}

	if req.UserName != "" {
		users := w.Users.List(cmdCtx, iam.ListUsersRequest{
			Filter: "userName eq \"" + req.UserName + "\"",
		})

		user, err := users.Next(cmdCtx)
		if err != nil {
			return dsc.NotFoundError("user", "user_name="+req.UserName)
		}
		return w.Users.DeleteById(cmdCtx, user.Id)
	}

	return dsc.ValidateRequired(dsc.RequiredField{Name: "id or user_name", Value: ""})
}

func (h *UserHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	var allUsers []any

	users := w.Users.List(cmdCtx, iam.ListUsersRequest{})
	for {
		user, err := users.Next(cmdCtx)
		if err != nil {
			break
		}
		allUsers = append(allUsers, userToState(&user))
	}

	return allUsers, nil
}

func userToState(user *iam.User) UserState {
	return UserState{
		ID:          user.Id,
		UserName:    user.UserName,
		DisplayName: user.DisplayName,
		Active:      user.Active,
		Exist:       true,
	}
}

// getWorkspaceClient creates a new Databricks workspace client.
func getWorkspaceClient(ctx dsc.ResourceContext) (context.Context, *databricks.WorkspaceClient, error) {
	cmdCtx := ctx.Cmd.Context()
	if cmdCtx == nil {
		cmdCtx = context.Background()
	}

	w, err := databricks.NewWorkspaceClient()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Databricks client: %w", err)
	}

	return cmdCtx, w, nil
}
