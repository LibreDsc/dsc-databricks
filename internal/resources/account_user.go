package resources

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/AccountUser", &AccountUserHandler{}, accountUserMetadata())
}

// AccountUser property descriptions from SDK documentation.
var accountUserPropertyDescriptions = dsc.PropertyDescriptions{
	"id":           "Databricks account user ID.",
	"user_name":    "Email address of the Databricks account user. This is used as the primary identifier.",
	"display_name": "String that represents a concatenation of given and family names.",
	"active":       "If this user is active.",
	"emails":       "All the emails associated with the Databricks account user.",
	"roles":        "Corresponds to AWS instance profile/ARN role.",
}

// AccountUserSchemaInput defines the desired-state fields for the schema
// and for unmarshaling input. id is computed on create so it carries omitempty.
type AccountUserSchemaInput struct {
	UserName    string             `json:"user_name"`
	DisplayName string             `json:"display_name,omitempty"`
	ID          string             `json:"id,omitempty"`
	Emails      []UserComplexValue `json:"emails,omitempty"`
	Roles       []UserComplexValue `json:"roles,omitempty"`
	Active      bool               `json:"active,omitempty"`
}

func (u AccountUserSchemaInput) toCreateRequest() iam.CreateAccountUserRequest {
	return iam.CreateAccountUserRequest{
		UserName:    u.UserName,
		DisplayName: u.DisplayName,
		Active:      u.Active,
		Emails:      toIamComplexValues(u.Emails),
		Roles:       toIamComplexValues(u.Roles),
	}
}

func accountUserMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/AccountUser",
		Description:       "Manage Databricks account-level users",
		SchemaDescription: "Schema for managing Databricks account-level users.",
		ResourceName:      "account user",
		Tags:              []string{"databricks", "user", "iam", "account"},
		Descriptions:      accountUserPropertyDescriptions,
		SchemaType:        reflect.TypeFor[AccountUserSchemaInput](),
	})
}

// AccountUserState represents the full state of a Databricks account-level user.
type AccountUserState struct {
	UserName    string             `json:"user_name"`
	DisplayName string             `json:"display_name,omitempty"`
	ID          string             `json:"id,omitempty"`
	Emails      []UserComplexValue `json:"emails,omitempty"`
	Roles       []UserComplexValue `json:"roles,omitempty"`
	Active      bool               `json:"active"`
	Exist       bool               `json:"_exist"`
}

// AccountUserHandler handles AccountUser resource operations.
type AccountUserHandler struct{}

func (h *AccountUserHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[AccountUserSchemaInput](input)
	if err != nil {
		return nil, err
	}

	state, err := h.getCurrentState(ctx, &req)
	if err != nil {
		return nil, err
	}

	return &dsc.GetResult{ActualState: state}, nil
}

// getCurrentState retrieves the current state for an account user from the API.
func (h *AccountUserHandler) getCurrentState(ctx dsc.ResourceContext, req *AccountUserSchemaInput) (AccountUserState, error) {
	cmdCtx, a, err := getAccountClient(ctx)
	if err != nil {
		return AccountUserState{Exist: false}, err
	}

	if req.ID != "" {
		dsc.Logger.Debugf(dsc.MsgLookup, "AccountUser", "id="+req.ID)
		user, err := a.UsersV2.Get(cmdCtx, iam.GetAccountUserRequest{Id: req.ID})
		if err != nil {
			dsc.Logger.Infof(dsc.MsgNotFound, "AccountUser", "id="+req.ID)
			return AccountUserState{Exist: false}, nil
		}
		return accountUserToState(user), nil
	}

	if req.UserName != "" {
		dsc.Logger.Debugf(dsc.MsgLookup, "AccountUser", "user_name="+req.UserName)
		users := a.UsersV2.List(cmdCtx, iam.ListAccountUsersRequest{
			Filter: "userName eq \"" + req.UserName + "\"",
		})

		user, err := users.Next(cmdCtx)
		if err != nil {
			dsc.Logger.Infof(dsc.MsgNotFound, "AccountUser", "user_name="+req.UserName)
			return AccountUserState{UserName: req.UserName, Exist: false}, nil
		}
		return accountUserToState(&user), nil
	}

	return AccountUserState{Exist: false}, fmt.Errorf("id or user_name must be provided")
}

func (h *AccountUserHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	schemaInput, err := dsc.UnmarshalInput[AccountUserSchemaInput](input)
	if err != nil {
		return nil, err
	}

	beforeState, _ := h.getCurrentState(ctx, &schemaInput)

	cmdCtx, a, err := getAccountClient(ctx)
	if err != nil {
		return nil, err
	}

	if beforeState.Exist {
		dsc.Logger.Infof(dsc.MsgUpdate, "AccountUser", "id="+beforeState.ID)
		// User already exists — GET the full user, overlay desired fields, then PUT back.
		fullUser, err := a.UsersV2.Get(cmdCtx, iam.GetAccountUserRequest{Id: beforeState.ID})
		if err != nil {
			return nil, fmt.Errorf("failed to get account user for update: %w", err)
		}
		if schemaInput.DisplayName != "" {
			fullUser.DisplayName = schemaInput.DisplayName
		}
		fullUser.Active = schemaInput.Active
		if len(schemaInput.Emails) > 0 {
			fullUser.Emails = toIamComplexValues(schemaInput.Emails)
		}
		if len(schemaInput.Roles) > 0 {
			fullUser.Roles = toIamComplexValues(schemaInput.Roles)
		}
		updateReq := accountUserToUpdateRequest(fullUser)
		if err := a.UsersV2.Update(cmdCtx, updateReq); err != nil {
			return nil, fmt.Errorf("failed to update account user: %w", err)
		}
		schemaInput.ID = beforeState.ID
	} else if schemaInput.UserName != "" {
		dsc.Logger.Infof(dsc.MsgCreate, "AccountUser", "user_name="+schemaInput.UserName)
		// User doesn't exist — create it.
		if _, err := a.UsersV2.Create(cmdCtx, schemaInput.toCreateRequest()); err != nil {
			return nil, err
		}
	} else {
		return nil, dsc.ValidateRequired(dsc.RequiredField{Name: "user_name", Value: ""})
	}

	afterState, _ := h.getCurrentState(ctx, &schemaInput)
	changedProps := dsc.CompareStates(beforeState, afterState)

	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *AccountUserHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	schemaInput, err := dsc.UnmarshalInput[AccountUserSchemaInput](input)
	if err != nil {
		return nil, err
	}

	actualState, err := h.getCurrentState(ctx, &schemaInput)
	if err != nil {
		return nil, err
	}

	desiredState := AccountUserState{
		ID:          schemaInput.ID,
		UserName:    schemaInput.UserName,
		DisplayName: schemaInput.DisplayName,
		Active:      schemaInput.Active,
		Emails:      schemaInput.Emails,
		Roles:       schemaInput.Roles,
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

func (h *AccountUserHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	schemaInput, err := dsc.UnmarshalInput[AccountUserSchemaInput](input)
	if err != nil {
		return err
	}

	cmdCtx, a, err := getAccountClient(ctx)
	if err != nil {
		return err
	}

	if schemaInput.ID != "" {
		dsc.Logger.Debugf(dsc.MsgDelete, "AccountUser", "id="+schemaInput.ID)
		return a.UsersV2.Delete(cmdCtx, iam.DeleteAccountUserRequest{Id: schemaInput.ID})
	}

	if schemaInput.UserName != "" {
		dsc.Logger.Debugf(dsc.MsgDelete, "AccountUser", "user_name="+schemaInput.UserName)
		users := a.UsersV2.List(cmdCtx, iam.ListAccountUsersRequest{
			Filter: "userName eq \"" + schemaInput.UserName + "\"",
		})

		user, err := users.Next(cmdCtx)
		if err != nil {
			return dsc.NotFoundError("account user", "user_name="+schemaInput.UserName)
		}
		return a.UsersV2.Delete(cmdCtx, iam.DeleteAccountUserRequest{Id: user.Id})
	}

	return dsc.ValidateRequired(dsc.RequiredField{Name: "id or user_name", Value: ""})
}

func (h *AccountUserHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, a, err := getAccountClient(ctx)
	if err != nil {
		return nil, err
	}

	dsc.Logger.Debugf(dsc.MsgListAll, "AccountUser")
	users, err := a.UsersV2.ListAll(cmdCtx, iam.ListAccountUsersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list account users: %w", err)
	}

	var allUsers []any
	for i := range users {
		allUsers = append(allUsers, accountUserToState(&users[i]))
	}

	return allUsers, nil
}

func accountUserToState(user *iam.AccountUser) AccountUserState {
	return AccountUserState{
		ID:          user.Id,
		UserName:    user.UserName,
		DisplayName: user.DisplayName,
		Active:      user.Active,
		Emails:      fromIamComplexValues(user.Emails),
		Roles:       fromIamComplexValues(user.Roles),
		Exist:       true,
	}
}

// accountUserToUpdateRequest converts a full iam.AccountUser (from Get) to an UpdateAccountUserRequest for PUT.
func accountUserToUpdateRequest(user *iam.AccountUser) iam.UpdateAccountUserRequest {
	return iam.UpdateAccountUserRequest{
		Id:              user.Id,
		Active:          user.Active,
		DisplayName:     user.DisplayName,
		Emails:          user.Emails,
		ExternalId:      user.ExternalId,
		Name:            user.Name,
		Roles:           user.Roles,
		UserName:        user.UserName,
		ForceSendFields: []string{"Active"},
	}
}
