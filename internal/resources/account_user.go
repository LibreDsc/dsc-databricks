package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

// AccountUserState represents the full state of a Databricks account-level user.
type AccountUserState struct {
	dsc.ExistProperty
	UserName    string             `json:"user_name" description:"Email address of the Databricks account user. This is used as the primary identifier."`
	DisplayName string             `json:"display_name,omitempty" description:"String that represents a concatenation of given and family names."`
	ID          string             `json:"id,omitempty" description:"Databricks account user ID."`
	Emails      []UserComplexValue `json:"emails,omitempty" description:"All the emails associated with the Databricks account user."`
	Roles       []UserComplexValue `json:"roles,omitempty" description:"Corresponds to AWS instance profile/ARN role."`
	Active      bool               `json:"active" dsc:"optional" description:"If this user is active."`
}

func (u *AccountUserState) toCreateRequest() iam.CreateAccountUserRequest {
	return iam.CreateAccountUserRequest{
		UserName:    u.UserName,
		DisplayName: u.DisplayName,
		Active:      u.Active,
		Emails:      toIamComplexValues(u.Emails),
		Roles:       toIamComplexValues(u.Roles),
	}
}

func accountUserConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/AccountUser",
		Version:     "0.1.0",
		Description: "Manage Databricks account-level users",
		Tags:        []string{"databricks", "user", "iam", "account"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks account-level users.",
			AllowAdditionalProperties: true,
		},
	}
}

// AccountUserHandler handles AccountUser resource operations.
type AccountUserHandler struct{}

func (h *AccountUserHandler) Get(ctx context.Context, in AccountUserState) (AccountUserState, error) {
	if err := requireAtLeastOne("id or user_name", in.ID, in.UserName); err != nil {
		return in, err
	}

	a, err := accountClient()
	if err != nil {
		return in, err
	}

	if in.ID != "" {
		logDebugf(MsgLookup, "AccountUser", "id="+in.ID)
		user, err := a.UsersV2.Get(ctx, iam.GetAccountUserRequest{Id: in.ID})
		if err != nil {
			logInfof(MsgNotFound, "AccountUser", "id="+in.ID)
			return dsc.NotFound(AccountUserState{ID: in.ID, UserName: in.UserName}, "AccountUser", "id="+in.ID)
		}
		return accountUserToState(user), nil
	}

	logDebugf(MsgLookup, "AccountUser", "user_name="+in.UserName)
	users := a.UsersV2.List(ctx, iam.ListAccountUsersRequest{
		Filter: "userName eq \"" + in.UserName + "\"",
	})

	user, err := users.Next(ctx)
	if err != nil {
		logInfof(MsgNotFound, "AccountUser", "user_name="+in.UserName)
		return dsc.NotFound(AccountUserState{UserName: in.UserName}, "AccountUser", "user_name="+in.UserName)
	}
	return accountUserToState(&user), nil
}

func (h *AccountUserHandler) Set(ctx context.Context, desired AccountUserState) (AccountUserState, error) {
	if err := requireAtLeastOne("id or user_name", desired.ID, desired.UserName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	a, err := accountClient()
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgUpdate, "AccountUser", "id="+current.ID)
		// User already exists — GET the full user, overlay desired fields, then PUT back.
		fullUser, err := a.UsersV2.Get(ctx, iam.GetAccountUserRequest{Id: current.ID})
		if err != nil {
			return desired, fmt.Errorf("failed to get account user for update: %w", err)
		}
		if desired.DisplayName != "" {
			fullUser.DisplayName = desired.DisplayName
		}
		fullUser.Active = desired.Active
		if len(desired.Emails) > 0 {
			fullUser.Emails = toIamComplexValues(desired.Emails)
		}
		if len(desired.Roles) > 0 {
			fullUser.Roles = toIamComplexValues(desired.Roles)
		}
		updateReq := accountUserToUpdateRequest(fullUser)
		if err := a.UsersV2.Update(ctx, updateReq); err != nil {
			return desired, fmt.Errorf("failed to update account user: %w", err)
		}
		desired.ID = current.ID
	} else {
		logInfof(MsgCreate, "AccountUser", "user_name="+desired.UserName)
		// User doesn't exist — create it.
		if err := requireFields(field{"user_name", desired.UserName}); err != nil {
			return desired, err
		}
		if _, err := a.UsersV2.Create(ctx, desired.toCreateRequest()); err != nil {
			return desired, err
		}
	}

	return h.Get(ctx, desired)
}

// projectAccountUserUpdate mirrors Set's SCIM overlay: non-empty desired
// strings and non-empty slices win, active is always taken from desired (Set
// force-sends it), and the id carries over from the current state.
func projectAccountUserUpdate(desired, current *AccountUserState) AccountUserState {
	projected := *current
	if desired.DisplayName != "" {
		projected.DisplayName = desired.DisplayName
	}
	projected.Active = desired.Active
	if len(desired.Emails) > 0 {
		projected.Emails = desired.Emails
	}
	if len(desired.Roles) > 0 {
		projected.Roles = desired.Roles
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the user.
func (h *AccountUserHandler) SetWhatIf(ctx context.Context, desired AccountUserState) (AccountUserState, error) {
	if err := requireAtLeastOne("id or user_name", desired.ID, desired.UserName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "AccountUser", "id="+current.ID)
		return projectAccountUserUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"user_name", desired.UserName}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "AccountUser", "user_name="+desired.UserName)
	// The server-assigned id is unknown before creation.
	projected := desired
	projected.ID = ""
	projected.SetExist(true)
	return projected, nil
}

func (h *AccountUserHandler) Test(ctx context.Context, desired AccountUserState) (dsc.TestResult[AccountUserState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[AccountUserState]{}, err
	}

	result := dsc.TestResult[AccountUserState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *AccountUserHandler) Delete(ctx context.Context, in AccountUserState) error {
	if err := requireAtLeastOne("id or user_name", in.ID, in.UserName); err != nil {
		return err
	}

	a, err := accountClient()
	if err != nil {
		return err
	}

	if in.ID != "" {
		logDebugf(MsgDelete, "AccountUser", "id="+in.ID)
		return a.UsersV2.Delete(ctx, iam.DeleteAccountUserRequest{Id: in.ID})
	}

	logDebugf(MsgDelete, "AccountUser", "user_name="+in.UserName)
	users := a.UsersV2.List(ctx, iam.ListAccountUsersRequest{
		Filter: "userName eq \"" + in.UserName + "\"",
	})

	user, err := users.Next(ctx)
	if err != nil {
		// Already absent — deleting a missing instance succeeds.
		logInfof(MsgNotFound, "AccountUser", "user_name="+in.UserName)
		return nil
	}
	return a.UsersV2.Delete(ctx, iam.DeleteAccountUserRequest{Id: user.Id})
}

func (h *AccountUserHandler) Export(ctx context.Context, _ AccountUserState) ([]AccountUserState, error) {
	a, err := accountClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "AccountUser")
	users, err := a.UsersV2.ListAll(ctx, iam.ListAccountUsersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list account users: %w", err)
	}

	var allUsers []AccountUserState
	for i := range users {
		allUsers = append(allUsers, accountUserToState(&users[i]))
	}

	return allUsers, nil
}

func accountUserToState(user *iam.AccountUser) AccountUserState {
	state := AccountUserState{
		ID:          user.Id,
		UserName:    user.UserName,
		DisplayName: user.DisplayName,
		Active:      user.Active,
		Emails:      fromIamComplexValues(user.Emails),
		Roles:       fromIamComplexValues(user.Roles),
	}
	state.SetExist(true)
	return state
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
