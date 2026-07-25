package resources

import (
	"context"
	"fmt"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/iam"
)

// UserState represents the state of a Databricks workspace user.
type UserState struct {
	dsc.ExistProperty
	ID           string             `json:"id,omitempty" description:"Databricks user ID. This is the unique identifier for the user."`
	UserName     string             `json:"user_name" description:"Email address of the Databricks user. This is used as the primary identifier."`
	DisplayName  string             `json:"display_name,omitempty" description:"String that represents a concatenation of given and family names."`
	Emails       []UserComplexValue `json:"emails,omitempty" description:"All the emails associated with the Databricks user."`
	Entitlements []UserComplexValue `json:"entitlements,omitempty" description:"Entitlements assigned to the user."`
	Roles        []UserComplexValue `json:"roles,omitempty" description:"Corresponds to AWS instance profile/arn role."`
	Active       bool               `json:"active" dsc:"optional" description:"If this user is active."`
}

func (u *UserState) toCreateRequest() iam.CreateUserRequest {
	return iam.CreateUserRequest{
		Id:           u.ID,
		UserName:     u.UserName,
		DisplayName:  u.DisplayName,
		Active:       u.Active,
		Emails:       toIamComplexValues(u.Emails),
		Entitlements: toIamComplexValues(u.Entitlements),
		Roles:        toIamComplexValues(u.Roles),
	}
}

func userConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/User",
		Version:     "0.1.0",
		Description: "Manage Databricks workspace users",
		Tags:        []string{"databricks", "user", "iam", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks workspace users.",
			AllowAdditionalProperties: true,
			Overrides:                 userSchemaOverrides,
		},
	}
}

// userSchemaOverrides adds enum constraints to the generated schema.
func userSchemaOverrides(schema map[string]any) {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return
	}

	// Add enum to entitlements items value field.
	entitlementEnum := []string{
		"workspace-consume",
		"workspace-access",
		"databricks-sql-access",
		"allow-cluster-create",
		"allow-instance-pool-create",
	}
	setNestedEnum(props, "entitlements", "value", entitlementEnum)
}

// UserHandler handles User resource operations.
type UserHandler struct{}

func (h *UserHandler) Get(ctx context.Context, in UserState) (UserState, error) {
	if err := requireAtLeastOne("id or user_name", in.ID, in.UserName); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	if in.ID != "" {
		logDebugf(MsgLookup, "User", "id="+in.ID)
		user, err := w.UsersV2.Get(ctx, iam.GetUserRequest{Id: in.ID})
		if err != nil {
			logInfof(MsgNotFound, "User", "id="+in.ID)
			return dsc.NotFound(UserState{ID: in.ID, UserName: in.UserName}, "User", "id="+in.ID)
		}
		return userToState(user), nil
	}

	logDebugf(MsgLookup, "User", "user_name="+in.UserName)
	users := w.UsersV2.List(ctx, iam.ListUsersRequest{
		Filter: "userName eq \"" + in.UserName + "\"",
	})

	user, err := users.Next(ctx)
	if err != nil {
		logInfof(MsgNotFound, "User", "user_name="+in.UserName)
		return dsc.NotFound(UserState{UserName: in.UserName}, "User", "user_name="+in.UserName)
	}
	return userToState(&user), nil
}

func (h *UserHandler) Set(ctx context.Context, desired UserState) (UserState, error) {
	if err := requireAtLeastOne("id or user_name", desired.ID, desired.UserName); err != nil {
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
		logInfof(MsgUpdate, "User", "id="+current.ID)
		// User already exists — GET the full user, overlay desired fields, then PUT back.
		// SCIM PUT requires the complete user representation including schemas, emails, etc.
		fullUser, err := w.UsersV2.Get(ctx, iam.GetUserRequest{Id: current.ID})
		if err != nil {
			return desired, fmt.Errorf("failed to get user for update: %w", err)
		}
		if desired.DisplayName != "" {
			fullUser.DisplayName = desired.DisplayName
		}
		fullUser.Active = desired.Active
		if len(desired.Emails) > 0 {
			fullUser.Emails = toIamComplexValues(desired.Emails)
		}
		if len(desired.Entitlements) > 0 {
			fullUser.Entitlements = toIamComplexValues(desired.Entitlements)
		}
		if len(desired.Roles) > 0 {
			fullUser.Roles = toIamComplexValues(desired.Roles)
		}
		updateReq := userToUpdateRequest(fullUser)
		if err := w.UsersV2.Update(ctx, updateReq); err != nil {
			return desired, fmt.Errorf("failed to update user: %w", err)
		}
		desired.ID = current.ID
	} else {
		logInfof(MsgCreate, "User", "user_name="+desired.UserName)
		// User doesn't exist — create it.
		if err := requireFields(field{"user_name", desired.UserName}); err != nil {
			return desired, err
		}
		if _, err := w.UsersV2.Create(ctx, desired.toCreateRequest()); err != nil {
			return desired, err
		}
	}

	return h.Get(ctx, desired)
}

// projectUserUpdate mirrors Set's SCIM overlay: non-empty desired strings and
// non-empty slices win, active is always taken from desired (Set force-sends
// it), and the id carries over from the current state.
func projectUserUpdate(desired, current *UserState) UserState {
	projected := *current
	if desired.DisplayName != "" {
		projected.DisplayName = desired.DisplayName
	}
	projected.Active = desired.Active
	if len(desired.Emails) > 0 {
		projected.Emails = desired.Emails
	}
	if len(desired.Entitlements) > 0 {
		projected.Entitlements = desired.Entitlements
	}
	if len(desired.Roles) > 0 {
		projected.Roles = desired.Roles
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the user.
func (h *UserHandler) SetWhatIf(ctx context.Context, desired UserState) (UserState, error) {
	if err := requireAtLeastOne("id or user_name", desired.ID, desired.UserName); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if current.ShouldExist() {
		logInfof(MsgWhatIfUpdate, "User", "id="+current.ID)
		return projectUserUpdate(&desired, &current), nil
	}

	if err := requireFields(field{"user_name", desired.UserName}); err != nil {
		return desired, err
	}
	logInfof(MsgWhatIfCreate, "User", "user_name="+desired.UserName)
	// The server-assigned id is unknown before creation.
	projected := desired
	projected.ID = ""
	projected.SetExist(true)
	return projected, nil
}

func (h *UserHandler) Test(ctx context.Context, desired UserState) (dsc.TestResult[UserState], error) {
	actual, err := h.Get(ctx, desired)
	if err != nil {
		return dsc.TestResult[UserState]{}, err
	}

	result := dsc.TestResult[UserState]{ActualState: actual}
	if !actual.ShouldExist() {
		// CompareStates skips canonical properties; report existence drift explicitly.
		result.DifferingProperties = []string{"_exist"}
		return result, nil
	}
	result.DifferingProperties = dsc.CompareStates(desired, actual)
	return result, nil
}

func (h *UserHandler) Delete(ctx context.Context, in UserState) error {
	if err := requireAtLeastOne("id or user_name", in.ID, in.UserName); err != nil {
		return err
	}

	w, err := workspaceClient()
	if err != nil {
		return err
	}

	if in.ID != "" {
		logDebugf(MsgDelete, "User", "id="+in.ID)
		return w.UsersV2.Delete(ctx, iam.DeleteUserRequest{Id: in.ID})
	}

	logDebugf(MsgDelete, "User", "user_name="+in.UserName)
	users := w.UsersV2.List(ctx, iam.ListUsersRequest{
		Filter: "userName eq \"" + in.UserName + "\"",
	})

	user, err := users.Next(ctx)
	if err != nil {
		// Already absent — deleting a missing instance succeeds.
		logInfof(MsgNotFound, "User", "user_name="+in.UserName)
		return nil
	}
	return w.UsersV2.Delete(ctx, iam.DeleteUserRequest{Id: user.Id})
}

func (h *UserHandler) Export(ctx context.Context, _ UserState) ([]UserState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "User")
	users, err := w.UsersV2.ListAll(ctx, iam.ListUsersRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}

	var allUsers []UserState
	for i := range users {
		allUsers = append(allUsers, userToState(&users[i]))
	}

	return allUsers, nil
}

func userToState(user *iam.User) UserState {
	state := UserState{
		ID:           user.Id,
		UserName:     user.UserName,
		DisplayName:  user.DisplayName,
		Active:       user.Active,
		Emails:       fromIamComplexValues(user.Emails),
		Entitlements: fromIamComplexValues(user.Entitlements),
		Roles:        fromIamComplexValues(user.Roles),
	}
	state.SetExist(true)
	return state
}

// userToUpdateRequest converts a full iam.User (from Get) to an UpdateUserRequest for PUT.
func userToUpdateRequest(user *iam.User) iam.UpdateUserRequest {
	return iam.UpdateUserRequest{
		Id:              user.Id,
		Active:          user.Active,
		DisplayName:     user.DisplayName,
		Emails:          user.Emails,
		Entitlements:    user.Entitlements,
		ExternalId:      user.ExternalId,
		Groups:          user.Groups,
		Name:            user.Name,
		Roles:           user.Roles,
		Schemas:         user.Schemas,
		UserName:        user.UserName,
		ForceSendFields: []string{"Active"},
	}
}
