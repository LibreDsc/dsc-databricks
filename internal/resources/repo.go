package resources

import (
	"encoding/json"
	"reflect"

	"github.com/LibreDsc/dsc-databricks/internal/dsc"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

func init() {
	dsc.RegisterResourceWithMetadata("LibreDsc.Databricks/Repo", &RepoHandler{}, repoMetadata())
}

// Repo property descriptions from SDK documentation.
var repoPropertyDescriptions = dsc.PropertyDescriptions{
	"path":          "Desired path for the repo in the workspace. If the repo is in /Repos, the path must be in the format /Repos/{folder}/{repo-name}.",
	"url":           "URL of the remote Git repository to link. Required when creating a new repo.",
	"provider":      "Git provider (case-insensitive). Valid values: gitHub, bitbucketCloud, gitLab, azureDevOpsServices, gitHubEnterprise, bitbucketServer, gitLabEnterpriseEdition, awsCodeCommit.",
	"branch":        "Branch to check out. When omitted on create, the repository default branch is used.",
	"id":            "Databricks numeric ID of the repo object in the workspace.",
	"head_commit_id": "SHA-1 hash of the current HEAD commit.",
}

// RepoSchemaInput defines the desired-state fields for the schema and for
// unmarshaling input. id and head_commit_id are read-only/computed, so they
// are omitted from the schema type and carry omitempty to remain optional on
// input.
type RepoSchemaInput struct {
	Path     string `json:"path"`
	URL      string `json:"url,omitempty"`
	Provider string `json:"provider,omitempty"`
	Branch   string `json:"branch,omitempty"`
}

func repoMetadata() dsc.ResourceMetadata {
	return dsc.BuildMetadata(dsc.MetadataConfig{
		ResourceType:      "LibreDsc.Databricks/Repo",
		Description:       "Manage Databricks Git folders (repos)",
		SchemaDescription: "Schema for managing Databricks Git folders linked to a remote repository.",
		ResourceName:      "repo",
		Tags:              []string{"databricks", "repo", "git", "workspace"},
		Descriptions:      repoPropertyDescriptions,
		SchemaType:        reflect.TypeFor[RepoSchemaInput](),
		// head_commit_id is a computed property that differs from any input
		// representation, but it is never part of desired state. branch is
		// plain value equality. The DSC synthetic test is sufficient.
		OmitTest: true,
	})
}

// RepoState represents the full state of a Databricks repo.
type RepoState struct {
	Path         string `json:"path"`
	URL          string `json:"url,omitempty"`
	Provider     string `json:"provider,omitempty"`
	Branch       string `json:"branch,omitempty"`
	HeadCommitID string `json:"head_commit_id,omitempty"`
	ID           int64  `json:"id,omitempty"`
	Exist        bool   `json:"_exist"`
}

// RepoHandler handles Repo resource operations.
type RepoHandler struct{}

func repoInfoToState(r *workspace.RepoInfo) RepoState {
	return RepoState{
		ID:           r.Id,
		Path:         r.Path,
		URL:          r.Url,
		Provider:     r.Provider,
		Branch:       r.Branch,
		HeadCommitID: r.HeadCommitId,
		Exist:        true,
	}
}

func (h *RepoHandler) getByPath(ctx dsc.ResourceContext, path string) (RepoState, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return RepoState{Path: path, Exist: false}, err
	}

	info, err := w.Repos.GetByPath(cmdCtx, path)
	if err != nil {
		return RepoState{Path: path, Exist: false}, nil
	}
	return repoInfoToState(info), nil
}

func (h *RepoHandler) Get(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.GetResult, error) {
	req, err := dsc.UnmarshalInput[RepoSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "path", Value: req.Path}); err != nil {
		return nil, err
	}

	state, err := h.getByPath(ctx, req.Path)
	if err != nil {
		return nil, err
	}

	if !state.Exist {
		state.URL = req.URL
		state.Provider = req.Provider
		state.Branch = req.Branch
	}

	return &dsc.GetResult{ActualState: state}, nil
}

func (h *RepoHandler) Set(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.SetResult, error) {
	req, err := dsc.UnmarshalInput[RepoSchemaInput](input)
	if err != nil {
		return nil, err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "path", Value: req.Path}); err != nil {
		return nil, err
	}

	beforeState, err := h.getByPath(ctx, req.Path)
	if err != nil {
		return nil, err
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	if !beforeState.Exist {
		if err := dsc.ValidateRequired(
			dsc.RequiredField{Name: "url", Value: req.URL},
			dsc.RequiredField{Name: "provider", Value: req.Provider},
		); err != nil {
			return nil, err
		}

		created, err := w.Repos.Create(cmdCtx, workspace.CreateRepoRequest{
			Url:      req.URL,
			Provider: req.Provider,
			Path:     req.Path,
		})
		if err != nil {
			return nil, err
		}

		afterState := RepoState{
			ID:           created.Id,
			Path:         created.Path,
			URL:          created.Url,
			Provider:     created.Provider,
			Branch:       created.Branch,
			HeadCommitID: created.HeadCommitId,
			Exist:        true,
		}

		// If a specific branch was requested and differs from the cloned default, update now.
		if req.Branch != "" && req.Branch != created.Branch {
			if err := w.Repos.Update(cmdCtx, workspace.UpdateRepoRequest{
				RepoId: created.Id,
				Branch: req.Branch,
			}); err != nil {
				return nil, err
			}
			afterState.Branch = req.Branch
		}

		changedProps := dsc.CompareStates(beforeState, afterState)
		return &dsc.SetResult{
			BeforeState:       beforeState,
			AfterState:        afterState,
			ChangedProperties: changedProps,
		}, nil
	}

	// Repo already exists — update the branch if requested and different.
	afterState := beforeState
	if req.Branch != "" && req.Branch != beforeState.Branch {
		if err := w.Repos.Update(cmdCtx, workspace.UpdateRepoRequest{
			RepoId: beforeState.ID,
			Branch: req.Branch,
		}); err != nil {
			return nil, err
		}
		afterState.Branch = req.Branch
	}

	changedProps := dsc.CompareStates(beforeState, afterState)
	return &dsc.SetResult{
		BeforeState:       beforeState,
		AfterState:        afterState,
		ChangedProperties: changedProps,
	}, nil
}

func (h *RepoHandler) Test(ctx dsc.ResourceContext, input json.RawMessage) (*dsc.TestResult, error) {
	// OmitTest: true — DSC uses the synthetic test (value equality).
	// This method is never invoked via the manifest, but must satisfy the interface.
	req, err := dsc.UnmarshalInput[RepoSchemaInput](input)
	if err != nil {
		return nil, err
	}

	getResult, err := h.Get(ctx, input)
	if err != nil {
		return nil, err
	}

	desiredState := RepoState{
		Path:     req.Path,
		URL:      req.URL,
		Provider: req.Provider,
		Branch:   req.Branch,
		Exist:    true,
	}

	differing := dsc.CompareStates(desiredState, getResult.ActualState)
	return &dsc.TestResult{
		DesiredState:        desiredState,
		ActualState:         getResult.ActualState,
		InDesiredState:      len(differing) == 0,
		DifferingProperties: differing,
	}, nil
}

func (h *RepoHandler) Delete(ctx dsc.ResourceContext, input json.RawMessage) error {
	req, err := dsc.UnmarshalInput[RepoSchemaInput](input)
	if err != nil {
		return err
	}
	if err := dsc.ValidateRequired(dsc.RequiredField{Name: "path", Value: req.Path}); err != nil {
		return err
	}

	state, err := h.getByPath(ctx, req.Path)
	if err != nil {
		return err
	}
	if !state.Exist {
		return nil
	}

	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return err
	}

	return w.Repos.DeleteByRepoId(cmdCtx, state.ID)
}

func (h *RepoHandler) Export(ctx dsc.ResourceContext) ([]any, error) {
	cmdCtx, w, err := getWorkspaceClient(ctx)
	if err != nil {
		return nil, err
	}

	repos, err := w.Repos.ListAll(cmdCtx, workspace.ListReposRequest{})
	if err != nil {
		return nil, err
	}

	all := make([]any, 0, len(repos))
	for i := range repos {
		all = append(all, repoInfoToState(&repos[i]))
	}
	return all, nil
}
