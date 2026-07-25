package resources

import (
	"context"

	dsc "github.com/LibreDsc/dsc-go-rdk"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

// RepoState represents the full state of a Databricks repo. head_commit_id and
// id are computed by the service.
type RepoState struct {
	dsc.ExistProperty
	Path         string `json:"path" description:"Desired path for the repo in the workspace. If the repo is in /Repos, the path must be in the format /Repos/{folder}/{repo-name}."`
	URL          string `json:"url,omitempty" description:"URL of the remote Git repository to link. Required when creating a new repo."`
	Provider     string `json:"provider,omitempty" description:"Git provider (case-insensitive). Valid values: gitHub, bitbucketCloud, gitLab, azureDevOpsServices, gitHubEnterprise, bitbucketServer, gitLabEnterpriseEdition, awsCodeCommit."`
	Branch       string `json:"branch,omitempty" description:"Branch to check out. When omitted on create, the repository default branch is used."`
	HeadCommitID string `json:"head_commit_id,omitempty" description:"SHA-1 hash of the current HEAD commit. (read-only)"`
	ID           int64  `json:"id,omitempty" description:"Databricks numeric ID of the repo object in the workspace. (read-only)"`
}

func repoConfig() dsc.ResourceConfig {
	return dsc.ResourceConfig{
		Type:        "LibreDsc.Databricks/Repo",
		Version:     "0.1.0",
		Description: "Manage Databricks Git folders (repos)",
		Tags:        []string{"databricks", "repo", "git", "workspace"},
		SetReturn:   dsc.SetReturnStateAndDiff,
		SchemaOptions: dsc.SchemaOptions{
			SchemaDescription:         "Schema for managing Databricks Git folders linked to a remote repository.",
			AllowAdditionalProperties: true,
		},
	}
}

// RepoHandler handles Repo resource operations. head_commit_id is a computed
// property that is never part of desired state and branch is plain value
// equality, so no Testable is implemented — the DSC synthetic test suffices.
type RepoHandler struct{}

func repoInfoToState(r *workspace.RepoInfo) RepoState {
	state := RepoState{
		ID:           r.Id,
		Path:         r.Path,
		URL:          r.Url,
		Provider:     r.Provider,
		Branch:       r.Branch,
		HeadCommitID: r.HeadCommitId,
	}
	state.SetExist(true)
	return state
}

func (h *RepoHandler) Get(ctx context.Context, in RepoState) (RepoState, error) {
	if err := requireFields(field{"path", in.Path}); err != nil {
		return in, err
	}

	w, err := workspaceClient()
	if err != nil {
		return in, err
	}

	logDebugf(MsgLookup, "Repo", "path="+in.Path)
	info, err := w.Repos.GetByPath(ctx, in.Path)
	if err != nil {
		logInfof(MsgNotFound, "Repo", "path="+in.Path)
		// Echo back the requested settings so the missing state is self-describing.
		return dsc.NotFound(RepoState{
			Path:     in.Path,
			URL:      in.URL,
			Provider: in.Provider,
			Branch:   in.Branch,
		}, "Repo", "path="+in.Path)
	}
	return repoInfoToState(info), nil
}

func (h *RepoHandler) Set(ctx context.Context, desired RepoState) (RepoState, error) {
	if err := requireFields(field{"path", desired.Path}); err != nil {
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

	if !current.ShouldExist() {
		logInfof(MsgCreate, "Repo", "path="+desired.Path)
		if err := requireFields(
			field{"url", desired.URL},
			field{"provider", desired.Provider},
		); err != nil {
			return desired, err
		}

		created, err := w.Repos.Create(ctx, workspace.CreateRepoRequest{
			Url:      desired.URL,
			Provider: desired.Provider,
			Path:     desired.Path,
		})
		if err != nil {
			return desired, err
		}

		afterState := RepoState{
			ID:           created.Id,
			Path:         created.Path,
			URL:          created.Url,
			Provider:     created.Provider,
			Branch:       created.Branch,
			HeadCommitID: created.HeadCommitId,
		}
		afterState.SetExist(true)

		// If a specific branch was requested and differs from the cloned default, update now.
		if desired.Branch != "" && desired.Branch != created.Branch {
			if err := w.Repos.Update(ctx, workspace.UpdateRepoRequest{
				RepoId: created.Id,
				Branch: desired.Branch,
			}); err != nil {
				return desired, err
			}
			afterState.Branch = desired.Branch
		}
		return afterState, nil
	}

	// Repo already exists — update the branch if requested and different.
	logInfof(MsgUpdate, "Repo", "path="+desired.Path)
	afterState := current
	if desired.Branch != "" && desired.Branch != current.Branch {
		if err := w.Repos.Update(ctx, workspace.UpdateRepoRequest{
			RepoId: current.ID,
			Branch: desired.Branch,
		}); err != nil {
			return desired, err
		}
		afterState.Branch = desired.Branch
	}
	return afterState, nil
}

// projectRepoCreate returns the state Set's create path would produce.
// id and head_commit_id are computed by the server and stay zero; when no
// branch is requested the repository default branch is unknown ahead of time.
func projectRepoCreate(desired *RepoState) RepoState {
	projected := RepoState{
		Path:     desired.Path,
		URL:      desired.URL,
		Provider: desired.Provider,
		Branch:   desired.Branch,
	}
	projected.SetExist(true)
	return projected
}

// projectRepoUpdate mirrors Set's update path: only the branch changes, and
// only when requested and different.
func projectRepoUpdate(desired, current *RepoState) RepoState {
	projected := *current
	if desired.Branch != "" && desired.Branch != current.Branch {
		projected.Branch = desired.Branch
	}
	projected.SetExist(true)
	return projected
}

// SetWhatIf predicts the state Set would produce without touching the repo.
func (h *RepoHandler) SetWhatIf(ctx context.Context, desired RepoState) (RepoState, error) {
	if err := requireFields(field{"path", desired.Path}); err != nil {
		return desired, err
	}

	current, err := h.Get(ctx, desired)
	if err != nil {
		return desired, err
	}

	if !current.ShouldExist() {
		if err := requireFields(
			field{"url", desired.URL},
			field{"provider", desired.Provider},
		); err != nil {
			return desired, err
		}
		logInfof(MsgWhatIfCreate, "Repo", "path="+desired.Path)
		return projectRepoCreate(&desired), nil
	}

	logInfof(MsgWhatIfUpdate, "Repo", "path="+desired.Path)
	return projectRepoUpdate(&desired, &current), nil
}

func (h *RepoHandler) Delete(ctx context.Context, in RepoState) error {
	if err := requireFields(field{"path", in.Path}); err != nil {
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

	logDebugf(MsgDelete, "Repo", "path="+in.Path)
	return w.Repos.DeleteByRepoId(ctx, current.ID)
}

func (h *RepoHandler) Export(ctx context.Context, _ RepoState) ([]RepoState, error) {
	w, err := workspaceClient()
	if err != nil {
		return nil, err
	}

	logDebugf(MsgListAll, "Repo")
	repos, err := w.Repos.ListAll(ctx, workspace.ListReposRequest{})
	if err != nil {
		return nil, err
	}

	all := make([]RepoState, 0, len(repos))
	for i := range repos {
		all = append(all, repoInfoToState(&repos[i]))
	}
	return all, nil
}
