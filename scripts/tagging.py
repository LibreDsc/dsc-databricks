#!/usr/bin/env python3

# /// script
# dependencies = ["PyGithub>=2,<3"]
# ///

"""
Release tagging script for dsc-databricks.

Reads NEXT_CHANGELOG.md, moves its content into CHANGELOG.md (with date stamp),
cleans NEXT_CHANGELOG.md (bumps minor version, clears sections), commits, pushes,
and creates an annotated git tag via the GitHub API.

Adapted from the Databricks CLI tagging workflow.

Usage:
    # Requires GITHUB_TOKEN and GITHUB_REPOSITORY environment variables.
    uv run --script scripts/tagging.py

    # Dry-run mode (no changes pushed):
    uv run --script scripts/tagging.py --dry-run
"""

import os
import re
import argparse
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timezone

from github import Github, InputGitTreeElement, InputGitAuthor

NEXT_CHANGELOG_FILE = "NEXT_CHANGELOG.md"
CHANGELOG_FILE = "CHANGELOG.md"


@dataclass
class TagInfo:
    version: str
    content: str

    def tag_name(self) -> str:
        return f"v{self.version}"


class GitHubRepo:
    """Wraps the GitHub API for creating commits and tags without local modifications."""

    def __init__(self, repo):
        self.repo = repo
        self.changed_files = []
        self.ref = "heads/main"
        head_ref = self.repo.get_git_ref(self.ref)
        self.sha = head_ref.object.sha

    def add_file(self, path: str, content: str):
        print(f"  Staging: {path}")
        blob = self.repo.create_git_blob(content=content, encoding="utf-8")
        element = InputGitTreeElement(path=path, mode="100644", type="blob", sha=blob.sha)
        self.changed_files.append(element)

    def commit_and_push(self, message: str):
        head_ref = self.repo.get_git_ref(self.ref)
        base_tree = self.repo.get_git_tree(sha=head_ref.object.sha)
        new_tree = self.repo.create_git_tree(self.changed_files, base_tree)
        parent_commit = self.repo.get_git_commit(head_ref.object.sha)
        new_commit = self.repo.create_git_commit(
            message=message, tree=new_tree, parents=[parent_commit]
        )
        head_ref.edit(new_commit.sha)
        self.sha = new_commit.sha
        self.changed_files = []
        print(f"  Committed: {new_commit.sha}")

    def tag(self, tag_name: str, tag_message: str):
        tagger = InputGitAuthor(
            name="github-actions[bot]",
            email="41898282+github-actions[bot]@users.noreply.github.com",
        )
        tag_obj = self.repo.create_git_tag(
            tag=tag_name,
            message=tag_message,
            object=self.sha,
            type="commit",
            tagger=tagger,
        )
        self.repo.create_git_ref(ref=f"refs/tags/{tag_name}", sha=tag_obj.sha)
        print(f"  Tagged: {tag_name}")


def get_next_tag_info() -> Optional[TagInfo]:
    """Reads NEXT_CHANGELOG.md and extracts the version + non-empty sections."""
    with open(NEXT_CHANGELOG_FILE, "r") as f:
        content = f.read()

    # Strip the "# NEXT CHANGELOG" header
    content = re.sub(r"^# NEXT CHANGELOG\n+", "", content, flags=re.MULTILINE)

    # Remove empty sections (### header with no bullet points before next ### or EOF)
    content = re.sub(r"###[^\n]+\n+(?=###|\Z)", "", content)

    # Normalize spacing before sections
    content = re.sub(r"(\n*)(###[^\n]+)", r"\n\n\2", content)

    if not re.search(r"###", content):
        print("All sections are empty. Nothing to release.")
        return None

    version_match = re.search(r"## Release v(\d+\.\d+\.\d+)", content)
    if not version_match:
        raise Exception("Version not found in NEXT_CHANGELOG.md")

    return TagInfo(version=version_match.group(1), content=content)


def write_changelog(tag_info: TagInfo) -> str:
    """Prepends the release entry (with date) into CHANGELOG.md. Returns new content."""
    with open(CHANGELOG_FILE, "r") as f:
        changelog = f.read()

    current_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    content_with_date = re.sub(
        r"## Release v(\d+\.\d+\.\d+)",
        rf"## Release v\1 ({current_date})",
        tag_info.content.strip(),
    )

    updated = re.sub(
        r"(# Version changelog\n)",
        f"\\1\n{content_with_date}\n\n",
        changelog,
    )
    return updated


def clean_next_changelog() -> str:
    """Bumps version in NEXT_CHANGELOG.md and clears section content. Returns new content."""
    with open(NEXT_CHANGELOG_FILE, "r") as f:
        content = f.read()

    # Remove content between ### sections, keep section headers
    cleaned = re.sub(r"(### [^\n]+\n)(?:.*?\n?)*?(?=###|$)", r"\1", content)
    # Normalize spacing
    cleaned = re.sub(r"(\n*)(###[^\n]+)", r"\n\n\2", cleaned)

    # Bump minor version
    version_match = re.search(r"Release v(\d+)\.(\d+)\.(\d+)", cleaned)
    if not version_match:
        raise Exception("Version not found in NEXT_CHANGELOG.md")

    major, minor, patch = map(int, version_match.groups())
    minor += 1
    patch = 0
    new_version = f"Release v{major}.{minor}.{patch}"
    cleaned = cleaned.replace(version_match.group(0), new_version)

    return cleaned


def main():
    parser = argparse.ArgumentParser(description="Tag a release for dsc-databricks")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without pushing changes",
    )
    args = parser.parse_args()

    # Read next changelog
    tag_info = get_next_tag_info()
    if tag_info is None:
        return

    print(f"Preparing release {tag_info.tag_name()}")

    # Prepare file contents
    new_changelog = write_changelog(tag_info)
    new_next_changelog = clean_next_changelog()

    if args.dry_run:
        print("\n--- DRY RUN ---")
        print(f"Would create tag: {tag_info.tag_name()}")
        print(f"\nNew CHANGELOG.md:\n{new_changelog[:500]}...")
        print(f"\nNew NEXT_CHANGELOG.md:\n{new_next_changelog}")
        return

    # Push via GitHub API
    token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repository:
        raise Exception("GITHUB_TOKEN and GITHUB_REPOSITORY environment variables are required")

    g = Github(token)
    repo = g.get_repo(repository)
    gh = GitHubRepo(repo)

    print("Committing changelog updates...")
    gh.add_file(CHANGELOG_FILE, new_changelog)
    gh.add_file(NEXT_CHANGELOG_FILE, new_next_changelog)
    gh.commit_and_push(f"Release {tag_info.tag_name()}")

    print("Creating tag...")
    gh.tag(tag_info.tag_name(), f"Release {tag_info.tag_name()}")

    print(f"Done! Release {tag_info.tag_name()} tagged successfully.")


if __name__ == "__main__":
    main()
