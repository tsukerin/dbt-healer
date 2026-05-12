from pathlib import Path
from datetime import datetime
import re
import logging
import os
from urllib.parse import quote

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

import github
import requests

from common.config import get_config

config = get_config()


def extract_solution_parts(solution: str) -> list[tuple[str, str]]:
    """Parse solution content and target file path."""
    solution_parts = []

    for idx, sol in enumerate(solution.split("----")):
        content_match = re.search(r"<solution>(.*?)</solution>", sol, re.DOTALL)
        file_match = re.search(r"<file>(.*?)</file>", sol, re.DOTALL)

        if not content_match or not file_match:
            logging.warning("Solution part №%s must contain <solution> and <file> blocks", idx + 1)
            continue

        solution_parts.append((content_match.group(1), file_match.group(1)))

    return solution_parts


def build_repo_file_path(raw_path: str) -> str:
    """Normalize path to repository format."""
    normalized = raw_path.strip().replace("\\", "/")

    if config.dbt_project_name and normalized.startswith(config.dbt_project_name):
        return normalized

    return str(Path(config.dbt_project_name) / normalized).replace("\\", "/")


def solution_files(solution_parts: list[tuple[str, str]]) -> str:
    """Return printable target file list."""
    return ", ".join(part[1].strip() for part in solution_parts)


def create_branch(repo: github.Repository.Repository, base_branch: str) -> str:
    """Create feature branch from base."""
    branch_name = f"feature/healer_fix_patch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    base_ref = repo.get_git_ref(f"heads/{base_branch}")
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_ref.object.sha)
    return branch_name


def update_file_in_branch(
    repo: github.Repository.Repository,
    file_path: str,
    content: str,
    branch: str,
) -> None:
    """Update target file in specified branch."""
    current = repo.get_contents(file_path, ref=branch)
    repo.update_file(
        path=current.path,
        message=(
            f"Edited {Path(file_path).name} "
            f"[Auto PR by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}]"
        ),
        content=content,
        sha=current.sha,
        branch=branch,
    )


def create_pull_request(
    repo: github.Repository.Repository,
    branch: str,
    solution_file: str,
) -> github.PullRequest.PullRequest:
    """Open pull request for the generated patch."""
    return repo.create_pull(
        title=f"Auto pull request by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}",
        body=f"fix {solution_file}",
        head=branch,
        base=config.base_branch,
    )


def _gitlab_project() -> tuple[str, str]:
    """Return GitLab API base URL and encoded project path."""
    repo_link = config.github_repo_link.strip().removesuffix(".git").rstrip("/")
    if repo_link.startswith("git@"):
        host, project_path = repo_link.removeprefix("git@").split(":", 1)
    else:
        match = re.match(r"^(?:https?://)?(?P<host>[^/\s]+)/(?P<path>.+)$", repo_link)
        if not match:
            raise ValueError(f"Unsupported GitLab repository link: {config.github_repo_link}")
        host, project_path = match.group("host"), match.group("path")

    return f"https://{host}/api/v4", quote(project_path.strip("/"), safe="")


def _gitlab_request(method: str, path: str, **kwargs) -> dict:
    """Call GitLab API with configured token."""
    api_base, project_path = _gitlab_project()
    response = requests.request(
        method,
        f"{api_base}/projects/{project_path}{path}",
        headers={"PRIVATE-TOKEN": config.github_token},
        timeout=120,
        **kwargs,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = response.text.strip()
        if detail:
            raise RuntimeError(f"GitLab API request failed: {detail}") from exc
        raise
    return response.json() if response.text else {}


def create_gitlab_branch(base_branch: str) -> str:
    """Create GitLab feature branch from base."""
    branch_name = f"feature/healer_fix_patch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _gitlab_request("POST", "/repository/branches", data={"branch": branch_name, "ref": base_branch})
    return branch_name


def update_file_in_gitlab_branch(file_path: str, content: str, branch: str) -> None:
    """Update target file in GitLab branch."""
    encoded_path = quote(file_path, safe="")
    _gitlab_request(
        "PUT",
        f"/repository/files/{encoded_path}",
        data={
            "branch": branch,
            "content": content,
            "commit_message": (
                f"Edited {Path(file_path).name} "
                f"[Auto PR by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}]"
            ),
        },
    )


def create_gitlab_merge_request(branch: str, solution_file: str) -> dict:
    """Open GitLab merge request for the generated patch."""
    return _gitlab_request(
        "POST",
        "/merge_requests",
        data={
            "source_branch": branch,
            "target_branch": config.base_branch,
            "title": f"Auto merge request by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "description": f"fix {solution_file}",
        },
    )
