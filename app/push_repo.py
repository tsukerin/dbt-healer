from pathlib import Path
from datetime import datetime
import re
import logging
import os

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

import github

from common.config import get_config

config = get_config()

def extract_solution_parts(solution: str) -> list[tuple[str, str]]:
    """Parse solution content and target file path."""
    solution_parts = []

    for idx, sol in enumerate(solution.split('----')):
        content_match = re.search(r"<solution>(.*?)</solution>", sol, re.DOTALL)
        file_match = re.search(r"<file>(.*?)</file>", sol, re.DOTALL)

        if not content_match or not file_match:
            logging.warning(f"Solution part №{idx+1} must contain <solution> and <file> blocks")
            continue

        solution_parts.append((content_match.group(1), file_match.group(1)))
    
    return solution_parts

def build_repo_file_path(raw_path: str) -> str:
    """Normalize path to repository format."""
    normalized = raw_path.strip().replace("\\", "/")

    if config.dbt_project_name and normalized.startswith(config.dbt_project_name):
        return normalized
    
    return str(Path(config.dbt_project_name) / normalized).replace("\\", "/")

def create_branch(repo: github.Repository.Repository, base_branch: str) -> str:
    """Create feature branch from base."""
    branch_name = f"feature/healer_fix_patch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    base_ref = repo.get_git_ref(f"heads/{base_branch}")
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_ref.object.sha)
    return branch_name


def update_file_in_branch(repo: github.Repository.Repository, file_path: str, content: str, branch: str) -> None:
    """Update target file in specified branch."""
    current = repo.get_contents(file_path, ref=branch)
    repo.update_file(
        path=current.path,
        message=f"Edited {Path(file_path).name} [Auto PR by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}]",
        content=content,
        sha=current.sha,
        branch=branch,
    )

def create_pull_request(repo: github.Repository.Repository, branch: str, solution_file: str) -> github.PullRequest.PullRequest:
    """Open pull request for the generated patch."""
    return repo.create_pull(
        title=f"Auto pull request by llm-healer {datetime.now().strftime('%Y%m%d_%H%M%S')}",
        body=f"fix {solution_file}",
        head=branch,
        base=config.base_branch,
    )
