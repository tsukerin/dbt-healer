import logging
import os
import asyncio

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

import github
from github import Github

from common.config import get_config

from app.push_repo import (
    extract_solution_parts,
    build_repo_file_path,
    create_branch,
    create_gitlab_branch,
    create_gitlab_merge_request,
    update_file_in_branch,
    update_file_in_gitlab_branch,
    create_pull_request,
    solution_files,
)

from app.utils import scan_hashes, get_context_log
from app.provider_builder import build_provider
from notifier.utils import notify_about_pr

config = get_config()


def _create_gitlab_request(solution_parts: list[tuple[str, str]], files: str) -> str:
    branch_name = create_gitlab_branch(config.base_branch)
    for solution_content, solution_file in solution_parts:
        update_file_in_gitlab_branch(
            build_repo_file_path(solution_file),
            solution_content,
            branch_name,
        )
    merge_request = create_gitlab_merge_request(branch_name, files)
    return merge_request["web_url"]


def _create_github_request(solution_parts: list[tuple[str, str]], files: str) -> str:
    client = Github(auth=github.Auth.Token(config.github_token))
    repo = client.get_repo(f"{config.github_name}/{config.github_repo}")
    branch_name = create_branch(repo, config.base_branch)

    for solution_content, solution_file in solution_parts:
        file_path = build_repo_file_path(solution_file)
        try:
            logging.info("Accessing file: %s", file_path)
            update_file_in_branch(repo, file_path, solution_content, branch_name)
        except github.GithubException as exc:
            logging.info("GitHub API error: %s - %s", exc.status, exc.data.get("message", ""))
            raise

    pull_request = create_pull_request(repo, branch_name, files)
    return pull_request.html_url


async def main() -> None:
    """Orchestrate solution retrieval, commit, and PR creation."""
    scan_hashes()
    context = get_context_log()
    model = build_provider(
        ai_provider=config.ai_provider,
        context=context,
        ollama_type=config.ai_provider_type,
    )
    solution = model.get_solution()
    logging.info(solution)
    if not solution.strip():
        logging.warning("No solution generated; skipping pull request creation.")
        return

    solution_parts = [
        part
        for part in extract_solution_parts(solution)
        if part[0].strip() != "NO_FIX"
    ]
    if not solution_parts:
        logging.warning("No valid solution blocks generated; skipping pull request creation.")
        return

    files = solution_files(solution_parts)
    if config.git_platform.lower() == "gitlab":
        request_url = _create_gitlab_request(solution_parts, files)
    else:
        request_url = _create_github_request(solution_parts, files)

    logging.info("Pull/merge request created successfully.")

    await notify_about_pr(files, request_url)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    asyncio.run(main())
