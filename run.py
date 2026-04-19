import logging
import sys
import os

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

import github
from github import Github
import asyncio
import subprocess
from pathlib import Path

from common.config import get_config

from app.push_repo import (
    extract_solution_parts,
    build_repo_file_path,
    create_branch,
    update_file_in_branch,
    create_pull_request,
)

from app.utils import scan_hashes, get_file_context, get_context_log
from app.provider_builder import ProviderType, build_provider
from notifier.utils import notify_about_pr

config = get_config()


async def main() -> None:
    """Orchestrate solution retrieval, commit, and PR creation."""
    scan_hashes()
    context = get_context_log()
    model = build_provider(
        ai_provider=ProviderType(config.ai_provider),
        context=context,
        ollama_type=config.ai_provider_type,
    )
    solution = model.get_solution()
    logging.info(solution)

    owner, repo = config.github_name, config.github_repo

    client = Github(auth=github.Auth.Token(config.github_token))
    repo = client.get_repo(f"{owner}/{repo}")
    
    files = ', '.join([part[1].strip('\n') for part in extract_solution_parts(solution)])

    if files:
        branch_name = create_branch(repo, config.base_branch)

        for part in extract_solution_parts(solution):
            solution_content, solution_file = part[0], part[1]
            file_path = build_repo_file_path(solution_file)

            try:
                logging.info(f"Accessing file: {file_path}")
                update_file_in_branch(repo, file_path, solution_content, branch_name)
            except github.GithubException as exc:
                logging.info(f"GitHub API error: {exc.status} - {exc.data.get('message', '')}")
                raise

        create_pull_request(repo, branch_name, files)
        
        logging.info("Pull request created successfully.")

    await notify_about_pr(files)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    asyncio.run(main())
