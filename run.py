import logging
import github
from github import Github
import asyncio

from common.config import (
    LOGS_FILE, 
    GITHUB_TOKEN, 
    REPO_NAME, 
    BASE_BRANCH, 
    GITHUB_USERNAME,
)

from healer.pusher import (
    extract_solution_parts,
    build_repo_file_path,
    create_branch,
    update_file_in_branch,
    create_pull_request,
)

from healer.core import get_solution
from healer.utils import scan_hashes
from notifier.utils import notify_about_pr


async def main() -> None:
    """Orchestrate solution retrieval, commit, and PR creation."""
    scan_hashes()
    solution = get_solution()
    logging.info(solution)

    client = Github(auth=github.Auth.Token(GITHUB_TOKEN))
    repo = client.get_repo(f"{GITHUB_USERNAME}/{REPO_NAME}")

    branch_name = create_branch(repo, BASE_BRANCH)

    for part in extract_solution_parts(solution):
        solution_content, solution_file = part[0], part[1]
        file_path = build_repo_file_path(solution_file)

        try:
            print(f"Accessing file: {file_path}")
            update_file_in_branch(repo, file_path, solution_content, branch_name)
        except github.GithubException as exc:
            print(f"GitHub API error: {exc.status} - {exc.data.get('message', '')}")
            raise

    pr = create_pull_request(repo, branch_name, ', '.join([part[1] for part in extract_solution_parts(solution)]))
    print(pr.id)
    print("Pull request created successfully.")

    await notify_about_pr()


if __name__ == "__main__":
    LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOGS_FILE.touch(exist_ok=True)

    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())