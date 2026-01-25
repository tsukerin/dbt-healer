import logging
import sys
import github
from github import Github
import asyncio
import subprocess
from pathlib import Path

from common.config import (
    LOGS_FILE, 
    GITHUB_TOKEN, 
    REPO_NAME, 
    BASE_BRANCH, 
    GITHUB_USERNAME,
)

from app.push_repo import (
    extract_solution_parts,
    build_repo_file_path,
    create_branch,
    update_file_in_branch,
    create_pull_request,
)

from app.utils import scan_hashes, get_file_context, get_context_log
from notifier.utils import notify_about_pr
from app.providers import GoogleAI


async def main() -> None:
    """Orchestrate solution retrieval, commit, and PR creation."""
    scan_hashes()
    context = get_context_log()
    model = GoogleAI(context)
    solution = model.get_solution()
    logging.info(solution)

    client = Github(auth=github.Auth.Token(GITHUB_TOKEN))
    repo = client.get_repo(f"{GITHUB_USERNAME}/{REPO_NAME}")
    files = ', '.join([part[1].strip('\n') for part in extract_solution_parts(solution)])

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

    pr = create_pull_request(repo, branch_name, files)
    print(pr.id)
    print("Pull request created successfully.")

    await notify_about_pr(files)


if __name__ == "__main__":
    LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOGS_FILE.touch(exist_ok=True)

    workdir = Path(Path.home() / ".failedrepo")
    workdir.mkdir(parents=True, exist_ok=True)

    repo = sys.argv[1]
    commit_hash = sys.argv[2]
    dbt_path = sys.argv[3]

    repo_name = repo.split("/")[-1].replace(".git", "")
    repo_dir = workdir / repo_name

    if not repo_dir.exists():
        subprocess.run(
            ["git", "clone", "--depth", "1", repo],
            cwd=workdir,
        )

    subprocess.run(
        ["git", "fetch", "origin", commit_hash],
        cwd=repo_dir,
    )

    subprocess.run(
        ["git", "checkout", commit_hash],
        cwd=repo_dir,
    )

    dbt_proj = repo_dir / dbt_path

    if not dbt_proj.exists():
        raise RuntimeError(f"DBT project not found at {dbt_proj}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    asyncio.run(main())