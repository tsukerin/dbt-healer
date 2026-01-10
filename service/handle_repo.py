import subprocess
import sys
from pathlib import Path

def trigger(repo, commit_hash, dbt_path):
    workdir = Path(f"/tmp/{dbt_path}")
    workdir.mkdir(parents=True, exist_ok=True)

    repo_name = repo.split("/")[-1].replace(".git", "")
    repo_dir = workdir / repo_name

    if not repo_dir.exists():
        subprocess.run(
            ["git", "clone", "--depth", "1", repo],
            cwd=workdir,
            check=True
        )

    subprocess.run(
        ["git", "fetch", "origin", commit_hash],
        cwd=repo_dir,
        check=True
    )

    subprocess.run(
        ["git", "checkout", commit_hash],
        cwd=repo_dir,
        check=True
    )

    dbt_proj = repo_dir / dbt_path

    if not dbt_proj.exists():
        raise RuntimeError(f"DBT project not found at {dbt_proj}")

    print("dbt project is here:", dbt_proj)
