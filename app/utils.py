from pathlib import Path
from git import Repo
from typing import BinaryIO
import subprocess
import logging

from common.config import get_config

config = get_config()

def scan_hashes() -> None:
    """Scan dbt log for error hashes and store them in a separate file."""
    with config.logs_file.open('r', encoding='utf-8') as err_:
        err_lines = err_.read().splitlines(True)

    if not config.dbt_log.exists():
        return

    with config.logs_file.open('a', encoding='utf-8') as err:
        with config.dbt_log.open('r', encoding='utf-8') as f:
            for line in f:
                if '=' * 30 in line and '|' in line:
                    h = line.split('|')[1].replace('=', '').strip() + '\n'
                    if h not in err_lines:
                        err.write(h)

def get_context_log() -> str:
    """Retrieve the context log based on the last stored error hash."""
    with config.logs_file.open('r', encoding='utf-8') as err:
        lines = err.read().splitlines(True)

    if not lines:
        return []

    last_hash = lines[-1].strip()

    if not config.dbt_log.exists():
        return []

    is_found = False
    context_log = []
    
    with config.dbt_log.open('r', encoding='utf-8') as f:
        for line in f:
            if last_hash in line:
                is_found = True
            if is_found and line.strip():
                context_log.append(line.strip())
    
    return '\n'.join(context_log)

def get_file_context(files: list[str] | str) -> str:
    sources = []

    if isinstance(files, str):
        files = [files]

    for file in files:
        raw = Path(file)
        if raw.is_absolute() or raw.exists():
            paths = [raw]
        else:
            try:
                paths = list(config.repo_root.rglob(file))
            except ValueError:
                paths = [raw]

        for path in paths:
            if 'target' not in path.parts and path.is_file():
                try:
                    with open(path, encoding="utf-8") as f:
                        text = f.read()
                except UnicodeDecodeError:
                    with open(path, encoding="utf-8", errors="replace") as f:
                        text = f.read()
                
                diff = subprocess.run(["git", "diff", "HEAD^", str(path)], text=True, capture_output=True).stdout
                sources.append(f'SOURCE OF {str(path)}: {text} \n---\n FILE DIFF: {diff}')

    return '\n'.join(sources)

def get_changed_files(path: Path | None = None, mode: str = 'debug') -> list[Path]:
    if path is None:
        path = config.repo_root

    repo = Repo(path)
    diff = repo.head.commit.diff(None)
    origin = repo.remotes.origin
    origin.fetch()

    changed = []

    if mode == 'debug':
        for item in diff:
            file = item.a_path
            print(f"Changed file: {file}")
            changed.append(Path(item.a_path).stem) if '.sql' in file else None

    elif mode == 'prod':
        diff_index = repo.commit("HEAD").diff("origin/master")

        for d in diff_index:
            changed.append(Path(d.a_path).stem) if d.change_type != 'D' and '.sql' in d.a_path else None

    if changed:
        logging.info("Changed files detected: " + ", ".join([str(file) for file in changed]))

    repo.close()
    
    return changed

def get_instruction(name: str) -> str:
    """
    Get available instructions:
    - handle_solution
    - handle_error_file
    """
    path = Path(__file__).resolve().parents[1] / "common" / "instructions"

    with open(path / f"{name}.md", mode="r", encoding="utf-8") as f:
        return f.read()

def clone_repo_from_ci(repo: str, commit_hash: str, dbt_path: str, log_file: BinaryIO) -> None:
    workdir = Path(Path.home() / ".failedrepo")
    workdir.mkdir(parents=True, exist_ok=True)

    repo_name = repo.split("/")[-1].replace(".git", "")
    repo_dir = workdir / repo_name

    auth_repo = repo.replace("https://", f"https://{config.github_name}:{config.github_token}@")

    if not (repo_dir / ".git").exists():
        subprocess.run(["git", "clone", "--depth", "1", auth_repo], cwd=workdir, check=True)
    
    subprocess.run(["git", "fetch", "origin"], cwd=repo_dir, check=True)
    subprocess.run(["git", "checkout", commit_hash], cwd=repo_dir, check=True)

    config.logs_file.parent.mkdir(parents=True, exist_ok=True)
    config.logs_file.touch(exist_ok=True)
    config.dbt_log.parent.mkdir(parents=True, exist_ok=True)

    with open(config.dbt_log, "wb") as f:
        f.write(log_file.file.read())

    dbt_proj = repo_dir / dbt_path

    if not dbt_proj.exists():
        raise RuntimeError(f"DBT project not found at {dbt_proj}")
