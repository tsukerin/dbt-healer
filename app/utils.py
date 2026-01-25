from pathlib import Path
from git import Repo
from common.config import REPO_ROOT, LOGS_FILE, DBT_LOG
import logging

def scan_hashes() -> None:
    """Scan dbt log for error hashes and store them in a separate file."""
    with LOGS_FILE.open('r', encoding='utf-8') as err_:
        err_lines = err_.read().splitlines(True)

    if not DBT_LOG.exists():
        return

    with LOGS_FILE.open('a', encoding='utf-8') as err:
        with DBT_LOG.open('r', encoding='utf-8') as f:
            for line in f:
                if '=' * 30 in line and '|' in line:
                    h = line.split('|')[1].replace('=', '').strip() + '\n'
                    if h not in err_lines:
                        err.write(h)

def get_context_log() -> str:
    """Retrieve the context log based on the last stored error hash."""
    with LOGS_FILE.open('r', encoding='utf-8') as err:
        lines = err.read().splitlines(True)

    if not lines:
        return []

    last_hash = lines[-1].strip()

    if not DBT_LOG.exists():
        return []

    is_found = False
    context_log = []
    
    with DBT_LOG.open('r', encoding='utf-8') as f:
        for line in f:
            if last_hash in line:
                is_found = True
            if is_found and line.strip():
                context_log.append(line.strip())
    
    return '\n'.join(context_log)

def get_file_context(files: list[str]) -> str:
    path = None
    sources = []

    for file in files:
        for path in Path('.').rglob(file):
            if 'target' not in path.parts and path.is_file():
                with open(path, encoding="utf-8") as f:
                    sources.append(f'SOURCE OF {str(path)}: {f.read()}')

    return '\n'.join(sources)

def get_changed_files(path: Path=REPO_ROOT, mode: str='debug') -> list[Path]:
    repo = Repo(path)
    diff = repo.head.commit.diff(None)
    origin = repo.remotes.origin
    origin.fetch()

    changed = []

    if mode == 'debug':
        for item in diff:
            file = item.a_path
            print(f"Changed file: {file}")

            changed.append(Path(item.a_path).stem) if 'sql' in file else None

    elif mode == 'prod':
        diff_index = repo.commit("HEAD").diff("origin/master")

        for d in diff_index:
            changed.append(Path(d.a_path).stem) if d.change_type != 'D' else None

    if changed:
        logging.info("Changed files detected: " + ", ".join([str(file) for file in changed]))

    repo.close()
    
    return changed

def get_instruction(name: str) -> str:
    """
    Get available insturctions:
    - handle_solution
    - handle_error_file
    """
    path = Path(__file__).resolve().parents[1] / "common" / "instructions"

    with open(path / f"{name}.md", mode="r", encoding="utf-8") as f:
        return ' '.join(f.readlines())