import re
import subprocess
from pathlib import Path

from app import utils

REVIEW_BLOCK_RE = re.compile(r"<review>(.*?)</review>", re.DOTALL)
MAX_REVIEW_TOTAL_CHARS = 32000
MAX_REVIEW_FILE_DIFF_CHARS = 12000
MAX_REVIEW_SOURCE_CHARS = 8000
REVIEW_SOURCE_EXTENSIONS = {".sql", ".yml", ".yaml"}


def _git_revision_exists(repo_path, revision: str) -> bool:
    """Check whether git revision exists in repository."""
    return subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", revision],
        cwd=repo_path,
        capture_output=True,
    ).returncode == 0


def _base_revision(repo_path) -> str | None:
    """Return best available base revision for review diff."""
    return next(
        (
            revision
            for revision in (f"origin/{utils.config.base_branch}", "HEAD^")
            if _git_revision_exists(repo_path, revision)
        ),
        None,
    )


def _git_output(repo_path, args: list[str]) -> str:
    """Run git and return stdout."""
    result = subprocess.run(
        ["git", *args],
        cwd=repo_path,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def _truncate(text: str, max_chars: int) -> str:
    """Trim long review sections while keeping the truncation explicit."""
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n[truncated]"


def _changed_files(repo_path, base: str, pathspec: str | None = None) -> list[tuple[str, str]]:
    """Return changed file status and path from git diff."""
    args = ["diff", "--name-status", base, "--"]
    if pathspec:
        args.append(pathspec)
    output = _git_output(repo_path, args)
    files = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        files.append((parts[0], parts[-1]))
    return files


def _current_source(repo_path, file_path: str) -> str:
    """Return current file source for dbt SQL/YAML files."""
    path = repo_path / file_path
    if path.suffix.lower() not in REVIEW_SOURCE_EXTENSIONS or not path.is_file():
        return ""
    return _truncate(path.read_text(encoding="utf-8", errors="replace"), MAX_REVIEW_SOURCE_CHARS)


def _review_file_context(repo_path, base: str, status: str, file_path: str) -> str:
    """Build independent review context for one changed file."""
    diff = _git_output(repo_path, ["diff", "--unified=80", base, "--", file_path]) or "NO_DIFF"
    source = _current_source(repo_path, file_path)
    source_block = f"\n<CURRENT_FILE>\n{source}\n</CURRENT_FILE>" if source else ""
    return (
        f"<REVIEW_FILE path=\"{file_path}\" status=\"{status}\">\n"
        f"<FILE_DIFF>\n{_truncate(diff, MAX_REVIEW_FILE_DIFF_CHARS)}\n</FILE_DIFF>"
        f"{source_block}\n"
        f"</REVIEW_FILE>"
    )


def build_review_context() -> str:
    """Build compact review context from changed dbt project files."""
    dbt_project_path = utils.get_failed_repo_path()
    repo_path = Path(_git_output(dbt_project_path, ["rev-parse", "--show-toplevel"]))
    try:
        project_pathspec = dbt_project_path.relative_to(repo_path).as_posix()
    except ValueError:
        project_pathspec = None

    base = _base_revision(repo_path)
    if not base:
        return ""

    changed_files = _changed_files(repo_path, base, project_pathspec)
    changed_file_list = "\n".join(f"{status}\t{path}" for status, path in changed_files) or "NO_CHANGED_FILES"
    review_files = "\n\n".join(
        _review_file_context(repo_path, base, status, path)
        for status, path in changed_files
    ) or "NO_REVIEW_FILES"

    context = (
        f"<BASE_REVISION>{base}</BASE_REVISION>\n\n"
        f"<CHANGED_FILES>\n{changed_file_list}\n</CHANGED_FILES>\n\n"
        f"<REVIEW_FILES>\n{review_files}\n</REVIEW_FILES>"
    )
    return _truncate(context, MAX_REVIEW_TOTAL_CHARS)


def review_finding(response: str) -> str:
    """Return review finding text or empty string."""
    match = REVIEW_BLOCK_RE.search(response or "")
    if not match:
        return ""
    text = match.group(1).strip()
    if not text or text.rstrip(".").upper() == "NO_FINDINGS":
        return ""
    return text
