from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote
import subprocess
import logging
import json
import re
from common.config import get_config

from app.dbt_exps import (
    DBT_SOURCE_DIRS,
    DBT_SOURCE_EXTENSIONS,
    DBT_NODE_RESOURCE_TYPES,
    DbtRegularExpressions
)

config = get_config()
exp = DbtRegularExpressions()

def get_failed_repo_path() -> Path:
    """Return checked-out dbt project path."""
    if not config.dbt_project_name:
        raise RuntimeError("DBT_PROJECT_NAME is not configured")

    failed_repo_path = config.repo_root / config.dbt_project_name
    if not failed_repo_path.exists():
        raise RuntimeError(f"DBT project not found at {failed_repo_path}")

    return failed_repo_path


def _dedupe(items: list[str]) -> list[str]:
    """Return non-empty items without duplicates."""
    return list(dict.fromkeys(item for item in items if item))


def _clean_log_text(log_text: str | list[str] | None) -> str:
    """Normalize log text and remove ANSI escapes."""
    if isinstance(log_text, list):
        log_text = "\n".join(log_text)
    return exp.ANSI_ESCAPE_RE.sub("", str(log_text or ""))


def _normalize_dbt_source_path(raw_path: str | None) -> str | None:
    """Normalize raw path to a dbt source path."""
    if not raw_path:
        return None

    path = raw_path.strip().strip("`'\".,;:()[]{}").replace("\\", "/")
    if not path.lower().endswith(DBT_SOURCE_EXTENSIONS):
        return None

    for source_dir in DBT_SOURCE_DIRS:
        marker = f"{source_dir}/"
        marker_index = path.find(marker)
        if marker_index >= 0:
            return path[marker_index:]

    if "/target/" in f"/{path}":
        return None

    return path


def _read_dbt_manifest() -> dict:
    """Read dbt manifest from failed repository."""
    try:
        manifest_path = get_failed_repo_path() / "target" / "manifest.json"
    except RuntimeError:
        return {}

    if not manifest_path.exists():
        return {}

    try:
        with open(manifest_path, mode="r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Unable to read dbt manifest at %s: %s", manifest_path, exc)
        return {}


def _resolve_manifest_source(resource_name: str | None) -> str | None:
    """Resolve dbt resource name through manifest metadata."""
    if not resource_name:
        return None

    name = resource_name.strip().strip("`'\".,;:()[]{}").split(".")[-1]
    if not name:
        return None

    manifest = _read_dbt_manifest()
    for node in manifest.get("nodes", {}).values():
        original_file_path = node.get("original_file_path")
        if node.get("resource_type") not in DBT_NODE_RESOURCE_TYPES or not original_file_path:
            continue

        identifiers = {
            str(node.get("name") or ""),
            str(node.get("alias") or ""),
            Path(original_file_path).stem,
        }
        if name in identifiers:
            return _normalize_dbt_source_path(original_file_path)

    for macro in manifest.get("macros", {}).values():
        original_file_path = macro.get("original_file_path")
        if not original_file_path:
            continue

        identifiers = {
            str(macro.get("name") or ""),
            Path(original_file_path).stem,
        }
        if name in identifiers:
            return _normalize_dbt_source_path(original_file_path)

    return None


def _resolve_test_failure_source(test_name: str | None, raw_path: str | None) -> str | None:
    """Resolve tested source file for dbt test failure."""
    if not test_name:
        return None

    name = test_name.strip().strip("`'\".,;:()[]{}").split(".")[-1]
    manifest = _read_dbt_manifest()
    for node_id, node in manifest.get("nodes", {}).items():
        identifiers = {str(node.get("name") or ""), str(node.get("alias") or "")}
        if node.get("resource_type") != "test":
            continue
        if (
            name not in identifiers
            and f".{name}." not in node_id
            and not node_id.endswith(f".{name}")
        ):
            continue
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_node = manifest.get("nodes", {}).get(dep_id)
            if dep_node and dep_node.get("resource_type") in ("model", "snapshot", "seed"):
                resolved = _normalize_dbt_source_path(dep_node.get("original_file_path"))
                if resolved:
                    return resolved

    try:
        failed_repo_path = get_failed_repo_path()
    except RuntimeError:
        failed_repo_path = None

    if failed_repo_path:
        candidates = sorted(
            (path for path in (failed_repo_path / "models").rglob("*.sql")),
            key=lambda path: len(path.stem),
            reverse=True,
        )
        for path in candidates:
            if f"_{path.stem}_" in f"_{name}_":
                return path.relative_to(failed_repo_path).as_posix()

    resolved_path = _normalize_dbt_source_path(raw_path)
    if resolved_path and (resolved_path.endswith(".sql") or resolved_path.startswith("tests/")):
        return resolved_path
    return None


def _resolve_source_file(resource_name: str | None) -> str | None:
    """Resolve dbt source file by resource name."""
    resolved = _resolve_manifest_source(resource_name)
    if resolved:
        return resolved

    if not resource_name:
        return None

    name = resource_name.strip().strip("`'\".,;:()[]{}").split(".")[-1]
    if not name:
        return None

    try:
        failed_repo_path = get_failed_repo_path()
    except RuntimeError:
        return None

    matches = sorted(
        path for path in failed_repo_path.rglob(f"{name}.sql")
        if "target" not in path.parts and "dbt_packages" not in path.parts
    )
    if not matches:
        return None

    return matches[0].relative_to(failed_repo_path).as_posix()


def _resolve_dbt_error_reference(
    resource_type: str | None,
    resource_name: str | None,
    raw_path: str | None,
) -> str | None:
    """Resolve dbt error reference to source file path."""
    if resource_type and resource_type.lower() == "test":
        return _resolve_test_failure_source(resource_name, raw_path)

    return _normalize_dbt_source_path(raw_path) or _resolve_source_file(resource_name)


def get_error_files_from_dbt_log(log_text: str | list[str] | None) -> list[str]:
    """Extract failing dbt source files from dbt's own error lines."""
    text = _clean_log_text(log_text)
    if not text:
        return []

    files = []
    unresolved_test_failure = False
    for pattern in exp.DBT_EXPLICIT_ERROR_PATTERNS:
        for match in pattern.finditer(text):
            resource = match.groupdict().get("resource")
            resolved = _resolve_dbt_error_reference(
                resource,
                match.groupdict().get("name"),
                match.groupdict().get("path"),
            )
            if resolved:
                files.append(resolved)
            elif resource and resource.lower() == "test":
                unresolved_test_failure = True

    if files or unresolved_test_failure:
        return _dedupe(files)

    for match in exp.DBT_SOURCE_PATH_RE.finditer(text):
        resolved = _normalize_dbt_source_path(match.group("path"))
        if resolved:
            files.append(resolved)

    if files:
        return _dedupe(files)

    for match in exp.DBT_STATUS_MODEL_RE.finditer(text):
        relation = match.group("relation")
        resolved = _resolve_source_file(relation.split(".")[-1])
        if resolved:
            files.append(resolved)

    for match in exp.DBT_NATURAL_MODEL_RE.finditer(text):
        resolved = _resolve_source_file(match.group("name"))
        if resolved:
            files.append(resolved)

    return _dedupe(files)

def scan_hashes() -> None:
    """Scan dbt log for error hashes and store them in a separate file."""
    with config.logs_file.open('r', encoding='utf-8') as err_:
        err_lines = err_.read().splitlines(True)

    if not config.uploaded_dbt_log.exists():
        return

    with config.logs_file.open('a', encoding='utf-8') as err:
        with config.uploaded_dbt_log.open('r', encoding='utf-8') as f:
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

    if not config.uploaded_dbt_log.exists():
        return []

    is_found = False
    context_log = []
    
    with config.uploaded_dbt_log.open('r', encoding='utf-8') as f:
        for line in f:
            if last_hash in line:
                is_found = True
            if is_found and line.strip():
                context_log.append(line.strip())
    
    return '\n'.join(context_log)


def get_instruction(name: str) -> str:
    """
    Get available instructions:
    - handle_solution
    """
    path = Path(__file__).resolve().parents[1] / "common" / "instructions"

    with open(path / f"{name}.md", mode="r", encoding="utf-8") as f:
        return f.read()


def has_dbt_dependencies(dbt_project_path: Path) -> bool:
    """Check whether dbt project declares package dependencies."""
    return any((dbt_project_path / name).exists() for name in ("packages.yml", "dependencies.yml"))


def prepare_dbt_metadata(dbt_project_path: Path) -> None:
    """Refresh dbt dependencies and manifest metadata."""
    if has_dbt_dependencies(dbt_project_path):
        try:
            subprocess.run(["dbt", "deps"], cwd=dbt_project_path, check=True)
        except subprocess.CalledProcessError as e:
            logging.warning(
                f"dbt deps failed; continuing without refreshed packages. "
                f"Lineage context may be unavailable. Error: {e}"
            )
    else:
        logging.info("No dbt package config found; skipping dbt deps.")

    try:
        subprocess.run(["dbt", "--show-all-deprecations", "parse"], cwd=dbt_project_path, check=True)
    except subprocess.CalledProcessError as e:
        logging.warning(
            f"dbt parse failed; continuing without manifest lineage context. Error: {e}"
        )


def _authenticated_repo_url(repo: str) -> str:
    """Return clone URL with configured token when available."""
    if not config.github_token or not repo.startswith("https://"):
        return repo

    token = quote(config.github_token, safe="")
    if config.git_platform.lower() == "gitlab":
        return repo.replace("https://", f"https://oauth2:{token}@")
    return repo.replace("https://", f"https://{config.github_name}:{token}@")


def clone_repo_from_ci(repo: str, commit_hash: str, dbt_path: str, log_file: BinaryIO) -> None:
    """Clone failed repository and store uploaded dbt log."""
    workdir = Path(Path.home() / ".failedrepo")
    workdir.mkdir(parents=True, exist_ok=True)

    repo_name = repo.split("/")[-1].replace(".git", "")
    repo_dir = workdir / repo_name

    auth_repo = _authenticated_repo_url(repo)

    if not (repo_dir / ".git").exists():
        subprocess.run(["git", "clone", "--depth", "1", auth_repo], cwd=workdir, check=True)
    
    subprocess.run(["git", "fetch", "origin"], cwd=repo_dir, check=True)
    subprocess.run(["git", "checkout", commit_hash], cwd=repo_dir, check=True)

    config.logs_file.parent.mkdir(parents=True, exist_ok=True)
    config.logs_file.touch(exist_ok=True)
    config.uploaded_dbt_log.parent.mkdir(parents=True, exist_ok=True)

    with open(config.uploaded_dbt_log, "wb") as f:
        f.write(log_file.file.read())

    failed_repo_path = repo_dir / dbt_path
    if not failed_repo_path.exists():
        raise RuntimeError(f"DBT project not found at {failed_repo_path}")

    prepare_dbt_metadata(failed_repo_path)
