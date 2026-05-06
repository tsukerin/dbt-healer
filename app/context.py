from pathlib import Path
import json
import logging
import re
import subprocess

from app import utils
from app.rag import (
    extract_error_signals,
    extract_macro_calls,
    relevance_score,
    structured_sql_context,
)

MODEL_TYPES = {"model", "snapshot", "seed"}
MAX_DIAGNOSTIC_MODELS = 6
MAX_IMPACT_MODELS = 5
MAX_UPSTREAM_DEPTH = 2
MAX_DOWNSTREAM_DEPTH = 1
COMPILED_SQL_RE = re.compile(r"compiled code at\s+(?P<path>target/[^\s]+\.sql)", re.IGNORECASE)
ERROR_LOCATION_RE = re.compile(r"\b(?:line|LINE)\s+\d+|\[\d+:\d+\]")


def _read_source_text(path: Path) -> str:
    """Read source file with utf-8 fallback."""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _relative_to_failed_repo(path: Path) -> str:
    """Return path relative to failed repository when possible."""
    try:
        return path.relative_to(utils.get_failed_repo_path()).as_posix()
    except (RuntimeError, ValueError):
        return path.as_posix()


def _git_revision_exists(repo_path: Path, revision: str) -> bool:
    """Check whether git revision exists in repository."""
    return subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", revision],
        cwd=repo_path,
        capture_output=True,
    ).returncode == 0


def _get_file_diff(path: Path) -> str:
    """Return diff for source file against available base revision."""
    try:
        failed_repo_path = utils.get_failed_repo_path()
        relative_path = path.relative_to(failed_repo_path)
    except (RuntimeError, ValueError):
        return "NO_DIFF"

    base_revision = next(
        (
            revision
            for revision in ("HEAD^", f"origin/{utils.config.base_branch}")
            if _git_revision_exists(failed_repo_path, revision)
        ),
        None,
    )
    if not base_revision:
        return "NO_DIFF"

    result = subprocess.run(
        ["git", "diff", base_revision, "--", relative_path.as_posix()],
        cwd=failed_repo_path,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        logging.warning("Unable to get git diff for %s: %s", relative_path, result.stderr.strip())
        return "NO_DIFF"

    return result.stdout.strip() or "NO_DIFF"


def _error_log() -> str:
    log = utils.get_context_log()
    if isinstance(log, list):
        return "\n".join(log)
    return str(log or "")


def _should_shrink_context() -> bool:
    return str(getattr(utils.config, "ai_provider", "") or "").lower() == "ollama"


def _context_text(source: str, signals: set[str], query: str) -> str:
    if not _should_shrink_context():
        return source
    return structured_sql_context(source, signals, query=query)


def _read_manifest(failed_repo_path: Path) -> dict:
    manifest_path = failed_repo_path / "target" / "manifest.json"
    if not manifest_path.exists():
        logging.warning("dbt manifest not found at %s; skipping lineage context.", manifest_path)
        return {}

    try:
        with manifest_path.open(mode="r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Unable to read dbt manifest at %s: %s", manifest_path, exc)
        return {}


def _find_node(manifest: dict, file: str) -> tuple[str | None, dict | None]:
    normalized = file.replace("\\", "/")
    stem = Path(file).stem

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") not in MODEL_TYPES or not node.get("original_file_path"):
            continue
        path = node.get("original_file_path", "").replace("\\", "/")
        if normalized == path or stem in {node.get("name"), Path(path).stem}:
            return node_id, node

    return None, None


def _model_source(failed_repo_path: Path, node: dict) -> str:
    return _read_source_text(failed_repo_path / node["original_file_path"])


def _upstream_model_ids(manifest: dict, node_id: str) -> list[str]:
    nodes = manifest.get("nodes", {})
    return [
        parent_id
        for parent_id in nodes.get(node_id, {}).get("depends_on", {}).get("nodes", [])
        if nodes.get(parent_id, {}).get("resource_type") in MODEL_TYPES
    ]


def _downstream_model_ids(manifest: dict, node_id: str) -> list[str]:
    nodes = manifest.get("nodes", {})
    return [
        child_id
        for child_id in manifest.get("child_map", {}).get(node_id, [])
        if nodes.get(child_id, {}).get("resource_type") in MODEL_TYPES
    ]


def _ranked_model_ids(failed_repo_path: Path, manifest: dict, node_ids: list[str], signals: set[str]) -> list[str]:
    return sorted(
        node_ids,
        key=lambda node_id: relevance_score(
            _model_source(failed_repo_path, manifest["nodes"][node_id]),
            manifest["nodes"][node_id].get("name", ""),
            manifest["nodes"][node_id].get("original_file_path", ""),
            signals,
        ),
        reverse=True,
    )


def _diagnostic_model_ids(
    failed_repo_path: Path,
    manifest: dict,
    root_id: str,
    signals: set[str],
) -> list[tuple[str, int]]:
    selected = []
    seen = set()
    direct = _ranked_model_ids(failed_repo_path, manifest, _upstream_model_ids(manifest, root_id), signals)

    for node_id in direct:
        if len(selected) >= MAX_DIAGNOSTIC_MODELS:
            break
        selected.append((node_id, 1))
        seen.add(node_id)

    for parent_id, depth in list(selected):
        if depth >= MAX_UPSTREAM_DEPTH:
            continue
        for node_id in _ranked_model_ids(failed_repo_path, manifest, _upstream_model_ids(manifest, parent_id), signals):
            if len(selected) >= MAX_DIAGNOSTIC_MODELS:
                break
            if node_id in seen:
                continue
            node = manifest["nodes"][node_id]
            if relevance_score(_model_source(failed_repo_path, node), node["name"], node["original_file_path"], signals) > 0:
                selected.append((node_id, depth + 1))
                seen.add(node_id)

    return selected


def _node_context(
    failed_repo_path: Path,
    manifest: dict,
    node_id: str,
    signals: set[str],
    label: str,
    depth: int,
    query: str = "",
) -> str:
    node = manifest["nodes"][node_id]
    source = _model_source(failed_repo_path, node)
    body = _context_text(source, signals, query)
    return (
        f"<{label} name=\"{node['name']}\" path=\"{node['original_file_path']}\" depth=\"{depth}\">\n"
        f"{body}\n"
        f"</{label}>"
    )


def _definition_text(path: Path, signals: set[str], query: str) -> str:
    text = _read_source_text(path)
    if path.suffix.lower() not in {".yml", ".yaml"}:
        return _context_text(text, signals, query)
    if not _should_shrink_context():
        return text

    lines = [
        line.rstrip()
        for line in text.splitlines()
        if any(signal and signal in line.lower() for signal in signals)
    ]
    compact = "\n".join(lines) or text
    return compact[:1600].strip()


def _macro_contexts(failed_repo_path: Path, manifest: dict, node: dict, source: str, query: str) -> list[str]:
    macro_ids = list(node.get("depends_on", {}).get("macros", []))
    macro_names = extract_macro_calls(source)

    for macro_id, macro in manifest.get("macros", {}).items():
        if macro.get("name") in macro_names:
            macro_ids.append(macro_id)

    contexts = []
    for macro_id in list(dict.fromkeys(macro_ids))[:4]:
        macro = manifest.get("macros", {}).get(macro_id)
        path = macro.get("original_file_path") if macro else None
        if not path:
            continue
        text = _context_text(_read_source_text(failed_repo_path / path), macro_names, query)
        contexts.append(f"<MACRO_CONTEXT name=\"{macro.get('name')}\" path=\"{path}\">\n{text}\n</MACRO_CONTEXT>")
    return contexts


def _definition_contexts(
    failed_repo_path: Path,
    manifest: dict,
    node: dict,
    error_log: str,
    signals: set[str],
) -> list[str]:
    contexts = []

    for source_id in node.get("depends_on", {}).get("nodes", []):
        source = manifest.get("sources", {}).get(source_id)
        path = source.get("original_file_path") if source else None
        if path:
            text = _definition_text(failed_repo_path / path, signals, error_log)
            contexts.append(f"<SOURCE_DEFINITION name=\"{source.get('name')}\" path=\"{path}\">\n{text}\n</SOURCE_DEFINITION>")

    for match in utils.exp.DBT_SOURCE_PATH_RE.finditer(error_log):
        path = utils._normalize_dbt_source_path(match.group("path"))
        if path and path.endswith((".yml", ".yaml")) and path != node.get("original_file_path"):
            text = _definition_text(failed_repo_path / path, signals, error_log)
            if text:
                contexts.append(f"<SCHEMA_DEFINITION path=\"{path}\">\n{text}\n</SCHEMA_DEFINITION>")

    return list(dict.fromkeys(contexts))[:3]


def _compiled_sql_context(failed_repo_path: Path, error_log: str) -> str:
    match = COMPILED_SQL_RE.search(error_log)
    if not match:
        return ""

    path = match.group("path")
    text = _read_source_text(failed_repo_path / path)
    if not text:
        return ""

    location = ERROR_LOCATION_RE.search(error_log)
    location_text = f' location="{location.group(0)}"' if location else ""
    if _should_shrink_context():
        text = text[:3000]
    return f"<COMPILED_SQL path=\"{path}\"{location_text}>\n{text}\n</COMPILED_SQL>"


def parse_lineage_models(model: str) -> dict[str, str]:
    """Collect selective upstream diagnostic context from manifest."""
    if "macros" in Path(model).parts:
        return {}

    failed_repo_path = utils.get_failed_repo_path()
    manifest = _read_manifest(failed_repo_path)
    node_id, node = _find_node(manifest, model)
    if not node_id or not node:
        logging.warning("Model for %s not found in dbt manifest; skipping lineage context.", model)
        return {}

    error_log = _error_log()
    primary_source = _model_source(failed_repo_path, node)
    signals = extract_error_signals(error_log, primary_source)
    contexts = {}

    for upstream_id, depth in _diagnostic_model_ids(failed_repo_path, manifest, node_id, signals):
        upstream = manifest["nodes"][upstream_id]
        contexts[upstream["name"]] = _node_context(
            failed_repo_path,
            manifest,
            upstream_id,
            signals,
            "UPSTREAM_MODEL",
            depth,
            error_log,
        )

    for index, macro_context in enumerate(_macro_contexts(failed_repo_path, manifest, node, primary_source, error_log), start=1):
        contexts[f"macro_{index}"] = macro_context

    for index, definition_context in enumerate(
        _definition_contexts(failed_repo_path, manifest, node, error_log, signals),
        start=1,
    ):
        contexts[f"definition_{index}"] = definition_context

    return contexts


def get_impact_context(file: str) -> str:
    """Build direct downstream context for post-fix validation."""
    failed_repo_path = utils.get_failed_repo_path()
    manifest = _read_manifest(failed_repo_path)
    node_id, node = _find_node(manifest, file)
    if not node_id or not node:
        return ""

    primary_source = _model_source(failed_repo_path, node)
    error_log = _error_log()
    signals = extract_error_signals(error_log, primary_source)
    downstream_ids = _ranked_model_ids(
        failed_repo_path,
        manifest,
        _downstream_model_ids(manifest, node_id),
        signals,
    )[:MAX_IMPACT_MODELS]
    sections = [
        _node_context(
            failed_repo_path,
            manifest,
            downstream_id,
            signals,
            "DOWNSTREAM_MODEL",
            MAX_DOWNSTREAM_DEPTH,
            error_log,
        )
        for downstream_id in downstream_ids
    ]
    return "\n".join(sections)


def _file_path(file: str) -> Path:
    path = Path(file)
    if path.is_absolute():
        return path
    return utils.get_failed_repo_path() / path


def get_file_context(files: list[str] | str) -> str:
    """Build primary source and diagnostic context for files."""
    sources = []

    if isinstance(files, str):
        files = [files]

    for file in files:
        path = _file_path(file)
        if "target" in path.parts or not path.is_file():
            continue

        error_log = _error_log()
        relative_path = _relative_to_failed_repo(path)
        diagnostic_context = "\n".join(parse_lineage_models(file).values())

        sources.append(
            f"<PRIMARY_ERROR_MODEL path=\"{relative_path}\">\n"
            f"SOURCE OF {relative_path}:\n{_read_source_text(path)}\n"
            f"FILE DIFF:\n{_get_file_diff(path)}\n"
            f"{_compiled_sql_context(utils.get_failed_repo_path(), error_log)}\n"
            f"</PRIMARY_ERROR_MODEL>\n\n"
            f"<DIAGNOSTIC_CONTEXT>\n{diagnostic_context or 'NO_DIAGNOSTIC_CONTEXT'}\n</DIAGNOSTIC_CONTEXT>\n\n"
            f"<IMPACT_CONTEXT>\nDeferred until a candidate fix is generated; do not use downstream models in the first fix.\n</IMPACT_CONTEXT>"
        )

    return "\n".join(sources)
