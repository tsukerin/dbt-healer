from pathlib import Path
from git import Repo
from typing import BinaryIO
import subprocess
import logging
import json
import re
from common.config import get_config

config = get_config()

DBT_SOURCE_DIRS = ("models", "snapshots", "seeds", "analyses", "macros")
DBT_SOURCE_EXTENSIONS = (".sql", ".yml", ".yaml")
DBT_NODE_RESOURCE_TYPES = ("model", "snapshot", "seed", "macro")


def _compile_dbt_log_pattern(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.VERBOSE)


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
DBT_ERROR_RE = _compile_dbt_log_pattern(rf"""
    \b
    (?:Database|Compilation|Runtime|Parsing) \s+ Error \s+ in \s+
    (?:sql \s+)?
    (?P<resource>model|snapshot|seed|macro) \s+
    (?P<name>[\w.$-]+)
    (?: \s+ \( (?P<path> [^)\n]+ ) \) )?
""")
DBT_FAILURE_RE = _compile_dbt_log_pattern(rf"""
    \b Failure \s+ in \s+
    (?P<resource>model|test|snapshot|seed|macro) \s+
    (?P<name>[\w.$-]+)
    (?: \s+ \( (?P<path> [^)\n]+ ) \) )?
""")
DBT_STATUS_MODEL_RE = _compile_dbt_log_pattern(rf"""
    \b ERROR \b
    [^\n]*
    \b model \s+
    (?P<relation>[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)
""")
DBT_NATURAL_MODEL_RE = _compile_dbt_log_pattern(rf"""
    \b error \b
    [^\n]{{0,200}}
    \b (?:in|for|from) \s+
    (?:the \s+)?
    (?P<name>[A-Za-z_][\w$]*) \s+ (?:model|macro) \b
""")
DBT_SOURCE_PATH_RE = _compile_dbt_log_pattern(rf"""
    (?P<path>
        (?:[A-Za-z]:)?
        /?
        (?:[\w.@+ -]+/)*
        (?:{"|".join(map(re.escape, DBT_SOURCE_DIRS))})/
        [\w.@+ /-]+
        \.
        (?:{"|".join(re.escape(ext.lstrip(".")) for ext in DBT_SOURCE_EXTENSIONS)})
    )
""")
DBT_EXPLICIT_ERROR_PATTERNS = (DBT_ERROR_RE, DBT_FAILURE_RE)


def get_failed_repo_path() -> Path:
    if not config.dbt_project_name:
        raise RuntimeError("DBT_PROJECT_NAME is not configured")

    failed_repo_path = config.repo_root / config.dbt_project_name
    if not failed_repo_path.exists():
        raise RuntimeError(f"DBT project not found at {failed_repo_path}")

    return failed_repo_path


def _dedupe(items: list[str]) -> list[str]:
    return list(dict.fromkeys(item for item in items if item))


def _clean_log_text(log_text: str | list[str] | None) -> str:
    if isinstance(log_text, list):
        log_text = "\n".join(log_text)
    return ANSI_ESCAPE_RE.sub("", str(log_text or ""))


def _normalize_dbt_source_path(raw_path: str | None) -> str | None:
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


def _resolve_source_file(resource_name: str | None) -> str | None:
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


def _resolve_dbt_error_reference(model_name: str | None, raw_path: str | None) -> str | None:
    return _normalize_dbt_source_path(raw_path) or _resolve_source_file(model_name)


def get_error_files_from_dbt_log(log_text: str | list[str] | None) -> list[str]:
    """Extract failing dbt source files from dbt's own error lines."""
    text = _clean_log_text(log_text)
    if not text:
        return []

    files = []
    for pattern in DBT_EXPLICIT_ERROR_PATTERNS:
        for match in pattern.finditer(text):
            resolved = _resolve_dbt_error_reference(
                match.groupdict().get("name"),
                match.groupdict().get("path"),
            )
            if resolved:
                files.append(resolved)

    if files:
        return _dedupe(files)

    for match in DBT_SOURCE_PATH_RE.finditer(text):
        resolved = _normalize_dbt_source_path(match.group("path"))
        if resolved:
            files.append(resolved)

    if files:
        return _dedupe(files)

    for match in DBT_STATUS_MODEL_RE.finditer(text):
        relation = match.group("relation")
        resolved = _resolve_source_file(relation.split(".")[-1])
        if resolved:
            files.append(resolved)

    for match in DBT_NATURAL_MODEL_RE.finditer(text):
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


def _relative_to_failed_repo(path: Path) -> str:
    try:
        return path.relative_to(get_failed_repo_path()).as_posix()
    except ValueError:
        return path.as_posix()


def _get_file_diff(path: Path) -> str:
    try:
        failed_repo_path = get_failed_repo_path()
        relative_path = path.relative_to(failed_repo_path)
    except (RuntimeError, ValueError):
        return "NO_DIFF"

    result = subprocess.run(
        ["git", "diff", "HEAD^", "--", relative_path.as_posix()],
        cwd=failed_repo_path,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        logging.warning("Unable to get git diff for %s: %s", relative_path, result.stderr.strip())
        return "NO_DIFF"

    return result.stdout.strip() or "NO_DIFF"


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
                
                relative_path = _relative_to_failed_repo(path)
                diff = _get_file_diff(path)
                lineage_models = '\n'.join(f"CONTEXT OF {model}:\n{context}" for model, context in parse_lineage_models(file).items())
                
                sources.append(f'SOURCE OF {relative_path}: {text}\nFILE DIFF: {diff}\nLINEAGE_MODELS: {lineage_models}')

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
    """
    path = Path(__file__).resolve().parents[1] / "common" / "instructions"

    with open(path / f"{name}.md", mode="r", encoding="utf-8") as f:
        return f.read()


def has_dbt_dependencies(dbt_project_path: Path) -> bool:
    return any((dbt_project_path / name).exists() for name in ("packages.yml", "dependencies.yml"))


def prepare_dbt_metadata(dbt_project_path: Path) -> None:
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
    config.uploaded_dbt_log.parent.mkdir(parents=True, exist_ok=True)

    with open(config.uploaded_dbt_log, "wb") as f:
        f.write(log_file.file.read())

    failed_repo_path = repo_dir / dbt_path
    if not failed_repo_path.exists():
        raise RuntimeError(f"DBT project not found at {failed_repo_path}")

    prepare_dbt_metadata(failed_repo_path)


def _relevant_lineage_context(
    text: str,
    query: str,
    model_name: str,
    path: str,
    relationship: str,
    max_chars: int = 2600,
) -> str:
    if len(text) <= max_chars:
        return text

    try:
        from langchain_classic.text_splitter import RecursiveCharacterTextSplitter
        from langchain_community.vectorstores import FAISS
        from langchain_core.documents import Document
        from langchain_ollama import OllamaEmbeddings

        document = Document(
            page_content=f"SOURCE OF {path}:\n{text}",
            metadata={"model": model_name, "path": path, "relationship": relationship},
        )
        chunks = RecursiveCharacterTextSplitter(
            chunk_size=1200,
            chunk_overlap=120,
            separators=["\n\n", "\n", " ", ""],
        ).split_documents([document])
        embeddings = OllamaEmbeddings(model="embeddinggemma", base_url=config.ollama_host or None)
        retriever = FAISS.from_documents(chunks, embeddings).as_retriever(search_kwargs={"k": min(2, len(chunks))})
        excerpts = [doc.page_content.strip() for doc in retriever.invoke(query)]
    except Exception as e:
        logging.warning(f"LangChain lineage retrieval failed; using plain truncation: {e}")
        excerpts = [text[:max_chars]]

    return (
        f"[lineage model truncated from {len(text)} chars with LangChain retrieval]\n"
        + "\n\n--- excerpt ---\n\n".join(excerpts)
    )


def parse_lineage_models(model: str) -> dict[str, str]:
    failed_repo_path = get_failed_repo_path()
    manifest_path = failed_repo_path / "target" / "manifest.json"
    if not manifest_path.exists():
        logging.warning("dbt manifest not found at %s; skipping lineage context.", manifest_path)
        return {}

    try:
        with open(manifest_path, mode="r") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Unable to read dbt manifest at %s: %s", manifest_path, exc)
        return {}

    model_name = Path(model).stem
    model_id = f"model.{config.dbt_project_name}.{model_name}"
    node = manifest.get("nodes", {}).get(model_id)
    if not node:
        logging.warning(f"Model {model_id} not found in dbt manifest; skipping lineage context.")
        return {}

    upstream = node["depends_on"]["nodes"]
    child_map = manifest["child_map"]
    downstream = child_map.get(model_id, [])

    failed_model_path = failed_repo_path / node["original_file_path"]
    try:
        failed_model_source = failed_model_path.read_text(encoding="utf-8")
    except OSError:
        failed_model_source = ""

    query = "\n".join((get_context_log(), failed_model_source))
    lineage = [(node_id, "upstream")for node_id in upstream] + [(node_id, "downstream")for node_id in downstream if node_id not in upstream]
    context_models = {}

    for node_id, relationship in lineage:
        node = manifest["nodes"].get(node_id)
        if node and node["resource_type"] == "model":
            file_path = failed_repo_path / node["original_file_path"]
            try:
                text = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                text = file_path.read_text(encoding="utf-8", errors="replace")

            context_models[node["name"]] = (
                f"relationship: {relationship}\n"
                f"path: {node['original_file_path']}\n"
                f"{_relevant_lineage_context(text, query, node['name'], node['original_file_path'], relationship)}"
            )

    return context_models

def relevant_context_lineage_models(context_models: dict[str, str]):
    pass
