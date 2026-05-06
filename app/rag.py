import logging
import re
from pathlib import Path

from common.config import get_config
from app.dbt_exps import (
    SQL_ALIAS_RE,
    SQL_CONFIG_RE,
    SQL_CTE_RE,
    SQL_ERROR_SIGNAL_RE,
    SQL_IDENTIFIER_RE,
    SQL_MACRO_CALL_RE,
    SQL_QUOTED_IDENTIFIER_RE,
    SQL_REF_RE,
    SQL_STOP_WORDS,
)


def _clean_symbol(value: str) -> str:
    return value.strip().strip("`'\".,;:()[]{}").split(".")[-1].lower()


def _symbols(values) -> set[str]:
    return {
        symbol
        for value in values
        for symbol in [_clean_symbol(value)]
        if symbol and symbol not in SQL_STOP_WORDS
    }


def extract_refs(text: str) -> set[str]:
    return _symbols(item for match in SQL_REF_RE.findall(text or "") for item in match if item)


def extract_macro_calls(text: str) -> set[str]:
    ignored = {"config", "ref", "source", "var", "env_var", "doc"}
    return {
        symbol
        for symbol in _symbols(SQL_MACRO_CALL_RE.findall(text or ""))
        if symbol not in ignored
    }


def extract_aliases(text: str) -> set[str]:
    return _symbols(SQL_ALIAS_RE.findall(text or ""))


def extract_error_signals(error_text: str, source: str = "") -> set[str]:
    """Extract model, macro, and column hints from dbt error text."""
    text = error_text or ""
    signals = set()
    signals |= _symbols(SQL_ERROR_SIGNAL_RE.findall(text))
    signals |= _symbols(SQL_QUOTED_IDENTIFIER_RE.findall(text))
    signals |= extract_refs(text)
    signals |= extract_macro_calls(text)
    signals |= _symbols(token for token in SQL_IDENTIFIER_RE.findall(text) if "_" in token or "$" in token)
    return signals | extract_refs(source)


def node_symbols(name: str, path: str, source: str) -> set[str]:
    """Return searchable symbols defined or used by a dbt node."""
    source = source or ""
    return (
        {_clean_symbol(name), _clean_symbol(Path(path).stem)}
        | extract_refs(source)
        | extract_macro_calls(source)
        | extract_aliases(source)
        | _symbols(SQL_CTE_RE.findall(source))
    )


def relevance_score(source: str, name: str, path: str, signals: set[str]) -> int:
    """Score a node by overlap with error symbols."""
    symbols = node_symbols(name, path, source)
    source_lower = (source or "").lower()
    return sum(3 if signal in symbols else int(bool(signal and signal in source_lower)) for signal in signals)


def _lines_matching(source: str, pattern: str) -> str:
    rx = re.compile(pattern, re.IGNORECASE)
    return "\n".join(line.rstrip() for line in source.splitlines() if rx.search(line))


def _final_select(source: str, max_chars: int) -> str:
    source = source or ""
    matches = list(re.finditer(r"\bselect\b", source, re.IGNORECASE))
    if not matches:
        return ""
    return source[matches[-1].start():][:max_chars].strip()


def _cap(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n[truncated]"


def _chunk_lines(source: str, max_chars: int = 900, overlap: int = 120) -> list[tuple[str, str, int]]:
    chunks = []
    start = 0
    index = 1
    while start < len(source):
        end = min(len(source), start + max_chars)
        chunks.append((f"RAW_CHUNK_{index}", source[start:end], 10))
        if end == len(source):
            break
        start = end - overlap
        index += 1
    return chunks


def _sql_chunks(source: str, signals: set[str], max_chars: int) -> list[tuple[str, str, int]]:
    source = source or ""
    config = "\n".join(match.group(0) for match in SQL_CONFIG_RE.finditer(source))
    refs = ", ".join(sorted(extract_refs(source)))
    macros = ", ".join(sorted(extract_macro_calls(source)))
    aliases = ", ".join(sorted(extract_aliases(source)))
    ctes = ", ".join(sorted(_symbols(SQL_CTE_RE.findall(source))))
    joins = _lines_matching(source, r"\bjoin\b|\bon\b")
    filters = _lines_matching(source, r"\bwhere\b|\bgroup\s+by\b|\bhaving\b|\bqualify\b")
    relevant = "\n".join(
        line.rstrip()
        for line in source.splitlines()
        if any(signal and signal in line.lower() for signal in signals)
    )

    chunks = [
        ("FINAL_SELECT", _final_select(source, max_chars // 2), 100),
        ("RELEVANT_LINES", relevant, 90),
        ("ALIASES", aliases, 80),
        ("REFS", refs, 75),
        ("MACRO_CALLS", macros, 75),
        ("CTES", ctes, 70),
        ("JOIN_CLAUSES", joins, 60),
        ("WHERE_GROUP_BY", filters, 60),
        ("CONFIG", config, 50),
    ]
    chunks += _chunk_lines(source)
    return [(title, body.strip(), priority) for title, body, priority in chunks if body and body.strip()]


def _lexical_retrieve(chunks: list[tuple[str, str, int]], signals: set[str], k: int) -> list[tuple[str, str]]:
    def score(item):
        _, body, priority = item
        body_lower = body.lower()
        matches = sum(1 for signal in signals if signal and signal in body_lower)
        return priority + matches * 20

    return [(title, body) for title, body, _ in sorted(chunks, key=score, reverse=True)[:k]]


def _vector_retrieve(chunks: list[tuple[str, str, int]], query: str, k: int) -> list[tuple[str, str]]:
    from langchain_community.vectorstores import FAISS
    from langchain_core.documents import Document
    from langchain_ollama import OllamaEmbeddings

    config = get_config()
    documents = [
        Document(
            page_content=body,
            metadata={"title": title},
        )
        for title, body, _ in chunks
    ]
    embeddings = OllamaEmbeddings(model="qwen3-embedding", base_url=config.ollama_host or None)
    docs = FAISS.from_documents(documents, embeddings).similarity_search(query, k=min(k, len(documents)))
    return [(doc.metadata["title"], doc.page_content) for doc in docs]


def _dedupe_chunks(chunks: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen = set()
    result = []
    for title, body in chunks:
        key = body.strip()
        if key and key not in seen:
            result.append((title, key))
            seen.add(key)
    return result


def retrieve_sql_context(
    source: str,
    query: str,
    signals: set[str],
    max_chars: int = 1600,
    k: int = 5,
    use_vector: bool = True,
) -> str:
    """Retrieve focused SQL chunks with vector search and lexical fallback."""
    source = source or ""
    chunks = _sql_chunks(source, signals, max_chars)
    if not chunks:
        return ""

    pinned = [(title, body) for title, body, _ in chunks if title in {"FINAL_SELECT", "RELEVANT_LINES"}][:2]
    if use_vector:
        try:
            selected = _vector_retrieve(chunks, query, k)
            retrieval = "vector"
        except Exception as exc:
            logging.warning("Vector RAG retrieval failed; using lexical SQL retrieval: %s", exc)
            selected = _lexical_retrieve(chunks, signals, k)
            retrieval = "lexical"
    else:
        selected = _lexical_retrieve(chunks, signals, k)
        retrieval = "lexical"

    sections = _dedupe_chunks(pinned + selected)
    text = "\n\n".join(f"{title}:\n{_cap(body, 700)}" for title, body in sections)
    header = f"[RAG {retrieval} retrieval from {len(source)} chars]\n"
    return header + _cap(text, max(0, max_chars - len(header)))


def structured_sql_context(
    source: str,
    signals: set[str],
    query: str = "",
    max_chars: int = 1600,
    use_vector: bool = True,
) -> str:
    """Return compact SQL context prioritized for dbt error repair."""
    source = source or ""
    if len(source) <= max_chars:
        return source

    query = query or " ".join(sorted(signals))
    return retrieve_sql_context(source, query, signals, max_chars=max_chars, use_vector=use_vector)
