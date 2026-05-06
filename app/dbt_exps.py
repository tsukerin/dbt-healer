import re

DBT_SOURCE_DIRS = ("models", "snapshots", "seeds", "analyses", "macros", "tests")
DBT_SOURCE_EXTENSIONS = (".sql", ".yml", ".yaml")
DBT_NODE_RESOURCE_TYPES = ("model", "snapshot", "seed", "macro")

SQL_STOP_WORDS = {
    "and", "as", "by", "case", "cast", "else", "end", "false", "from",
    "group", "having", "join", "left", "not", "null", "on", "or", "order",
    "right", "select", "then", "true", "when", "where", "with",
}

SQL_IDENTIFIER_RE = re.compile(r"\b[A-Za-z_][\w$]*\b")
SQL_QUOTED_IDENTIFIER_RE = re.compile(r"[`'\"]([A-Za-z_][\w$.]*|[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)[`'\"]")
SQL_ERROR_SIGNAL_RE = re.compile(
    r"\b(?:column|field|identifier|relation|table|model|macro|source|alias)\s+"
    r"[`'\"]?(?P<name>[A-Za-z_][\w$.]*)",
    re.IGNORECASE,
)
SQL_REF_RE = re.compile(r"\b(?:ref|source)\s*\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]+)['\"])?", re.IGNORECASE)
SQL_MACRO_CALL_RE = re.compile(r"{{\s*([A-Za-z_][\w.]*)\s*\(", re.IGNORECASE)
SQL_ALIAS_RE = re.compile(r"\bas\s+([A-Za-z_][\w$]*)\b", re.IGNORECASE)
SQL_CTE_RE = re.compile(r"(?:\bwith|,)\s+([A-Za-z_][\w$]*)\s+as\s*\(", re.IGNORECASE)
SQL_CONFIG_RE = re.compile(r"{{\s*config\s*\((.*?)\)\s*}}", re.IGNORECASE | re.DOTALL)


class DbtRegularExpressions:
    def _compile_dbt_log_pattern(pattern: str) -> re.Pattern[str]:
        """Compile dbt log pattern with shared flags."""
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
