import re

DBT_SOURCE_DIRS = ("models", "snapshots", "seeds", "analyses", "macros", "tests")
DBT_SOURCE_EXTENSIONS = (".sql", ".yml", ".yaml")
DBT_NODE_RESOURCE_TYPES = ("model", "snapshot", "seed", "macro")

class DbtRegularExpressions:
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