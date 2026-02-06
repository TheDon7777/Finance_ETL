# src/ddl.py
from __future__ import annotations

from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError


SQL_STARTERS = (
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "GRANT",
    "REVOKE",
    "DO",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SET",
)


def _strip_sql_comments(sql: str) -> str:
    """
    Remove SQL comments from a statement chunk.
    Supports:
      - line comments: -- ...
      - block comments: /* ... */
    Not a full SQL parser; sufficient for schema files.
    """
    # Remove block comments /* ... */
    out_chars = []
    i = 0
    n = len(sql)
    in_block = False
    while i < n:
        if not in_block and i + 1 < n and sql[i] == "/" and sql[i + 1] == "*":
            in_block = True
            i += 2
            continue
        if in_block and i + 1 < n and sql[i] == "*" and sql[i + 1] == "/":
            in_block = False
            i += 2
            continue
        if not in_block:
            out_chars.append(sql[i])
        i += 1

    no_block = "".join(out_chars)

    # Remove line comments -- ...
    cleaned_lines = []
    for line in no_block.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        cleaned_lines.append(line)

    return "\n".join(cleaned_lines).strip()


def _trim_to_sql_start(stmt: str) -> str:
    """
    If a chunk begins with accidental plain text (not SQL), trim lines until the first
    line that looks like a SQL statement starter.

    This protects against users accidentally pasting notes into schema.sql.
    """
    lines = [ln.rstrip() for ln in stmt.splitlines()]
    # drop leading blank lines
    while lines and not lines[0].strip():
        lines.pop(0)

    # trim leading junk lines until we hit a SQL starter
    while lines:
        first = lines[0].lstrip()
        if not first:
            lines.pop(0)
            continue

        upper = first.upper()
        # handle optional leading parens/whitespace
        upper = upper.lstrip("(").lstrip()

        if any(upper.startswith(k) for k in SQL_STARTERS):
            break

        # not a SQL starter; drop this line as junk
        lines.pop(0)

    return "\n".join(lines).strip()


def apply_schema(engine: Engine, schema_sql_path: Path) -> None:
    """
    Best-effort schema apply.

    - Splits schema.sql on ';'
    - Strips comments
    - Trims accidental non-SQL leading lines
    - Skips empty statements
    - Skips privilege errors / already-exists errors
    """
    sql = schema_sql_path.read_text(encoding="utf-8", errors="ignore")

    raw_chunks = [s.strip() for s in sql.split(";")]

    with engine.begin() as conn:
        for chunk in raw_chunks:
            if not chunk:
                continue

            stmt = _strip_sql_comments(chunk)
            if not stmt:
                continue

            stmt = _trim_to_sql_start(stmt)
            if not stmt:
                continue

            try:
                conn.execute(text(stmt))
            except DBAPIError as e:
                msg = str(getattr(e, "orig", e)).lower()
                skippable = (
                    "insufficientprivilege" in msg
                    or "permission denied" in msg
                    or "must be owner" in msg
                    or "already exists" in msg
                )
                if skippable:
                    continue
                raise
