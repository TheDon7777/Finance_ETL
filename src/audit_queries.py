# src/audit_queries.py
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def list_change_events(engine: Engine, limit: int = 50) -> pd.DataFrame:
    q = text("""
        SELECT
          change_event_id::text AS change_event_id,
          started_at,
          finished_at,
          status,
          actor,
          source_name,
          file_name,
          inserted_count,
          updated_count,
          unchanged_count,
          conflicted_count,
          rejected_count,
          notes
        FROM etl_change_events
        ORDER BY started_at DESC
        LIMIT :lim
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"lim": limit})


def get_row_changes(engine: Engine, change_event_id: str, limit: int = 200) -> pd.DataFrame:
    q = text("""
        SELECT
          created_at,
          table_name,
          pk,
          op,
          applied,
          conflict,
          conflict_reason,
          changed_columns
        FROM etl_row_changes
        WHERE change_event_id = CAST(:eid AS uuid)
        ORDER BY created_at DESC
        LIMIT :lim
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"eid": change_event_id, "lim": limit})


def get_conflicts(engine: Engine, change_event_id: str, limit: int = 200) -> pd.DataFrame:
    q = text("""
        SELECT
          created_at,
          table_name,
          pk,
          conflict_reason,
          conflict_columns,
          resolved,
          resolution,
          resolved_at
        FROM etl_conflicts
        WHERE change_event_id = CAST(:eid AS uuid)
        ORDER BY created_at DESC
        LIMIT :lim
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={"eid": change_event_id, "lim": limit})


def get_column_diffs(
    engine: Engine,
    change_event_id: str,
    table_name: str,
    column_name: str,
    limit: int = 20000,
) -> pd.DataFrame:
    """
    Return per-row before/after diffs for a specific changed column.

    Uses etl_row_changes as the source of truth and pulls:
      - pk
      - source_row_num (from db_after JSON if present)
      - before_value / after_value for the requested column

    Note: limit is a safety cap to avoid accidentally trying to render huge result sets.
    """
    q = text("""
        SELECT
          created_at,
          pk,
          NULLIF(db_after->>'source_row_num','')::int AS source_row_num,
          (db_before->> :col) AS before_value,
          (db_after->> :col)  AS after_value
        FROM etl_row_changes
        WHERE change_event_id = CAST(:eid AS uuid)
          AND table_name = :tbl
          AND applied = true
          AND op IN ('UPDATE','INSERT')
          AND changed_columns IS NOT NULL
          AND :col = ANY(changed_columns)
        ORDER BY source_row_num NULLS LAST, created_at ASC
        LIMIT :lim
    """)
    with engine.begin() as conn:
        return pd.read_sql(q, conn, params={
            "eid": change_event_id,
            "tbl": table_name,
            "col": column_name,
            "lim": limit,
        })
