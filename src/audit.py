# src/audit.py
from __future__ import annotations

import uuid
import json
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, List

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class ChangeEventContext:
    change_event_id: str
    actor: str
    source_name: str
    file_name: str
    dry_run: bool
    date_min: Optional[date] = None
    date_max: Optional[date] = None


def start_change_event(
    engine: Engine,
    *,
    actor: str,
    source_name: str,
    file_name: str,
    dry_run: bool,
    date_min: Optional[date] = None,
    date_max: Optional[date] = None,
) -> ChangeEventContext:
    change_event_id = str(uuid.uuid4())

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_change_events
                (change_event_id, status, actor, source_name, file_name, dry_run, date_min, date_max)
                VALUES (:id, 'RUNNING', :actor, :source, :file, :dry, :dmin, :dmax)
                """
            ),
            {
                "id": change_event_id,
                "actor": actor,
                "source": source_name,
                "file": file_name,
                "dry": bool(dry_run),
                "dmin": date_min,
                "dmax": date_max,
            },
        )

    return ChangeEventContext(
        change_event_id=change_event_id,
        actor=actor,
        source_name=source_name,
        file_name=file_name,
        dry_run=dry_run,
        date_min=date_min,
        date_max=date_max,
    )


def log_row_change(
    engine: Engine,
    *,
    change_event_id: str,
    table_name: str,
    pk: str,
    op: str,  # INSERT | UPDATE
    changed_columns: List[str],
    db_before: Optional[Dict[str, Any]],
    db_after: Dict[str, Any],
    applied: bool,
    conflict: bool = False,
    conflict_reason: Optional[str] = None,
) -> None:
    before_json = None if db_before is None else json.dumps(db_before, default=str)
    after_json = json.dumps(db_after, default=str)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_row_changes
                (change_event_id, table_name, pk, op, conflict, conflict_reason, applied,
                 changed_columns, db_before, db_after)
                VALUES
                (:eid, :t, :pk, :op, :conflict, :reason, :applied,
                 :cols, CAST(:before AS jsonb), CAST(:after AS jsonb))
                """
            ),
            {
                "eid": change_event_id,
                "t": table_name,
                "pk": pk,
                "op": op,
                "conflict": bool(conflict),
                "reason": conflict_reason,
                "applied": bool(applied),
                "cols": list(changed_columns),
                "before": before_json,
                "after": after_json,
            },
        )


def finish_change_event(
    engine: Engine,
    *,
    change_event_id: str,
    status: str,  # SUCCESS | CONFLICTS | FAILED | DRY_RUN
    inserted: int,
    updated: int,
    unchanged: int,
    conflicted: int,
    rejected: int,
    notes: Optional[str] = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl_change_events
                SET status = :status,
                    finished_at = now(),
                    inserted_count = :ins,
                    updated_count = :upd,
                    unchanged_count = :unch,
                    conflicted_count = :conf,
                    rejected_count = :rej,
                    notes = :notes
                WHERE change_event_id = :id
                """
            ),
            {
                "status": status,
                "ins": int(inserted),
                "upd": int(updated),
                "unch": int(unchanged),
                "conf": int(conflicted),
                "rej": int(rejected),
                "notes": notes,
                "id": change_event_id,
            },
        )
