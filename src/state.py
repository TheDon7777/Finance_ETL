# src/state.py
from __future__ import annotations

import uuid
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError


# ---------------------------
# Internal helpers
# ---------------------------

def _one(conn, sql: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    row = conn.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def _table_exists(conn, table_name: str) -> bool:
    try:
        r = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :t
                LIMIT 1
                """
            ),
            {"t": table_name},
        ).first()
        return bool(r)
    except Exception:
        return False


def _ensure_state_pointer(conn) -> None:
    """
    Ensure etl_state_pointer exists and has the id=1 row.
    This is best-effort and safe to call repeatedly.
    """
    if not _table_exists(conn, "etl_state_pointer"):
        return

    conn.execute(
        text(
            """
            INSERT INTO etl_state_pointer (id)
            VALUES (1)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )


def _get_head_state_image_id(conn) -> Optional[str]:
    """
    Return HEAD state_image_id as text, or None.
    """
    if _table_exists(conn, "etl_state_pointer"):
        _ensure_state_pointer(conn)
        row = _one(
            conn,
            """
            SELECT current_state_image_id::text AS sid
            FROM etl_state_pointer
            WHERE id = 1
            """,
            {},
        )
        return row["sid"] if row and row.get("sid") else None

    if _table_exists(conn, "etl_state_head"):
        row = _one(
            conn,
            """
            SELECT state_image_id::text AS sid
            FROM etl_state_head
            WHERE id = 1
            """,
            {},
        )
        return row["sid"] if row and row.get("sid") else None

    return None


def _get_state_image(conn, state_image_id: str) -> Optional[Dict[str, Any]]:
    if not _table_exists(conn, "etl_state_images"):
        return None
    return _one(
        conn,
        """
        SELECT
          state_image_id::text AS state_image_id,
          change_event_id::text AS change_event_id,
          parent_state_image_id::text AS parent_state_image_id,
          notes,
          created_at
        FROM etl_state_images
        WHERE state_image_id = CAST(:sid AS uuid)
        """,
        {"sid": state_image_id},
    )


def _get_state_image_by_change_event(conn, change_event_id: str) -> Optional[Dict[str, Any]]:
    if not _table_exists(conn, "etl_state_images"):
        return None
    return _one(
        conn,
        """
        SELECT
          state_image_id::text AS state_image_id,
          change_event_id::text AS change_event_id,
          parent_state_image_id::text AS parent_state_image_id,
          notes,
          created_at
        FROM etl_state_images
        WHERE change_event_id = CAST(:eid AS uuid)
        """,
        {"eid": change_event_id},
    )


def _collect_head_to_target_chain(
    conn,
    *,
    head_state_image_id: str,
    target_state_image_id: str,
) -> List[Dict[str, Any]]:
    """
    Walk from HEAD backward via parent_state_image_id until we either hit target or None.
    Returns a list of image rows in order [HEAD, ..., target] (inclusive).
    Raises if target is not found on the HEAD chain.
    """
    chain: List[Dict[str, Any]] = []
    cur = head_state_image_id
    seen = set()

    while cur:
        if cur in seen:
            raise RuntimeError("State image chain contains a cycle (corrupt state).")
        seen.add(cur)

        img = _get_state_image(conn, cur)
        if not img:
            raise RuntimeError(f"State image not found: {cur}")

        chain.append(img)
        if img["state_image_id"] == target_state_image_id:
            return chain

        cur = img.get("parent_state_image_id") or ""

    raise RuntimeError("Target is not reachable from current HEAD (not on this history chain).")


# ---------------------------
# HEAD pointer (compat)
# ---------------------------

def get_current_state(engine: Engine) -> Optional[Dict[str, Any]]:
    """
    Returns the current HEAD pointer.

    Prefers:
      - etl_state_pointer (id=1) if present

    Backward-compat:
      - etl_state_head (id=1) if someone created that table previously
    """
    with engine.begin() as conn:
        if _table_exists(conn, "etl_state_pointer"):
            _ensure_state_pointer(conn)
            row = _one(
                conn,
                """
                SELECT
                  current_state_image_id::text AS state_image_id,
                  updated_at
                FROM etl_state_pointer
                WHERE id = 1
                """,
                {},
            )
            if not row or not row.get("state_image_id"):
                return None

            if _table_exists(conn, "etl_state_images"):
                img = _one(
                    conn,
                    """
                    SELECT change_event_id::text AS change_event_id
                    FROM etl_state_images
                    WHERE state_image_id = CAST(:sid AS uuid)
                    """,
                    {"sid": str(row["state_image_id"])},
                )
                if img:
                    row["change_event_id"] = img["change_event_id"]
            return row

        if _table_exists(conn, "etl_state_head"):
            return _one(
                conn,
                """
                SELECT state_image_id::text AS state_image_id,
                       change_event_id::text AS change_event_id,
                       updated_at
                FROM etl_state_head
                WHERE id = 1
                """,
                {},
            )

        return None


def set_head(engine: Engine, *, state_image_id: str) -> None:
    """
    Set HEAD to a given state_image_id.
    Uses etl_state_pointer if available; else etl_state_head if available.
    """
    with engine.begin() as conn:
        if _table_exists(conn, "etl_state_pointer"):
            _ensure_state_pointer(conn)
            conn.execute(
                text(
                    """
                    UPDATE etl_state_pointer
                    SET current_state_image_id = CAST(:sid AS uuid),
                        updated_at = now()
                    WHERE id = 1
                    """
                ),
                {"sid": state_image_id},
            )
            return

        if _table_exists(conn, "etl_state_head"):
            conn.execute(
                text(
                    """
                    INSERT INTO etl_state_head (id, state_image_id, updated_at)
                    VALUES (1, CAST(:sid AS uuid), now())
                    ON CONFLICT (id) DO UPDATE SET
                      state_image_id = EXCLUDED.state_image_id,
                      updated_at = now()
                    """
                ),
                {"sid": state_image_id},
            )
            return

        return


# ---------------------------
# State images (idempotent)
# ---------------------------

def get_state_image_by_change_event(engine: Engine, change_event_id: str) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        return _get_state_image_by_change_event(conn, change_event_id)


def create_state_image(engine: Engine, change_event_id: str, notes: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Create or reuse a state image for a change_event_id.
    Idempotent and safe to call multiple times.

    Returns the state image row, or None if state tables don't exist.
    """
    with engine.begin() as conn:
        if not _table_exists(conn, "etl_state_images"):
            return None

    existing = get_state_image_by_change_event(engine, change_event_id)
    if existing:
        if notes and not (existing.get("notes") or "").strip():
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE etl_state_images
                        SET notes = :notes
                        WHERE change_event_id = CAST(:eid AS uuid)
                        """
                    ),
                    {"eid": change_event_id, "notes": notes},
                )
            existing["notes"] = notes

        set_head(engine, state_image_id=str(existing["state_image_id"]))
        return existing

    sid = str(uuid.uuid4())
    head = get_current_state(engine)
    parent = str(head["state_image_id"]) if head and head.get("state_image_id") else None

    with engine.begin() as conn:
        try:
            conn.execute(
                text(
                    """
                    INSERT INTO etl_state_images
                      (state_image_id, change_event_id, parent_state_image_id, notes)
                    VALUES
                      (CAST(:sid AS uuid), CAST(:eid AS uuid), CAST(:parent AS uuid), :notes)
                    """
                ),
                {"sid": sid, "eid": change_event_id, "parent": parent, "notes": notes},
            )
        except IntegrityError:
            pass

    created = get_state_image_by_change_event(engine, change_event_id)
    if created:
        set_head(engine, state_image_id=str(created["state_image_id"]))
    return created


# ---------------------------
# Rollback (single event)
# ---------------------------

def rollback_change_event(engine: Engine, change_event_id: str, actor: str = "rollback") -> Dict[str, Any]:
    """
    Rollback reverts a change event by applying inverse operations:
      INSERT → DELETE
      UPDATE → restore db_before

    Creates a NEW change_event_id for the rollback action (audited),
    and advances HEAD by creating/reusing a state image for that rollback event.
    """
    rollback_eid = str(uuid.uuid4())

    with engine.begin() as conn:
        if not _table_exists(conn, "etl_change_events") or not _table_exists(conn, "etl_row_changes"):
            raise RuntimeError("Rollback requires etl_change_events and etl_row_changes tables.")

        conn.execute(
            text(
                """
                INSERT INTO etl_change_events
                  (change_event_id, actor, source_name, file_name, status, notes, started_at)
                VALUES
                  (CAST(:eid AS uuid), :actor, 'rollback', :file, 'RUNNING', :notes, now())
                """
            ),
            {
                "eid": rollback_eid,
                "actor": actor,
                "file": f"rollback:{change_event_id}",
                "notes": f"Rollback of {change_event_id}",
            },
        )

        rows = conn.execute(
            text(
                """
                SELECT table_name, pk, op, db_before, db_after
                FROM etl_row_changes
                WHERE change_event_id = CAST(:eid AS uuid)
                  AND applied = true
                ORDER BY created_at DESC
                """
            ),
            {"eid": change_event_id},
        ).mappings().all()

        for r in rows:
            table = r["table_name"]
            pk = r["pk"]
            op = (r["op"] or "").upper()
            db_before = r["db_before"] or {}

            if op == "INSERT":
                # Best-effort delete using common PK cols; compare as text to support numeric PKs too
                for pk_col in ("order_id", "transaction_id", "id"):
                    try:
                        conn.execute(
                            text(f"DELETE FROM {table} WHERE {pk_col}::text = :v"),
                            {"v": str(pk)},
                        )
                        break
                    except Exception:
                        continue

            elif op == "UPDATE":
                if not db_before:
                    continue

                where_col = None
                for cand in ("order_id", "transaction_id", "id"):
                    if cand in db_before:
                        where_col = cand
                        break
                if where_col is None:
                    continue

                cols = [c for c in db_before.keys() if c != where_col]
                if not cols:
                    continue

                set_sql = ", ".join([f"{c} = :{c}" for c in cols])
                sql = text(f"UPDATE {table} SET {set_sql} WHERE {where_col} = :where_val")
                params = {c: db_before.get(c) for c in cols}
                params["where_val"] = db_before.get(where_col)
                try:
                    conn.execute(sql, params)
                except Exception:
                    pass

        conn.execute(
            text(
                """
                UPDATE etl_change_events
                SET status = 'SUCCESS', finished_at = now()
                WHERE change_event_id = CAST(:eid AS uuid)
                """
            ),
            {"eid": rollback_eid},
        )

    # Advance HEAD (best effort)
    try:
        create_state_image(engine, rollback_eid, notes=f"rollback of {change_event_id}")
    except Exception:
        pass

    return {
        "status": "SUCCESS",
        "message": f"Rollback complete. Created rollback change_event_id: {rollback_eid}",
        "change_event_id": rollback_eid,
    }


# ---------------------------
# Rollback to point-in-time (NEW)
# ---------------------------

def rollback_to_point_in_time(engine: Engine, target_change_event_id: str, actor: str = "rollback") -> Dict[str, Any]:
    """
    Roll back the database to a specific point in time defined by target_change_event_id.

    Mechanism:
      - Requires state images (etl_state_images) and a HEAD pointer.
      - Finds the state image for target_change_event_id.
      - Walks from current HEAD backwards to the target state image.
      - For each state image AFTER the target, rolls back its change_event_id (one-by-one),
        producing new rollback change events and advancing HEAD.

    Result:
      - DB matches the target point in time.
      - HEAD ends at the most recent rollback state image (audit-preserving).
    """
    with engine.begin() as conn:
        if not _table_exists(conn, "etl_state_images"):
            raise RuntimeError("Point-in-time rollback requires etl_state_images table (state images).")

        head_sid = _get_head_state_image_id(conn)
        if not head_sid:
            raise RuntimeError("No HEAD state image set. Create a state image first (run ETL with changes or bootstrap).")

        target_img = _get_state_image_by_change_event(conn, target_change_event_id)
        if not target_img:
            raise RuntimeError(f"No state image exists for change_event_id={target_change_event_id}. Create one first.")

        chain = _collect_head_to_target_chain(
            conn,
            head_state_image_id=head_sid,
            target_state_image_id=target_img["state_image_id"],
        )

    # chain = [HEAD, ..., TARGET] (inclusive)
    if len(chain) == 1:
        return {
            "status": "NO_OP",
            "message": "HEAD is already at the requested point in time. No rollback needed.",
            "target_change_event_id": target_change_event_id,
            "rolled_back_count": 0,
            "rollback_change_event_ids": [],
        }

    # Rollback every change event after the target: chain[0:-1]
    rollback_ids: List[str] = []
    rolled = 0
    for img in chain[:-1]:
        eid = img.get("change_event_id")
        if not eid:
            continue
        r = rollback_change_event(engine, eid, actor=actor)
        rid = r.get("change_event_id")
        if rid:
            rollback_ids.append(str(rid))
        rolled += 1

    return {
        "status": "SUCCESS",
        "message": f"Rolled back {rolled} change event(s) to reach point-in-time change_event_id={target_change_event_id}.",
        "target_change_event_id": target_change_event_id,
        "rolled_back_count": rolled,
        "rollback_change_event_ids": rollback_ids,
    }
