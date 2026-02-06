# src/merge.py
from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple, Callable, DefaultDict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .audit import log_row_change


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    conflicted: int = 0
    rejected: int = 0


def _row_to_json(row: pd.Series) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if pd.isna(v):
            out[k] = None
        else:
            out[k] = v.item() if hasattr(v, "item") else v
    return out


def _diff_cols(existing: Optional[Dict[str, Any]], incoming: Dict[str, Any], cols: List[str]) -> List[str]:
    """Return list of columns whose values differ between existing and incoming."""
    if existing is None:
        return cols[:]
    changed: List[str] = []
    for c in cols:
        if existing.get(c) != incoming.get(c):
            changed.append(c)
    return changed


def _fetch_existing_bulk(
    engine: Engine,
    table: str,
    pk_col: str,
    pk_vals: List[Any],
    chunk_size: int = 2000,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not pk_vals:
        return out

    with engine.begin() as conn:
        for i in range(0, len(pk_vals), chunk_size):
            chunk = pk_vals[i : i + chunk_size]
            params = {f"p{i2}": v for i2, v in enumerate(chunk)}
            placeholders = ", ".join([f":p{i2}" for i2 in range(len(chunk))])
            sql = text(f"SELECT * FROM {table} WHERE {pk_col} IN ({placeholders})")
            rows = conn.execute(sql, params).mappings().all()
            for r in rows:
                out[str(r[pk_col])] = dict(r)
    return out


def _push_sample(sample_list: List[str], value: str, limit: int) -> None:
    if len(sample_list) < limit:
        sample_list.append(value)


def merge_upsert(
    *,
    engine: Engine,
    change_event_id: str,
    table: str,
    pk_col: str,
    df: pd.DataFrame,
    compare_cols: List[str],
    protected_cols: List[str],
    dry_run: bool = False,
    hash_col: Optional[str] = "row_hash",
    meta_cols: Optional[List[str]] = None,
    diff_sample_size: int = 25,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    progress_every: int = 2000,
    fetch_chunk_size: int = 2000,
    write_chunk_size: int = 2000,
    backfill_hash: bool = True,
) -> Tuple[MergeStats, pd.DataFrame, Dict[str, Any]]:
    """
    Fast upsert with:
      - bulk fetch existing rows
      - row_hash short-circuit (when available)
      - batch upsert
      - diff summary grouped by changed business column (counts + capped PK samples)

    Important:
      - A difference in row_hash alone is NOT treated as a business update.
      - Metadata-only columns (meta_cols) do not count as business updates.
    """
    stats = MergeStats()
    conflicts: List[Dict[str, Any]] = []

    meta_cols = meta_cols or ["source_row_num"]

    updated_by_column_counts: DefaultDict[str, int] = defaultdict(int)
    updated_by_column_samples: DefaultDict[str, List[str]] = defaultdict(list)

    diff_summary: Dict[str, Any] = {
        "table": table,
        "inserted_count": 0,
        "updated_count": 0,
        "conflicted_count": 0,
        "rejected_count": 0,
        "hash_backfilled_count": 0,
        "inserted_pks_sample": [],
        "updated_pks_sample": [],
        "conflicted_pks_sample": [],
        "updated_by_column_counts": {},
        "updated_by_column_samples": {},
    }

    df = df.copy()

    if pk_col not in df.columns:
        raise KeyError(f"merge_upsert: pk_col '{pk_col}' not found in df columns: {list(df.columns)}")

    # Reject blank PKs
    pk_series = df[pk_col]
    bad_pk_mask = pk_series.isna() | (pk_series.astype(str).str.strip() == "")
    if bad_pk_mask.any():
        rej = int(bad_pk_mask.sum())
        stats.rejected += rej
        diff_summary["rejected_count"] = rej
        df = df.loc[~bad_pk_mask].copy()

    if df.empty:
        return stats, pd.DataFrame(conflicts), diff_summary

    pk_vals = df[pk_col].tolist()
    existing_map = _fetch_existing_bulk(
        engine=engine,
        table=table,
        pk_col=pk_col,
        pk_vals=pk_vals,
        chunk_size=fetch_chunk_size,
    )

    total = int(len(df))

    # Build batch upsert statement. compare_cols are the columns written to the table.
    cols = [pk_col] + [c for c in compare_cols if c != pk_col]
    cols_sql = ", ".join(cols + ["last_change_event_id", "last_updated_at"])
    vals_sql = ", ".join([f":{c}" for c in cols] + [":last_change_event_id", "now()"])
    update_sql = ", ".join(
        [f"{c}=EXCLUDED.{c}" for c in cols if c != pk_col]
        + ["last_change_event_id=EXCLUDED.last_change_event_id", "last_updated_at=now()"]
    )

    upsert_sql = text(
        f"""
        INSERT INTO {table} ({cols_sql})
        VALUES ({vals_sql})
        ON CONFLICT ({pk_col}) DO UPDATE SET
          {update_sql}
        """
    )

    # Business columns for diffing: exclude pk, hash, and metadata
    business_cols = [c for c in compare_cols if c != pk_col and c != hash_col and c not in set(meta_cols)]

    to_write_params: List[Dict[str, Any]] = []
    to_write_audit: List[Dict[str, Any]] = []

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        pk_val = row[pk_col]
        pk_key = str(pk_val)

        incoming = _row_to_json(row)
        existing = existing_map.get(pk_key)

        inc_h = incoming.get(hash_col) if hash_col else None
        ex_h = existing.get(hash_col) if (hash_col and existing is not None) else None

        # Hash short-circuit: if both hashes exist and match => unchanged
        if hash_col and existing is not None and inc_h is not None and ex_h is not None and inc_h == ex_h:
            stats.unchanged += 1
            if progress_cb and (idx % progress_every == 0 or idx == total):
                progress_cb(idx, total, f"{table}: scanning")
            continue

        # Determine changed business columns
        changed_cols = _diff_cols(existing, incoming, business_cols)

        # If business columns identical, treat as unchanged (even if metadata/hash differs)
        if existing is not None and not changed_cols:
            stats.unchanged += 1

            # Optional: backfill hash without counting as update
            if (
                backfill_hash
                and (not dry_run)
                and hash_col
                and inc_h is not None
                and (ex_h is None or ex_h != inc_h)
            ):
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                UPDATE {table}
                                SET {hash_col} = :h,
                                    last_change_event_id = :eid,
                                    last_updated_at = now()
                                WHERE {pk_col} = :pk
                                """
                            ),
                            {"h": inc_h, "eid": change_event_id, "pk": pk_val},
                        )
                    diff_summary["hash_backfilled_count"] += 1
                except Exception:
                    pass

            if progress_cb and (idx % progress_every == 0 or idx == total):
                progress_cb(idx, total, f"{table}: scanning")
            continue

        # Conflict rules (updates only)
        conflict_cols: List[str] = []
        if existing is not None:
            for c in protected_cols:
                if c in business_cols and existing.get(c) != incoming.get(c):
                    conflict_cols.append(c)

        if conflict_cols:
            stats.conflicted += 1
            diff_summary["conflicted_count"] = stats.conflicted
            _push_sample(diff_summary["conflicted_pks_sample"], pk_key, diff_sample_size)

            if not dry_run:
                log_row_change(
                    engine,
                    change_event_id=change_event_id,
                    table_name=table,
                    pk=pk_key,
                    op="UPDATE",
                    changed_columns=changed_cols,
                    db_before=existing,
                    db_after=incoming,
                    applied=False,
                    conflict=True,
                    conflict_reason=f"Protected field mismatch: {', '.join(conflict_cols)}",
                )

            conflicts.append(
                {
                    "pk": pk_key,
                    "conflict_columns": ", ".join(conflict_cols),
                    "db_before": existing,
                    "patch_after": incoming,
                }
            )

            if progress_cb and (idx % progress_every == 0 or idx == total):
                progress_cb(idx, total, f"{table}: scanning")
            continue

        # Insert vs update
        if existing is None:
            stats.inserted += 1
            diff_summary["inserted_count"] = stats.inserted
            _push_sample(diff_summary["inserted_pks_sample"], pk_key, diff_sample_size)
            op = "INSERT"
        else:
            stats.updated += 1
            diff_summary["updated_count"] = stats.updated
            _push_sample(diff_summary["updated_pks_sample"], pk_key, diff_sample_size)
            op = "UPDATE"

            for col in changed_cols:
                updated_by_column_counts[col] += 1
                _push_sample(updated_by_column_samples[col], pk_key, diff_sample_size)

        if dry_run:
            if progress_cb and (idx % progress_every == 0 or idx == total):
                progress_cb(idx, total, f"{table}: scanning")
            continue

        # Write params (we write compare_cols + meta cols, as provided)
        params = {c: incoming.get(c) for c in cols}
        params["last_change_event_id"] = change_event_id
        to_write_params.append(params)
        to_write_audit.append(
            dict(pk=pk_key, op=op, changed_columns=changed_cols, db_before=existing, db_after=incoming)
        )

        if progress_cb and (idx % progress_every == 0 or idx == total):
            progress_cb(idx, total, f"{table}: scanning")

    # Batch write + audit only for changed rows
    if (not dry_run) and to_write_params:
        with engine.begin() as conn:
            for i in range(0, len(to_write_params), write_chunk_size):
                conn.execute(upsert_sql, to_write_params[i : i + write_chunk_size])

        for a in to_write_audit:
            log_row_change(
                engine,
                change_event_id=change_event_id,
                table_name=table,
                pk=a["pk"],
                op=a["op"],
                changed_columns=a["changed_columns"],
                db_before=a["db_before"],
                db_after=a["db_after"],
                applied=True,
                conflict=False,
                conflict_reason=None,
            )

    diff_summary["updated_by_column_counts"] = dict(
        sorted(updated_by_column_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    )
    diff_summary["updated_by_column_samples"] = dict(updated_by_column_samples)

    return stats, pd.DataFrame(conflicts), diff_summary
