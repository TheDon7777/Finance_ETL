from __future__ import annotations

from pathlib import Path
from typing import Optional, Any, Callable, Dict, List
import inspect

import pandas as pd

from src.db import load_db_config, make_engine
from src.ddl import apply_schema
from src.extract import read_table_clean_cols
from src.validate import require_columns
from src.merge import merge_upsert
from src.rebuild_fact import rebuild_fact_months
from src.export import export_gold_fact_to_csv
from src.audit import start_change_event, finish_change_event

try:
    from src.state import create_state_image
except Exception:
    create_state_image = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_PATH = ROOT / "data" / "gold" / "gold_fact_finance.csv"
DEFAULT_CATEGORY_MAP_PATH = ROOT / "data" / "category_map.csv"


def call_with_supported_kwargs(func: Callable[..., Any], *args, **kwargs) -> Any:
    sig = inspect.signature(func)
    allowed = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    try:
        return func(*args, **filtered)
    except TypeError as e:
        raise TypeError(f"{func.__name__}{sig} failed: {e}. Passed kwargs: {sorted(filtered.keys())}") from e


def _month_starts_from_dates(s: pd.Series) -> List[str]:
    dt = pd.to_datetime(s, errors="coerce").dropna()
    if dt.empty:
        return []
    months = dt.dt.to_period("M").dt.to_timestamp().dt.date.astype(str).unique().tolist()
    months.sort()
    return months


def _compute_row_hash(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    """
    Fast fingerprint of selected columns.
    Uses pandas hashing for speed and stability within pandas.
    Returns a hex string (16 chars).
    """
    base = df[cols].copy()
    base = base.fillna("")
    # Ensure stable string representation
    for c in cols:
        base[c] = base[c].astype(str)

    h = pd.util.hash_pandas_object(base, index=False).astype("uint64")
    return h.map(lambda x: f"{int(x):016x}")


def _clean_pk_series(series: pd.Series) -> pd.Series:
    """Clean a would-be PK column into a nullable string series (reject NaN/blank/'nan')."""
    s = series.copy()
    s = s.where(~s.isna(), pd.NA)
    s = s.astype("string").str.strip()
    bad = s.isna() | (s == "") | (s.str.lower().isin(["nan", "none"]))
    s = s.where(~bad, pd.NA)
    return s


def run_import(
    *,
    sales_path: Optional[Path] = None,
    budget_path: Optional[Path] = None,
    dry_run: bool = False,
    actor: str = "streamlit",
    source_name: str = "file_upload",
    gold_out_path: Path = DEFAULT_GOLD_PATH,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    def _progress(msg: str) -> None:
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    _progress("Connecting to database…")
    cfg = load_db_config(ROOT / "config" / "db.yml")
    engine = make_engine(cfg)

    _progress("Applying schema (best-effort)…")
    apply_schema(engine, ROOT / "sql" / "schema.sql")

    # Discover if not provided
    raw_dir = ROOT / "data" / "raw"
    if sales_path is None:
        sfiles = list(raw_dir.glob("*sales*.csv"))
        sales_path = sfiles[0] if sfiles else None
    if budget_path is None:
        bfiles = list(raw_dir.glob("*budget*.csv")) + list(raw_dir.glob("*budget*.xlsx")) + list(raw_dir.glob("*budget*.xls"))
        budget_path = bfiles[0] if bfiles else None

    if not sales_path or not budget_path:
        return {"status": "FAILED", "message": "Sales and budget files not found.", "change_event_id": None}

    _progress("Starting change event…")
    ctx = start_change_event(
        engine,
        actor=actor,
        source_name=source_name,
        file_name=f"{sales_path.name}, {budget_path.name}",
        dry_run=dry_run,
    )
    change_event_id = ctx.change_event_id

    try:
        _progress("Reading input files…")
        sales_df = read_table_clean_cols(sales_path)
        bud_df = read_table_clean_cols(budget_path)

        _progress("Validating columns…")
        call_with_supported_kwargs(require_columns, sales_df, ["order_id", "order_date", "revenue"], context="sales")
        call_with_supported_kwargs(require_columns, bud_df, ["Transaction ID", "Date", "Budget Amount", "Actual Amount"], context="budget")

        # -----------------------
        # Build staging dataframes
        # -----------------------
        _progress("Preparing staging frames…")

        sales_stg = sales_df.copy()
        sales_stg["order_id"] = pd.to_numeric(sales_stg["order_id"], errors="coerce").astype("Int64")
        sales_stg["order_date"] = pd.to_datetime(sales_stg["order_date"], errors="coerce").dt.date
        sales_stg["revenue"] = pd.to_numeric(sales_stg["revenue"], errors="coerce")

        stg_sales_cols = ["order_id", "source_row_num", "order_date", "region", "payment_method", "revenue"]
        for c in stg_sales_cols:
            if c not in sales_stg.columns:
                sales_stg[c] = None
        sales_stg = sales_stg[stg_sales_cols].dropna(subset=["order_id"])
        sales_stg["order_id"] = sales_stg["order_id"].astype(int)

        # Add fingerprint
        sales_hash_cols = ["order_date", "region", "payment_method", "revenue"]
        sales_stg["row_hash"] = _compute_row_hash(sales_stg, sales_hash_cols)

        budget_stg = bud_df.copy()
        # PK cleaning: reject NaN/blank IDs so we never upsert a literal "nan" transaction_id
        budget_stg["transaction_id"] = _clean_pk_series(budget_stg["Transaction ID"])
        budget_stg = budget_stg.dropna(subset=["transaction_id"])
        budget_stg["date"] = pd.to_datetime(budget_stg["Date"], errors="coerce").dt.date
        budget_stg["department"] = budget_stg["Department"].astype("string").str.strip() if "Department" in budget_stg.columns else None
        budget_stg["category"] = budget_stg["Category"].astype("string").str.strip() if "Category" in budget_stg.columns else None
        for c in ["region", "payment_method"]:
            if c not in budget_stg.columns:
                budget_stg[c] = None
        budget_stg["budget_amount"] = pd.to_numeric(budget_stg["Budget Amount"], errors="coerce")
        budget_stg["actual_amount"] = pd.to_numeric(budget_stg["Actual Amount"], errors="coerce")

        budget_stg = budget_stg[
            ["transaction_id", "source_row_num", "date", "department", "category", "region", "budget_amount", "actual_amount", "payment_method"]
        ]

        # Add fingerprint
        budget_hash_cols = ["date", "department", "category", "region", "budget_amount", "actual_amount", "payment_method"]
        budget_stg["row_hash"] = _compute_row_hash(budget_stg, budget_hash_cols)

        # -----------------------
        # Merge (staging) — hash optimized
        # -----------------------
        sales_compare = ["order_id", "source_row_num", "order_date", "region", "payment_method", "revenue", "row_hash"]
        sales_protected = ["order_date"]  # your existing rule

        budget_compare = ["transaction_id", "source_row_num", "date", "department", "category", "region", "budget_amount", "actual_amount", "payment_method", "row_hash"]
        budget_protected: List[str] = []

        def _merge_progress(done: int, total: int, stage: str) -> None:
            _progress(f"{stage} {done:,}/{total:,}")

        _progress("Merging sales staging…")
        sales_stats, _sales_conflicts, sales_diff = merge_upsert(
            engine=engine,
            change_event_id=change_event_id,
            table="stg_sales_orders",
            pk_col="order_id",
            df=sales_stg,
            compare_cols=sales_compare,
            protected_cols=sales_protected,
            dry_run=dry_run,
            hash_col="row_hash",
            meta_cols=["source_row_num"],
            progress_cb=_merge_progress,
        )

        _progress("Merging budget staging…")
        budget_stats, _budget_conflicts, budget_diff = merge_upsert(
            engine=engine,
            change_event_id=change_event_id,
            table="stg_budget_transactions",
            pk_col="transaction_id",
            df=budget_stg,
            compare_cols=budget_compare,
            protected_cols=budget_protected,
            dry_run=dry_run,
            hash_col="row_hash",
            meta_cols=["source_row_num"],
            progress_cb=_merge_progress,
        )

        inserted = sales_stats.inserted + budget_stats.inserted
        updated = sales_stats.updated + budget_stats.updated
        unchanged = sales_stats.unchanged + budget_stats.unchanged
        conflicted = sales_stats.conflicted + budget_stats.conflicted
        rejected = sales_stats.rejected + budget_stats.rejected

        diff_summary = {
            "sales": sales_diff,
            "budget": budget_diff,
        }

        no_changes = (inserted == 0 and updated == 0 and conflicted == 0 and rejected == 0)
        if no_changes:
            _progress("No changes detected — finishing early.")
            finish_change_event(
                engine,
                change_event_id=change_event_id,
                status="SUCCESS" if not dry_run else "DRY_RUN",
                inserted=0,
                updated=0,
                unchanged=int(unchanged),
                conflicted=0,
                rejected=int(rejected),
                notes="No changes detected (idempotent run).",
            )
            return {
                "status": "NO_CHANGES",
                "message": "No changes detected — database already matches these files.",
                "change_event_id": str(change_event_id),
                "sales_rows": int(len(sales_df)),
                "budget_rows": int(len(bud_df)),
                "inserted": 0,
                "updated": 0,
                "unchanged": int(unchanged),
                "rejected": int(rejected),
                "gold_path": None,
                "diff_summary": diff_summary,
            }

        gold_path: Optional[Path] = None
        if not dry_run:
            _progress("Rebuilding fact table…")
            months = sorted(set(_month_starts_from_dates(sales_df["order_date"]) + _month_starts_from_dates(bud_df["Date"])))
            rebuild_fact_months(engine=engine, months=months or None, change_event_id=str(change_event_id))

            _progress("Exporting gold CSV…")
            gold_path = export_gold_fact_to_csv(engine, gold_out_path)

        _progress("Finishing change event…")
        finish_change_event(
            engine,
            change_event_id=change_event_id,
            status=("DRY_RUN" if dry_run else ("SUCCESS" if conflicted == 0 else "CONFLICTS")),
            inserted=int(inserted),
            updated=int(updated),
            unchanged=int(unchanged),
            conflicted=int(conflicted),
            rejected=int(rejected),
            notes=None,
        )

        if (not dry_run) and (create_state_image is not None):
            try:
                _progress("Creating state image (HEAD)…")
                create_state_image(engine, str(change_event_id), notes="pipeline success")
            except Exception:
                pass

        _progress("Done.")
        return {
            "status": "SUCCESS" if not dry_run else "DRY_RUN",
            "message": "ETL completed successfully." if not dry_run else "Dry run completed (no DB writes).",
            "change_event_id": str(change_event_id),
            "sales_rows": int(len(sales_df)),
            "budget_rows": int(len(bud_df)),
            "inserted": int(inserted),
            "updated": int(updated),
            "unchanged": int(unchanged),
            "rejected": int(rejected),
            "gold_path": str(gold_path) if gold_path else None,
            "diff_summary": diff_summary,
        }

    except Exception as e:
        finish_change_event(
            engine,
            change_event_id=change_event_id,
            status="FAILED",
            inserted=0,
            updated=0,
            unchanged=0,
            conflicted=0,
            rejected=0,
            notes=f"{type(e).__name__}: {e}",
        )
        raise
