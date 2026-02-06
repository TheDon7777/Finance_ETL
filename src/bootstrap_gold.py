# src/bootstrap_gold.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.audit import start_change_event, log_row_change, finish_change_event


def bootstrap_fact_from_gold_csv(
    engine: Engine,
    *,
    gold_csv_path: Path,
    actor: str = "bootstrap",
    truncate_first: bool = True,
) -> dict:
    """
    Bootstrap the DB from an existing gold_fact_finance.csv.

    What it does:
    - Creates a change_event in etl_change_events
    - Optionally TRUNCATEs fact_finance_monthly (default True for a clean init)
    - Inserts each gold row into fact_finance_monthly
    - Writes a row-level audit entry into etl_row_changes for each inserted fact row

    Note: This bootstraps the FACT table only. It does not backfill staging tables.
    """

    if not gold_csv_path.exists():
        raise FileNotFoundError(f"Gold CSV not found: {gold_csv_path}")

    df = pd.read_csv(gold_csv_path)

    required = {"month_start", "department", "category", "scenario", "amount", "source"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Gold CSV missing required columns: {sorted(missing)}")

    # Normalize
    df["month_start"] = pd.to_datetime(df["month_start"], errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Drop invalid rows
    df = df.dropna(subset=["month_start", "amount"])

    date_min = df["month_start"].min() if len(df) else None
    date_max = df["month_start"].max() if len(df) else None

    ctx = start_change_event(
        engine,
        actor=actor,
        source_name="bootstrap_gold",
        file_name=gold_csv_path.name,
        dry_run=False,
        date_min=date_min,
        date_max=date_max,
    )
    eid = ctx.change_event_id

    inserted = 0
    rejected = 0

    with engine.begin() as conn:
        if truncate_first:
            conn.execute(text("TRUNCATE TABLE fact_finance_monthly"))

        for _, r in df.iterrows():
            try:
                row = {
                    "month_start": r["month_start"],
                    "department": str(r["department"]),
                    "category": str(r["category"]),
                    "scenario": str(r["scenario"]),
                    "amount": float(r["amount"]),
                    "source": str(r["source"]),
                    "eid": eid,
                }

                conn.execute(
                    text(
                        """
                        INSERT INTO fact_finance_monthly
                          (month_start, department, category, scenario, amount, source,
                           last_change_event_id, last_updated_at)
                        VALUES
                          (:month_start, :department, :category, :scenario, :amount, :source,
                           :eid, now())
                        ON CONFLICT (month_start, department, category, scenario, source)
                        DO UPDATE SET
                          amount = EXCLUDED.amount,
                          last_change_event_id = EXCLUDED.last_change_event_id,
                          last_updated_at = now()
                        """
                    ),
                    row,
                )

                pk = f"{row['month_start']}|{row['department']}|{row['category']}|{row['scenario']}|{row['source']}"

                log_row_change(
                    engine,
                    change_event_id=eid,
                    table_name="fact_finance_monthly",
                    pk=pk,
                    op="INSERT",
                    changed_columns=["month_start", "department", "category", "scenario", "amount", "source"],
                    db_before=None,
                    db_after={
                        "month_start": str(row["month_start"]),
                        "department": row["department"],
                        "category": row["category"],
                        "scenario": row["scenario"],
                        "amount": row["amount"],
                        "source": row["source"],
                    },
                    applied=True,
                )

                inserted += 1
            except Exception:
                rejected += 1

    finish_change_event(
        engine,
        change_event_id=eid,
        status="SUCCESS",
        inserted=inserted,
        updated=0,
        unchanged=0,
        conflicted=0,
        rejected=rejected,
        notes="Bootstrap load from gold CSV into fact_finance_monthly (staging not backfilled).",
    )

    return {
        "change_event_id": eid,
        "status": "SUCCESS",
        "inserted": inserted,
        "rejected": rejected,
        "date_min": str(date_min) if date_min else None,
        "date_max": str(date_max) if date_max else None,
    }
