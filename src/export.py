# src/export.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy.engine import Engine


def export_gold_fact_to_csv(engine: Engine, out_path: Path) -> Path:
    """
    Export a Tableau Public friendly CSV (no audit fields needed).
    """
    query = """
    SELECT
      month_start,
      department,
      category,
      scenario,
      amount,
      source
    FROM fact_finance_monthly
    ORDER BY month_start, department, category, scenario, source
    """
    df = pd.read_sql(query, engine)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path
