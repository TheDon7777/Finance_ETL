from __future__ import annotations

from typing import Any, Optional
import pandas as pd
from dateutil import parser


def _to_month_start(x) -> str | None:
    if pd.isna(x):
        return None
    try:
        dt = parser.parse(str(x))
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).date().isoformat()
    except Exception:
        return None


def _to_number(x):
    if pd.isna(x):
        return None
    s = str(x).strip().replace("$", "").replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None


def _normalize_category_map(category_map: Any) -> pd.DataFrame:
    """
    Accepts:
      - pd.DataFrame with raw_category/canonical_category (and optional default_department)
      - dict mapping raw_category -> canonical_category
      - None / {} / empty -> identity mapping (no-op)
    Returns a DataFrame with required columns.
    """
    if category_map is None:
        return pd.DataFrame(columns=["raw_category", "canonical_category", "default_department"])

    if isinstance(category_map, pd.DataFrame):
        df = category_map.copy()
        # Ensure expected columns exist (empty if missing)
        for c in ["raw_category", "canonical_category", "default_department"]:
            if c not in df.columns:
                df[c] = None
        return df[["raw_category", "canonical_category", "default_department"]]

    if isinstance(category_map, dict):
        rows = [{"raw_category": str(k), "canonical_category": str(v), "default_department": None} for k, v in category_map.items()]
        return pd.DataFrame(rows, columns=["raw_category", "canonical_category", "default_department"])

    # Fallback: treat as empty/no-op
    return pd.DataFrame(columns=["raw_category", "canonical_category", "default_department"])


def apply_category_map(df: pd.DataFrame, category_col: str, map_df: pd.DataFrame) -> pd.DataFrame:
    # map raw_category -> canonical_category (+ optional default_department)
    map_df = map_df.copy()

    if map_df.empty:
        out = df.copy()
        out["canonical_category"] = out[category_col].astype(str)
        return out

    map_df["raw_category"] = map_df["raw_category"].astype(str)
    map_df["canonical_category"] = map_df["canonical_category"].astype(str)

    df = df.copy()
    df[category_col] = df[category_col].astype(str)

    merged = df.merge(map_df, how="left", left_on=category_col, right_on="raw_category")
    merged["canonical_category"] = merged["canonical_category"].fillna(merged[category_col])
    return merged


def transform_budget_vs_actual_to_fact(
    df: pd.DataFrame,
    date_col: str,
    dept_col: str | None,
    category_col: str,
    actual_col: str,
    budget_col: str,
    category_map: Any,
) -> pd.DataFrame:
    df = df.copy()
    df["month_start"] = df[date_col].apply(_to_month_start)
    df = df.dropna(subset=["month_start"])

    map_df = _normalize_category_map(category_map)
    df = apply_category_map(df, category_col, map_df)

    # Department: use provided column, else fallback to default_department from mapping, else "Finance"
    if dept_col and dept_col in df.columns:
        df["department"] = df[dept_col].astype(str)
    else:
        df["department"] = df.get("default_department", pd.Series(["Finance"] * len(df))).fillna("Finance").astype(str)

    # Build two fact sets
    actual = pd.DataFrame({
        "month_start": df["month_start"],
        "department": df["department"],
        "category": df["canonical_category"],
        "scenario": "Actual",
        "amount": df[actual_col].apply(_to_number),
        "source": "kaggle_budget_vs_actual"
    })

    budget = pd.DataFrame({
        "month_start": df["month_start"],
        "department": df["department"],
        "category": df["canonical_category"],
        "scenario": "Budget",
        "amount": df[budget_col].apply(_to_number),
        "source": "kaggle_budget_vs_actual"
    })

    out = pd.concat([actual, budget], ignore_index=True)
    out = out.dropna(subset=["month_start", "amount"])

    # Aggregate monthly
    out = (
        out.groupby(["month_start", "department", "category", "scenario", "source"], as_index=False)
           .agg(amount=("amount", "sum"))
    )

    return out
