import pandas as pd
from dateutil import parser

def _to_month_start(x) -> str | None:
    if pd.isna(x):
        return None
    try:
        dt = parser.parse(str(x))
        # month start
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

def transform_sales_to_fact(df: pd.DataFrame, date_col: str, revenue_col: str) -> pd.DataFrame:
    out = pd.DataFrame()
    out["month_start"] = df[date_col].apply(_to_month_start)
    out["department"] = "Sales"
    out["category"] = "Revenue"
    out["scenario"] = "Actual"
    out["amount"] = df[revenue_col].apply(_to_number)

    # drop invalid
    out = out.dropna(subset=["month_start", "amount"])

    # aggregate monthly
    out = (out.groupby(["month_start","department","category","scenario"], as_index=False)
              .agg(amount=("amount","sum")))

    out["source"] = "kaggle_sales_2025"
    return out
