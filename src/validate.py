from typing import List
import pandas as pd

def require_columns(df: pd.DataFrame, cols: List[str], context: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{context}] Missing required columns: {missing}. Present: {list(df.columns)}")
