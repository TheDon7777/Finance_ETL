from datetime import datetime
from pathlib import Path
import pandas as pd

def build_gold_fact(sales_fact: pd.DataFrame, budget_fact: pd.DataFrame) -> pd.DataFrame:
    fact = pd.concat([sales_fact, budget_fact], ignore_index=True)

    load_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fact["load_id"] = load_id

    # Stable ordering for Tableau
    fact = fact.sort_values(["month_start","department","category","scenario"]).reset_index(drop=True)
    return fact

def write_gold(fact: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fact.to_csv(out_path, index=False)
