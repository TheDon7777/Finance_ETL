# src/main.py
from pathlib import Path
import argparse

from pipeline import run_import

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sales", type=str, default=None, help="Path to sales CSV (optional).")
    parser.add_argument("--budget", type=str, default=None, help="Path to budget CSV (optional).")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing gold CSV.")
    args = parser.parse_args()

    res = run_import(
        sales_path=Path(args.sales) if args.sales else None,
        budget_path=Path(args.budget) if args.budget else None,
        dry_run=args.dry_run,
    )
    print(res["status"], "-", res["message"])
    if res.get("gold_path"):
        print("Gold:", res["gold_path"])

if __name__ == "__main__":
    main()
