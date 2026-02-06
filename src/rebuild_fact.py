# src/rebuild_fact.py
from __future__ import annotations

from typing import Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine


def rebuild_fact_months(
    *,
    engine: Engine,
    months: Optional[List[str]],  # list of 'YYYY-MM-01' strings
    change_event_id: str,
) -> None:
    """
    Rebuild fact_finance_monthly for impacted months only.
    months: list of month_start dates as strings 'YYYY-MM-01'. If None -> rebuild all.
    """
    with engine.begin() as conn:
        if months:
            conn.execute(
                text(
                    "DELETE FROM fact_finance_monthly "
                    "WHERE month_start = ANY(CAST(:months AS date[]))"
                ),
                {"months": months},
            )
            month_filter_sales = "WHERE date_trunc('month', order_date)::date = ANY(CAST(:months AS date[]))"
            month_filter_budget = "WHERE date_trunc('month', date)::date = ANY(CAST(:months AS date[]))"
            params_months = {"months": months}
        else:
            conn.execute(text("TRUNCATE TABLE fact_finance_monthly"))
            month_filter_sales = ""
            month_filter_budget = ""
            params_months = {}

        # Revenue actual from sales
        conn.execute(
            text(
                f"""
                INSERT INTO fact_finance_monthly
                (month_start, department, category, scenario, amount, source, last_change_event_id, last_updated_at)
                SELECT
                  date_trunc('month', order_date)::date AS month_start,
                  'Sales' AS department,
                  'Revenue' AS category,
                  'Actual' AS scenario,
                  SUM(revenue) AS amount,
                  'sales_orders' AS source,
                  :eid AS last_change_event_id,
                  now() AS last_updated_at
                FROM stg_sales_orders
                {month_filter_sales}
                GROUP BY 1
                """
            ),
            {"eid": change_event_id, **params_months},
        )

        # Actuals from budget
        conn.execute(
            text(
                f"""
                INSERT INTO fact_finance_monthly
                (month_start, department, category, scenario, amount, source, last_change_event_id, last_updated_at)
                SELECT
                  date_trunc('month', date)::date AS month_start,
                  COALESCE(department, 'Unknown') AS department,
                  COALESCE(category, 'Uncategorized') AS category,
                  'Actual' AS scenario,
                  SUM(actual_amount) AS amount,
                  'budget_vs_actual' AS source,
                  :eid AS last_change_event_id,
                  now() AS last_updated_at
                FROM stg_budget_transactions
                {month_filter_budget}
                GROUP BY 1,2,3
                """
            ),
            {"eid": change_event_id, **params_months},
        )

        # Budgets from budget
        conn.execute(
            text(
                f"""
                INSERT INTO fact_finance_monthly
                (month_start, department, category, scenario, amount, source, last_change_event_id, last_updated_at)
                SELECT
                  date_trunc('month', date)::date AS month_start,
                  COALESCE(department, 'Unknown') AS department,
                  COALESCE(category, 'Uncategorized') AS category,
                  'Budget' AS scenario,
                  SUM(budget_amount) AS amount,
                  'budget_vs_actual' AS source,
                  :eid AS last_change_event_id,
                  now() AS last_updated_at
                FROM stg_budget_transactions
                {month_filter_budget}
                GROUP BY 1,2,3
                """
            ),
            {"eid": change_event_id, **params_months},
        )
