# app.py
from __future__ import annotations

import uuid
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# --- Project imports ---
from src.pipeline import run_import
from src.db import load_db_config, make_engine

# Audit / UI queries
from src.audit_queries import (
    list_change_events,
    get_row_changes,
    get_conflicts,
    get_column_diffs,
)

# Optional bootstrap (if you have it)
try:
    from src.bootstrap_gold import bootstrap_fact_from_gold_csv
except Exception:
    bootstrap_fact_from_gold_csv = None

# State / rollback
try:
    from src.state import (
        get_current_state,
        create_state_image,
        rollback_change_event,
        rollback_to_point_in_time,  # NEW
    )
except Exception:
    get_current_state = None
    create_state_image = None
    rollback_change_event = None
    rollback_to_point_in_time = None


# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
GOLD_DIR = DATA_DIR / "gold"

RAW_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Helpers
# -----------------------------
def save_uploaded_file(uploaded_file) -> Path:
    """Save an uploaded file to a temp location (ephemeral; auto-cleaned after run)."""
    suffix = Path(uploaded_file.name).suffix.lower()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(uploaded_file.getbuffer())
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def fmt_range(min_s: Optional[str], max_s: Optional[str]) -> str:
    if not min_s or not max_s:
        return "—"
    return f"{min_s} → {max_s}"


def df_safe_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit/Arrow can choke on UUID objects. Force object-like values to strings.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].apply(lambda v: str(v) if v is not None else None)
    return out


def load_engine_or_stop():
    try:
        cfg = load_db_config(Path("config/db.yml"))
        engine = make_engine(cfg)
        return engine
    except Exception as e:
        st.error(f"Database not configured or not reachable: {type(e).__name__}: {e}")
        st.stop()


@st.cache_resource
def get_engine_cached():
    cfg = load_db_config(Path("config/db.yml"))
    return make_engine(cfg)


def render_diff_summary_with_values(engine, change_event_id: str, diff_summary: dict) -> None:
    """
    Show a collapsed-by-default diff summary grouped by column, where expanding a column
    reveals the *true* before/after values and source row positions (line numbers).

    This queries etl_row_changes lazily, so unchanged rows are never loaded.
    """
    def _render_one(title: str, d: dict, table_name: str) -> None:
        st.markdown(f"### {title}")
        inserted = int(d.get("inserted_count", 0) or 0)
        updated = int(d.get("updated_count", 0) or 0)
        conflicted = int(d.get("conflicted_count", 0) or 0)
        rejected = int(d.get("rejected_count", 0) or 0)
        backfilled = int(d.get("hash_backfilled_count", 0) or 0)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Inserted", inserted)
        c2.metric("Updated", updated)
        c3.metric("Conflicted", conflicted)
        c4.metric("Rejected", rejected)
        c5.metric("Hash backfilled", backfilled)

        if updated == 0:
            st.caption("No updates.")
            return

        st.markdown("#### Updates by changed column")
        st.caption("Expand a column to see true before/after values (collapsed by default).")

        col_counts = d.get("updated_by_column_counts", {}) or {}
        col_samples = d.get("updated_by_column_samples", {}) or {}

        for col, cnt in col_counts.items():
            label = f"{col} — {cnt:,} rows"
            with st.expander(label, expanded=False):
                if col_samples.get(col):
                    st.write("Sample PKs:")
                    st.write(col_samples.get(col, []))

                try:
                    df = get_column_diffs(engine, change_event_id, table_name, col, limit=20000)
                    df = df_safe_for_streamlit(df)
                    if df.empty:
                        st.info("No per-row diff values found for this column (unexpected).")
                    else:
                        st.dataframe(df, width="stretch", hide_index=True)
                        if len(df) >= 20000:
                            st.warning("Showing first 20,000 diffs for safety. Increase limit if needed.")
                except Exception as e:
                    st.warning(f"Could not load detailed diffs: {type(e).__name__}: {e}")

    sales = diff_summary.get("sales", {}) or {}
    budget = diff_summary.get("budget", {}) or {}

    _render_one("Sales", sales, table_name="stg_sales_orders")
    st.divider()
    _render_one("Budget", budget, table_name="stg_budget_transactions")


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Financial ETL Tool", layout="wide")
st.title("Financial ETL Tool")

tabs = st.tabs(["Run ETL", "Change Log"])


# =============================
# TAB 1 — RUN ETL
# =============================
with tabs[0]:
    left, right = st.columns([1, 1], gap="large")

    with left:
        st.subheader("Run options")
        use_existing_raw = st.checkbox(
            "Use existing files already in data/raw",
            value=False,
            help="Auto-detect sales/budget files from data/raw",
        )
        dry_run = st.checkbox("Dry run (no DB writes, no gold export)", value=False)

        st.divider()
        st.subheader("Inputs")

        sales_path: Optional[Path] = None
        budget_path: Optional[Path] = None

        if not use_existing_raw:
            sales_upload = st.file_uploader("Sales file (CSV)", type=["csv"])
            budget_upload = st.file_uploader("Budget vs Actual file (CSV or XLSX)", type=["csv", "xlsx", "xls"])

            if sales_upload:
                sales_path = save_uploaded_file(sales_upload)
                st.caption(f"Saved (temp): {sales_path}")

            if budget_upload:
                budget_path = save_uploaded_file(budget_upload)
                st.caption(f"Saved (temp): {budget_path}")
        else:
            st.info("Pipeline will auto-discover files in data/raw")

        run_btn = st.button("Run ETL", type="primary", use_container_width=True)

    with right:
        st.subheader("Result")
        if run_btn:
            if not use_existing_raw and (sales_path is None or budget_path is None):
                st.error("Please upload BOTH files or enable auto-discovery.")
            else:
                with st.spinner("Running ETL pipeline..."):
                    res = run_import(
                        sales_path=sales_path,
                        budget_path=budget_path,
                        dry_run=dry_run,
                    )

                for p in [sales_path, budget_path]:
                    try:
                        if p and p.exists() and str(p).startswith("/tmp"):
                            p.unlink()
                    except Exception:
                        pass

                status = res.get("status", "UNKNOWN")
                msg = res.get("message", "")
                change_event_id = res.get("change_event_id", "")

                if status in ("SUCCESS", "DRY_RUN", "NO_CHANGES"):
                    if status == "NO_CHANGES":
                        st.info(msg or "No changes detected — database already matches these files.")
                    else:
                        st.success(f"{status}: {msg or 'ETL completed.'}")
                else:
                    st.error(f"{status}: {msg}")

                st.write("**Change Event ID:**", str(change_event_id))

                st.divider()
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Inserted", res.get("inserted", 0))
                m2.metric("Updated", res.get("updated", 0))
                m3.metric("Unchanged", res.get("unchanged", 0))
                m4.metric("Rejected", res.get("rejected", 0))

                diff_summary = res.get("diff_summary")
                if diff_summary and change_event_id:
                    st.divider()
                    with st.expander("Diff summary (by column, with before/after)", expanded=False):
                        try:
                            engine2 = get_engine_cached()
                            render_diff_summary_with_values(engine2, str(change_event_id), diff_summary)
                        except Exception as e:
                            st.warning(f"Could not render diff summary: {type(e).__name__}: {e}")


# =============================
# TAB 2 — CHANGE LOG
# =============================
with tabs[1]:
    st.subheader("Change Log & History")

    engine = load_engine_or_stop()

    st.markdown("### Current State (HEAD)")
    if get_current_state is None:
        st.info("State module not available. Ensure src/state.py exists and is importable.")
    else:
        try:
            head = get_current_state(engine)
            if head:
                st.write(
                    f"**HEAD state_image_id:** `{head['state_image_id']}`  \n"
                    f"**HEAD change_event_id:** `{head.get('change_event_id','')}`  \n"
                    f"**Updated:** {head['updated_at']}"
                )
            else:
                st.info("No HEAD pointer yet. Run ETL (with changes) or Bootstrap to create first state image.")
        except Exception as e:
            st.warning(f"Could not read HEAD pointer: {type(e).__name__}: {e}")

    st.divider()

    st.markdown("### Change Events")
    try:
        events = list_change_events(engine, limit=50)
        events = df_safe_for_streamlit(events)
    except Exception as e:
        st.error(f"Could not query change events. Verify schema + permissions. {type(e).__name__}: {e}")
        st.stop()

    if events.empty:
        st.info("No change events recorded yet.")
        st.stop()

    st.dataframe(events, width="stretch", hide_index=True)

    selected_id = st.selectbox(
        "Select a change_event_id to inspect",
        options=events["change_event_id"].tolist(),
    )

    if selected_id:
        st.divider()
        st.markdown("### Inspect Event")

        c1, c2 = st.columns([1, 1], gap="large")

        with c1:
            st.markdown("#### Row Changes")
            try:
                df_changes = get_row_changes(engine, selected_id)
                df_changes = df_safe_for_streamlit(df_changes)
                if df_changes.empty:
                    st.info("No row changes recorded for this event.")
                else:
                    st.dataframe(df_changes, width="stretch", hide_index=True)
            except Exception as e:
                st.warning(f"Could not load row changes: {type(e).__name__}: {e}")

        with c2:
            st.markdown("#### Conflicts")
            try:
                df_conflicts = get_conflicts(engine, selected_id)
                df_conflicts = df_safe_for_streamlit(df_conflicts)
                if df_conflicts.empty:
                    st.info("No conflicts for this event.")
                else:
                    st.dataframe(df_conflicts, width="stretch", hide_index=True)
            except Exception as e:
                st.info(f"Conflicts unavailable: {type(e).__name__}: {e}")

        st.divider()
        st.markdown("### State + Rollback")

        colA, colB = st.columns([1, 1], gap="large")

        with colA:
            if create_state_image is None:
                st.caption("State module not available.")
            else:
                if st.button("Create state image for this event (set HEAD)", use_container_width=True):
                    try:
                        create_state_image(engine, selected_id, notes="manual state image creation")
                        st.success("Created state image and updated HEAD.")
                    except Exception as e:
                        st.error(f"Failed to create state image: {type(e).__name__}: {e}")

        with colB:
            actor = st.text_input("Actor (for rollback audit)", value="streamlit")

            # ---- Single-event rollback (existing)
            if rollback_change_event is None:
                st.caption("Rollback not available (state module missing).")
            else:
                if st.button("Rollback this change_event_id (single event)", use_container_width=True):
                    with st.spinner("Rolling back…"):
                        try:
                            r = rollback_change_event(engine, selected_id, actor=actor)
                            st.success(r.get("message", "Rollback complete."))
                            if r.get("change_event_id"):
                                st.caption(f"Rollback change_event_id: {r['change_event_id']}")
                        except Exception as e:
                            st.error(f"Rollback failed: {type(e).__name__}: {e}")

        st.divider()
        st.markdown("### Rollback to point in time")

        target_id = st.selectbox(
            "Choose the change_event_id you want to roll back TO (point in time)",
            options=events["change_event_id"].tolist(),
            help="This will undo every change event after the chosen one on the HEAD history chain.",
        )

        actor2 = st.text_input("Actor (for point-in-time rollback audit)", value=actor, key="actor_pit")

        if rollback_to_point_in_time is None:
            st.info("Point-in-time rollback not available (update src/state.py).")
        else:
            if st.button("Rollback to selected point in time", type="primary", use_container_width=True):
                with st.spinner(f"Rolling back to {target_id}…"):
                    try:
                        r = rollback_to_point_in_time(engine, target_id, actor=actor2)
                        if r.get("status") == "NO_OP":
                            st.info(r.get("message", "No rollback needed."))
                        else:
                            st.success(r.get("message", "Point-in-time rollback complete."))
                        st.caption(f"Rolled back count: {r.get('rolled_back_count', 0)}")
                        rb_ids = r.get("rollback_change_event_ids", []) or []
                        if rb_ids:
                            with st.expander("Rollback change_event_ids created", expanded=False):
                                st.write(rb_ids)
                    except Exception as e:
                        st.error(f"Point-in-time rollback failed: {type(e).__name__}: {e}")
