Finance ETL

A production-grade, auditable ETL pipeline for financial data with:

Fast change detection (row hashing)

True before/after diffs

Streamlit UI

State images + rollback (single event or point-in-time)

Minimal disk usage (ephemeral uploads)

Deterministic, idempotent runs

Designed to behave like a real data platform, not a demo.

✨ Key Features
🔄 ETL Pipeline

Extract

CSV / Excel support

Ephemeral uploads (temp files auto-deleted)

Raw row count detection for fast progress bars

Preserves Excel-style row numbers (source_row_num)

Transform

Sales normalization

Budget vs Actual normalization

Clean primary key enforcement

Configurable column mappings

Load

Hash-based change detection

Bulk upsert with conflict protection

Metadata preserved for audit

⚡ Fast & Correct Change Detection

Uses row-level hashing (row_hash) to short-circuit unchanged rows

Only rows whose hash changes are diffed

Metadata-only changes (like row position) do not trigger updates

Identical reruns exit early (NO_CHANGES)

🔍 True Diff Summary (Not Samples)

After each run you get:

Insert / Update / Conflict / Reject counts

Diff grouped by column

For each changed column:

Primary key

Source row number (Excel line)

Before value

After value

All diffs are:

Lazy-loaded (UI never freezes)

Backed by the audit tables

Collapsed by default

🧾 Full Audit Trail

Every run creates:

etl_change_events – run metadata

etl_row_changes – row-level before/after JSON

etl_conflicts – protected field violations

Nothing is ever silently overwritten.

🧠 State Images & History

Each successful run can create a state image:

State images form a linked list (parent → child)

HEAD always points to the current state

Used for safe rollback and history traversal

⏪ Rollback Capabilities
1️⃣ Rollback a single change event

Undo exactly one change event:

INSERT → DELETE

UPDATE → restore db_before

Creates a new rollback change event (fully audited).

2️⃣ Rollback to a point in time (⭐ powerful)

Pick an earlier change_event_id and:

Automatically rolls back every change after it

Walks the state image chain safely

Leaves HEAD at the requested historical point

This is true point-in-time recovery, not a reset.

🖥️ Streamlit UI
Run ETL tab

Upload files or auto-discover from data/raw

Progress bars with row counts

Diff summary with expandable before/after tables

Download gold output

Change Log tab

View all change events

Inspect row-level diffs

Inspect conflicts

Create state images

Rollback (single event or point-in-time)

🗂 Project Structure
financial_etl/
├── app.py                    # Streamlit UI
├── src/
│   ├── extract.py            # File reading + row counts
│   ├── transform_sales.py
│   ├── transform_budget.py
│   ├── merge.py              # Hash-aware upsert + diff aggregation
│   ├── pipeline.py           # Orchestration
│   ├── audit.py              # Audit writers
│   ├── audit_queries.py      # UI queries
│   ├── state.py              # State images + rollback logic
│   ├── rebuild_fact.py
│   ├── export.py
│   ├── ddl.py                # Robust schema application
│   └── db.py
├── sql/
│   └── schema.sql
├── data/
│   ├── raw/
│   └── gold/
├── config/
│   ├── db.yml
│   └── column_maps.yml
└── README.md

🧬 Database Tables
Core

etl_change_events

etl_row_changes

etl_conflicts

State

etl_state_images

etl_state_pointer (HEAD)

Staging

stg_sales_orders

stg_budget_transactions

Gold

fact_finance_monthly

🛠 Setup
1) Install dependencies
pip install -r requirements.txt

2) Configure database

Edit:

config/db.yml

3) Run the app
streamlit run app.py
