# Financial Data Governance & ETL Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io)
[![Status](https://img.shields.io/badge/Status-Production--Grade-success?style=for-the-badge)]()

**A production-grade, auditable ETL pipeline designed to treat financial data with the rigor it deserves.**

Unlike standard scripts that blindly overwrite tables, this platform focuses on **Data Trust**. It features cryptographic change detection, immutable audit logs, point-in-time recovery, and granular diff summaries to ensure no transaction is ever lost or corrupted.

> **Design Philosophy:** Behave like a real enterprise data platform, not a demo. Deterministic, idempotent, and transparent.

---

## 📸 Interface Preview

### Granular Reconciliation

  <img src="https://github.com/user-attachments/assets/8458cf75-5a73-4b48-8bee-37b4a5a58711" height=300px />
  <img src="https://github.com/user-attachments/assets/550b7e09-2002-4c5d-b7c3-007cd44f79b1" height=300px width=450px />
</p>

### *Automated detection of exact cell-level changes (Before vs. After) preventing silent data corruption.*
---

## ✨ Key Features

### 🔄 Robust ETL Pipeline
* **Ephemeral Ingestion:** Processes uploads in memory/temp storage; auto-cleans artifacts to minimize disk footprint.
* **Excel-Aware:** Preserves `source_row_num` (Excel line numbers) to help operations teams map database errors back to the original spreadsheet.
* **Normalization:** Automates sales and budget vs. actuals cleaning with configurable column mapping.
* **Idempotency:** Running the same file twice results in a `NO_CHANGES` state, preventing duplicate data pollution.

### ⚡ Cryptographic Change Detection
* **Row-Level Hashing:** Calculates a `row_hash` for every incoming record.
* **Zero-Copy Logic:** Short-circuits processing for unchanged rows.
* **Precision:** Only rows where the *hash differs* trigger a database write. Metadata updates (like row sorting) do not trigger false data updates.

<img width="2558" height="1274" alt="Screenshot 2026-02-05 032152" src="https://github.com/user-attachments/assets/cba43042-42ea-47b5-a706-c06ab7f09a74" />
*Processing 100,000+ rows asynchronously with real-time feedback.*

### 🔍 Deep-Diff Reconciliation
Stakeholders don't trust "Black Box" updates. This system provides a **True Diff Summary**:
* **Granular Reporting:** Breaks down Inserts, Updates, Conflicts, and Rejects.
* **Cell-Level Precision:** For every updated row, the UI shows:
    * Primary Key
    * Original Excel Line Number
    * **Before Value** (Snapshot)
    * **After Value** (Proposed)
* **Lazy Loading:** Diffs are backed by audit tables and paginated, ensuring the UI never freezes even on 100k+ row datasets.

### 🧾 Immutable Audit Trail & Rollback
Nothing is ever silently overwritten.
1.  **Full Audit Logging:**
    * `etl_change_events`: Who ran it, when, and what file.
    * `etl_row_changes`: JSONB storage of exact before/after states.
    * `etl_conflicts`: Logs protected field violations without crashing the pipeline.
2.  **Linked-List State Management:**
    * Maintains a `HEAD` pointer for the current dataset state.
    * State images form a parent-child linked list for history traversal.
3.  **Time-Travel Recovery:**
    * **Single Event Rollback:** Undo a specific bad upload (Insert → Delete, Update → Restore).
    * **Point-in-Time Recovery:** Pick a historical `change_event_id` and automatically roll back the database to that exact second, safely walking the state chain.

<img width="2557" height="1269" alt="Screenshot 2026-02-05 203456" src="https://github.com/user-attachments/assets/6655d5e5-1377-4c89-83a1-d3838657f85f" />
*Full history tracking of every execution, including failures and rollbacks.*

---

## 🗂 Project Architecture



```text
financial_etl/
├── app.py                  # Streamlit Entry Point (UI)
├── src/
│   ├── extract.py          # File reading, hashing, row counts
│   ├── transform_*.py      # Normalization logic (Sales/Budget)
│   ├── merge.py            # Hash-aware upsert logic & diff generation
│   ├── pipeline.py         # Orchestration (The "Controller")
│   ├── audit.py            # Immutable log writers
│   ├── state.py            # Linked-list state & rollback logic
│   └── ddl.py              # Schema enforcement
├── data/
│   ├── raw/                # Incoming CSV/Excel landing zone
│   └── gold/               # Cleaned, governed output exports
└── config/
    ├── db.yml              # Database connection
    └── column_maps.yml     # Field mapping configuration

