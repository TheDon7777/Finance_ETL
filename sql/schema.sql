-- NOTE: This is your full schema.sql; only the relevant stg_* blocks changed
-- to include source_row_num and the ALTER TABLE section includes it too.

-- ============================
-- CHANGE EVENTS / AUDIT
-- ============================
CREATE TABLE IF NOT EXISTS etl_change_events (
  change_event_id UUID PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  actor TEXT,
  source_name TEXT,
  file_name TEXT,
  inserted_count INTEGER NOT NULL DEFAULT 0,
  updated_count INTEGER NOT NULL DEFAULT 0,
  unchanged_count INTEGER NOT NULL DEFAULT 0,
  conflicted_count INTEGER NOT NULL DEFAULT 0,
  rejected_count INTEGER NOT NULL DEFAULT 0,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS etl_row_changes (
  row_change_id UUID PRIMARY KEY,
  change_event_id UUID NOT NULL REFERENCES etl_change_events(change_event_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  table_name TEXT NOT NULL,
  pk TEXT NOT NULL,
  op TEXT NOT NULL, -- INSERT/UPDATE/DELETE
  applied BOOLEAN NOT NULL DEFAULT true,
  conflict BOOLEAN NOT NULL DEFAULT false,
  conflict_reason TEXT,
  changed_columns TEXT[],
  db_before JSONB,
  db_after JSONB
);

CREATE TABLE IF NOT EXISTS etl_conflicts (
  conflict_id UUID PRIMARY KEY,
  change_event_id UUID NOT NULL REFERENCES etl_change_events(change_event_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  table_name TEXT NOT NULL,
  pk TEXT NOT NULL,
  conflict_reason TEXT,
  conflict_columns TEXT,
  resolved BOOLEAN NOT NULL DEFAULT false,
  resolution TEXT,
  resolved_at TIMESTAMPTZ
);

-- ============================
-- STAGING TABLES
-- ============================
CREATE TABLE IF NOT EXISTS stg_sales_orders (
  order_id BIGINT PRIMARY KEY,
  -- 1-based row position from source file (header is row 1, first data row is row 2)
  source_row_num INTEGER,
  order_date DATE,
  region TEXT,
  payment_method TEXT,
  revenue NUMERIC,

  -- fingerprint/hash for change detection
  row_hash TEXT,

  last_change_event_id UUID,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_budget_transactions (
  transaction_id TEXT PRIMARY KEY,
  -- 1-based row position from source file (header is row 1, first data row is row 2)
  source_row_num INTEGER,
  date DATE,
  department TEXT,
  category TEXT,
  region TEXT,
  budget_amount NUMERIC,
  actual_amount NUMERIC,
  payment_method TEXT,

  -- fingerprint/hash for change detection
  row_hash TEXT,

  last_change_event_id UUID,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Upgrade existing DBs (CREATE TABLE IF NOT EXISTS won't add columns)
ALTER TABLE stg_sales_orders
  ADD COLUMN IF NOT EXISTS row_hash TEXT;

ALTER TABLE stg_sales_orders
  ADD COLUMN IF NOT EXISTS source_row_num INTEGER;

ALTER TABLE stg_budget_transactions
  ADD COLUMN IF NOT EXISTS row_hash TEXT;

ALTER TABLE stg_budget_transactions
  ADD COLUMN IF NOT EXISTS source_row_num INTEGER;

-- Helpful indexes for diffing/diagnostics
CREATE INDEX IF NOT EXISTS idx_stg_sales_orders_row_hash
  ON stg_sales_orders(row_hash);

CREATE INDEX IF NOT EXISTS idx_stg_budget_transactions_row_hash
  ON stg_budget_transactions(row_hash);

-- ============================
-- FACT TABLE
-- ============================
CREATE TABLE IF NOT EXISTS fact_finance_monthly (
  month_start DATE NOT NULL,
  department TEXT NOT NULL,
  category TEXT NOT NULL,
  scenario TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  source TEXT NOT NULL,
  last_change_event_id UUID,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (month_start, department, category, scenario, source)
);

CREATE INDEX IF NOT EXISTS idx_fact_month
  ON fact_finance_monthly(month_start);
