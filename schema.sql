-- SQLite schema for stock analysis app
-- 起動時に PRAGMA journal_mode=WAL を別途実行すること

CREATE TABLE IF NOT EXISTS batch_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT,
    finished_at   TEXT,
    status        TEXT CHECK (status IN ('running', 'success', 'error')),
    target_count  INTEGER,
    success_count INTEGER,
    error_count   INTEGER,
    message       TEXT
);

CREATE TABLE IF NOT EXISTS stocks (
    code       TEXT PRIMARY KEY,
    name       TEXT,
    market     TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS daily_quotes (
    code           TEXT,
    date           TEXT,
    open           REAL,
    high           REAL,
    low            REAL,
    close          REAL,
    volume         REAL,
    turnover_value REAL,
    raw_json       TEXT,
    updated_at     TEXT,
    source         TEXT,
    source_version TEXT,
    ingested_at    TEXT,
    PRIMARY KEY (code, date)
);

CREATE TABLE IF NOT EXISTS statements (
    code              TEXT,
    disclosed_date    TEXT,
    net_sales         REAL,
    operating_profit  REAL,
    equity            REAL,
    total_assets      REAL,
    net_income        REAL,
    eps               REAL,
    raw_json          TEXT,
    updated_at        TEXT,
    source            TEXT,
    source_version    TEXT,
    ingested_at       TEXT,
    PRIMARY KEY (code, disclosed_date)
);

CREATE TABLE IF NOT EXISTS dividends (
    code                TEXT,
    record_date         TEXT,
    dividend_per_share  REAL,
    raw_json            TEXT,
    updated_at          TEXT,
    source              TEXT,
    source_version      TEXT,
    ingested_at         TEXT,
    PRIMARY KEY (code, record_date)
);

CREATE TABLE IF NOT EXISTS announcements (
    code           TEXT,
    date           TEXT,
    raw_json       TEXT,
    updated_at     TEXT,
    source         TEXT,
    source_version TEXT,
    ingested_at    TEXT,
    PRIMARY KEY (code, date)
);

CREATE TABLE IF NOT EXISTS judgments (
    batch_run_id INTEGER,
    code         TEXT,
    strategy     TEXT,
    signal       TEXT,
    score        REAL,
    price        REAL,
    as_of        TEXT,
    top_reason   TEXT,
    rules_json   TEXT,
    PRIMARY KEY (batch_run_id, code, strategy)
);

CREATE INDEX IF NOT EXISTS idx_daily_quotes_code_date
    ON daily_quotes (code, date);

CREATE INDEX IF NOT EXISTS idx_judgments_strategy_signal
    ON judgments (strategy, signal);

CREATE INDEX IF NOT EXISTS idx_judgments_batch_run_id
    ON judgments (batch_run_id);

CREATE TABLE IF NOT EXISTS edinet_code_cache (
    security_code TEXT PRIMARY KEY,
    edinet_code   TEXT NOT NULL,
    cached_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS news (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    code                 TEXT NOT NULL,
    published_at         TEXT NOT NULL,
    title                TEXT NOT NULL,
    url                  TEXT NOT NULL,
    summary              TEXT,
    sentiment_score      REAL NOT NULL,
    source               TEXT NOT NULL,
    sentiment_method     TEXT DEFAULT 'rule',
    sentiment_model      TEXT,
    sentiment_confidence REAL,
    UNIQUE(code, url)
);

-- ────────────────────────────────────────────
-- Watermark（銘柄・フィード別の最終取得済み公開日時）
-- ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    code              TEXT NOT NULL,
    feed              TEXT NOT NULL, -- news / quotes / statements など
    last_published_at TEXT,
    last_ingested_at  TEXT NOT NULL,
    PRIMARY KEY (code, feed)
);

CREATE INDEX IF NOT EXISTS idx_news_code_published_at
    ON news (code, published_at DESC);

-- ────────────────────────────────────────────
-- ギャップアップ引け前仕込み戦略（クイックスタート）
-- ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS qs_candidates (
    trade_date        TEXT NOT NULL,
    code              TEXT NOT NULL,
    gap_up_rate       REAL NOT NULL,
    prev_close        REAL NOT NULL,
    day_open          REAL NOT NULL,
    day_high          REAL,
    latest_price      REAL,
    volume_ratio      REAL,
    high_distance     REAL,
    status            TEXT NOT NULL DEFAULT 'picked', -- picked/alive/rejected
    reject_reason     TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS qs_survival_snapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date        TEXT NOT NULL,
    ts_jst            TEXT NOT NULL,
    code              TEXT NOT NULL,
    price             REAL NOT NULL,
    cum_volume        REAL,
    delta_volume      REAL,
    base_price_1500   REAL,
    drop_from_1500    REAL
);

CREATE INDEX IF NOT EXISTS idx_qs_survival_trade_code_ts
    ON qs_survival_snapshots (trade_date, code, ts_jst);

CREATE TABLE IF NOT EXISTS qs_order_signals (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date        TEXT NOT NULL,
    ts_jst            TEXT NOT NULL,
    code              TEXT NOT NULL,
    side              TEXT NOT NULL, -- buy/sell
    signal_type       TEXT NOT NULL, -- entry/exit
    price             REAL NOT NULL,
    reason            TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'new'
);

CREATE INDEX IF NOT EXISTS idx_qs_order_signals_trade_date
    ON qs_order_signals (trade_date, ts_jst);

CREATE TABLE IF NOT EXISTS qs_positions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    code              TEXT NOT NULL,
    entry_date        TEXT NOT NULL,
    entry_ts_jst      TEXT NOT NULL,
    entry_price       REAL NOT NULL,
    allocation_pct    REAL NOT NULL,
    state             TEXT NOT NULL DEFAULT 'open', -- open/closed
    exit_date         TEXT,
    exit_ts_jst       TEXT,
    exit_price        REAL,
    exit_reason       TEXT
);

CREATE INDEX IF NOT EXISTS idx_qs_positions_state
    ON qs_positions (state, code);
