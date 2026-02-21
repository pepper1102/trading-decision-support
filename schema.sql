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
    PRIMARY KEY (code, disclosed_date)
);

CREATE TABLE IF NOT EXISTS dividends (
    code                TEXT,
    record_date         TEXT,
    dividend_per_share  REAL,
    raw_json            TEXT,
    updated_at          TEXT,
    PRIMARY KEY (code, record_date)
);

CREATE TABLE IF NOT EXISTS announcements (
    code       TEXT,
    date       TEXT,
    raw_json   TEXT,
    updated_at TEXT,
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
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT NOT NULL,
    summary         TEXT,
    sentiment_score REAL NOT NULL,
    source          TEXT NOT NULL,
    UNIQUE(code, url)
);

CREATE INDEX IF NOT EXISTS idx_news_code_published_at
    ON news (code, published_at DESC);
