import Database from 'better-sqlite3';

/**
 * Durable projection and settlement outbox. Plain SQL, no ORM. Wide values are
 * read back as BigInt so nothing is silently rounded.
 */
export function openDatabase(path: string): Database.Database {
  const db = new Database(path);
  db.defaultSafeIntegers(true);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = FULL');
  db.pragma('foreign_keys = ON');
  migrate(db);
  return db;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS projection_state (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  order_id           INTEGER PRIMARY KEY,
  account            INTEGER NOT NULL,
  instrument         INTEGER NOT NULL,
  client_order_id    INTEGER NOT NULL,
  side               TEXT    NOT NULL,
  order_type         TEXT    NOT NULL,
  price_ticks        INTEGER NOT NULL,
  total_lots         INTEGER NOT NULL,
  filled_lots        INTEGER NOT NULL,
  state              TEXT    NOT NULL,
  updated_output_seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_by_account ON orders (account, state);

CREATE TABLE IF NOT EXISTS trades (
  trade_id      INTEGER PRIMARY KEY,
  output_seq    INTEGER NOT NULL UNIQUE,
  engine_seq    INTEGER NOT NULL,
  engine_time_ns INTEGER NOT NULL,
  instrument    INTEGER NOT NULL,
  maker_account INTEGER NOT NULL,
  taker_account INTEGER NOT NULL,
  aggressor     TEXT    NOT NULL,
  price_ticks   INTEGER NOT NULL,
  quantity_lots INTEGER NOT NULL,
  settlement_id TEXT REFERENCES settlements (settlement_id)
);
CREATE INDEX IF NOT EXISTS trades_unsettled ON trades (settlement_id, trade_id);

CREATE TABLE IF NOT EXISTS positions (
  account       INTEGER NOT NULL,
  instrument    INTEGER NOT NULL,
  position_lots INTEGER NOT NULL,
  bought_lots   INTEGER NOT NULL,
  sold_lots     INTEGER NOT NULL,
  PRIMARY KEY (account, instrument)
);

CREATE TABLE IF NOT EXISTS risk_rejects (
  reason TEXT PRIMARY KEY,
  count  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settlements (
  settlement_id  TEXT PRIMARY KEY,
  venue          TEXT    NOT NULL,
  status         TEXT    NOT NULL,
  manifest_hash  TEXT    NOT NULL,
  first_trade_id INTEGER NOT NULL,
  last_trade_id  INTEGER NOT NULL,
  trade_count    INTEGER NOT NULL,
  attempts       INTEGER NOT NULL DEFAULT 0,
  receipt        TEXT,
  last_error     TEXT,
  created_at_ms  INTEGER NOT NULL,
  updated_at_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS settlements_by_status ON settlements (status, settlement_id);
`;

function migrate(db: Database.Database): void {
  db.exec(SCHEMA);
  db.prepare("INSERT OR IGNORE INTO projection_state (key, value) VALUES ('last_output_seq', '0')").run();
  db.prepare("INSERT OR IGNORE INTO projection_state (key, value) VALUES ('as_of_engine_seq', '0')").run();
  db.prepare("INSERT OR IGNORE INTO projection_state (key, value) VALUES ('projection_seq', '0')").run();
}

export function readState(db: Database.Database, key: string): bigint {
  const row = db.prepare('SELECT value FROM projection_state WHERE key = ?').get(key) as
    | { value: string }
    | undefined;
  return row === undefined ? 0n : BigInt(row.value);
}

export function writeState(db: Database.Database, key: string, value: bigint): void {
  db.prepare('UPDATE projection_state SET value = ? WHERE key = ?').run(value.toString(), key);
}
