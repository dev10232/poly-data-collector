/**
 * SQLite database for Polymarket round data.
 * Append-only design: one record per market round, updated in-place during round lifetime.
 *
 * 5 threshold slots: (store at left, record at right)
 * - s1: 40 -> 50
 * - s2: 50 -> 62
 * - s3: 45 -> 56
 * - s4: 35 -> 44
 * - s5: 43 -> 54
 */

const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data', 'rounds.db');

let db;

function getDb() {
  if (!db) {
    const fs = require('fs');
    const dir = path.dirname(DB_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    db = new Database(DB_PATH);
    initSchema(db);
    migrateAddSlots(db);
  }
  return db;
}

function initSchema(database) {
  database.exec(`
    CREATE TABLE IF NOT EXISTS market_rounds (
      id TEXT PRIMARY KEY,
      m_id TEXT,
      coin TEXT NOT NULL,
      duration INTEGER NOT NULL,
      start_ts INTEGER NOT NULL,
      end_ts INTEGER NOT NULL,
      up_40_ts INTEGER,
      down_40_ts INTEGER,
      up_60_elapsed INTEGER,
      down_60_elapsed INTEGER,
      s1_price REAL,
      up_20_ts INTEGER,
      down_20_ts INTEGER,
      up_40_elapsed INTEGER,
      down_40_elapsed INTEGER,
      s2_price REAL,
      up_15_ts INTEGER,
      down_15_ts INTEGER,
      up_30_elapsed INTEGER,
      down_30_elapsed INTEGER,
      s3_price REAL,
      up_25_ts INTEGER,
      down_25_ts INTEGER,
      up_50a_elapsed INTEGER,
      down_50a_elapsed INTEGER,
      s4_price REAL,
      up_35_ts INTEGER,
      down_35_ts INTEGER,
      up_50b_elapsed INTEGER,
      down_50b_elapsed INTEGER,
      s5_price REAL,
      created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
      updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
    );
    CREATE INDEX IF NOT EXISTS idx_rounds_coin_duration ON market_rounds(coin, duration);
    CREATE INDEX IF NOT EXISTS idx_rounds_start ON market_rounds(start_ts);
  `);
  migrateFromLegacy(database);
}

/** Migrate from old schema (round_id, etc) to new schema. */
function migrateFromLegacy(database) {
  const cols = database.prepare("PRAGMA table_info(market_rounds)").all();
  const names = cols.map((c) => c.name);

  if (names.includes('id')) return;
  if (!names.includes('round_id')) return;

  const migrate = database.transaction(() => {
    database.exec(`
      CREATE TABLE market_rounds_new (
        id TEXT PRIMARY KEY,
        m_id TEXT,
        coin TEXT NOT NULL,
        duration INTEGER NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER NOT NULL,
        up_40_ts INTEGER,
        down_40_ts INTEGER,
        up_60_elapsed INTEGER,
        down_60_elapsed INTEGER,
        created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
        updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
      );
      INSERT INTO market_rounds_new (id, m_id, coin, duration, start_ts, end_ts, up_40_ts, down_40_ts, up_60_elapsed, down_60_elapsed, created_at, updated_at)
      SELECT
        COALESCE(round_id, ''),
        market_id,
        coin,
        duration,
        COALESCE(round_start_ts, 0),
        COALESCE(round_end_ts, 0),
        elapsed_to_040_up,
        elapsed_to_040_down,
        elapsed_to_060,
        elapsed_to_060,
        created_at,
        updated_at
      FROM market_rounds;
      DROP TABLE market_rounds;
      ALTER TABLE market_rounds_new RENAME TO market_rounds;
      CREATE INDEX IF NOT EXISTS idx_rounds_coin_duration ON market_rounds(coin, duration);
      CREATE INDEX IF NOT EXISTS idx_rounds_start ON market_rounds(start_ts);
    `);
  });
  try {
    migrate();
  } catch (e) {
    if (!e.message.includes('no such column')) throw e;
  }
}

/** Add slot columns to existing tables that don't have them. */
function migrateAddSlots(database) {
  const cols = database.prepare("PRAGMA table_info(market_rounds)").all();
  const names = new Set(cols.map((c) => c.name));

  const toAdd = [
    's1_price REAL',
    'up_20_ts INTEGER', 'down_20_ts INTEGER', 'up_40_elapsed INTEGER', 'down_40_elapsed INTEGER', 's2_price REAL',
    'up_15_ts INTEGER', 'down_15_ts INTEGER', 'up_30_elapsed INTEGER', 'down_30_elapsed INTEGER', 's3_price REAL',
    'up_25_ts INTEGER', 'down_25_ts INTEGER', 'up_50a_elapsed INTEGER', 'down_50a_elapsed INTEGER', 's4_price REAL',
    'up_35_ts INTEGER', 'down_35_ts INTEGER', 'up_50b_elapsed INTEGER', 'down_50b_elapsed INTEGER', 's5_price REAL',
    'up_70_s2_elapsed INTEGER', 'down_70_s2_elapsed INTEGER',
    'up_60_s3_elapsed INTEGER', 'down_60_s3_elapsed INTEGER',
    'up_70_s4_elapsed INTEGER', 'down_70_s4_elapsed INTEGER',
    'up_50_elapsed INTEGER', 'down_50_elapsed INTEGER',
    'up_50_ts INTEGER', 'down_50_ts INTEGER',
    'up_62_elapsed INTEGER', 'down_62_elapsed INTEGER',
    'up_45_ts INTEGER', 'down_45_ts INTEGER',
    'up_56_elapsed INTEGER', 'down_56_elapsed INTEGER',
    'up_44_elapsed INTEGER', 'down_44_elapsed INTEGER',
    'up_43_ts INTEGER', 'down_43_ts INTEGER',
    'up_54_elapsed INTEGER', 'down_54_elapsed INTEGER',
  ];

  for (const colDef of toAdd) {
    const colName = colDef.split(' ')[0];
    if (names.has(colName)) continue;
    try {
      database.exec(`ALTER TABLE market_rounds ADD COLUMN ${colDef}`);
    } catch (e) {
      if (!e.message.includes('duplicate column')) throw e;
    }
  }
}

/**
 * Ensure a round exists, return id.
 */
function ensureRound(row) {
  const d = getDb();
  const now = Date.now();
  const id = row.round_id || row.id;
  const stmt = d.prepare(`
    INSERT INTO market_rounds (
      id, m_id, coin, duration, start_ts, end_ts, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at
  `);
  stmt.run(
    id,
    row.m_id ?? row.market_id ?? null,
    row.coin,
    row.duration,
    row.start_ts ?? row.round_start_ts,
    row.end_ts ?? row.round_end_ts,
    now
  );
  return id;
}

/**
 * Set slot store (left) elapsed for outcome. col e.g. 'up_40_ts', 'down_20_ts'
 */
function setSlotStore(roundId, col, ts) {
  const d = getDb();
  const row = d.prepare('SELECT start_ts FROM market_rounds WHERE id = ?').get(roundId);
  if (!row) return;
  const elapsed = Math.round((ts - row.start_ts) / 1000);
  const stmt = d.prepare(`UPDATE market_rounds SET ${col} = ?, updated_at = ? WHERE id = ?`);
  stmt.run(elapsed, ts, roundId);
}

/**
 * Set slot record (right) elapsed and price.
 */
function setSlotRecord(roundId, elapsedCol, priceCol, ts, price) {
  const d = getDb();
  const row = d.prepare('SELECT start_ts FROM market_rounds WHERE id = ?').get(roundId);
  if (!row) return;
  const elapsed = Math.round((ts - row.start_ts) / 1000);
  d.prepare(`UPDATE market_rounds SET ${elapsedCol} = ?, ${priceCol} = ?, updated_at = ? WHERE id = ?`).run(elapsed, price ?? null, ts, roundId);
}

/** Slot 1: 40→50 */
function setSlot1StoreUp(roundId, ts) { setSlotStore(roundId, 'up_40_ts', ts); }
function setSlot1StoreDown(roundId, ts) { setSlotStore(roundId, 'down_40_ts', ts); }
function setSlot1RecordUp(roundId, ts, price) { setSlotRecord(roundId, 'up_50_elapsed', 's1_price', ts, price); }
function setSlot1RecordDown(roundId, ts, price) { setSlotRecord(roundId, 'down_50_elapsed', 's1_price', ts, price); }

/** Slot 2: 50→62 */
function setSlot2StoreUp(roundId, ts) { setSlotStore(roundId, 'up_50_ts', ts); }
function setSlot2StoreDown(roundId, ts) { setSlotStore(roundId, 'down_50_ts', ts); }
function setSlot2RecordUp(roundId, ts, price) { setSlotRecord(roundId, 'up_62_elapsed', 's2_price', ts, price); }
function setSlot2RecordDown(roundId, ts, price) { setSlotRecord(roundId, 'down_62_elapsed', 's2_price', ts, price); }

/** Slot 3: 45→56 */
function setSlot3StoreUp(roundId, ts) { setSlotStore(roundId, 'up_45_ts', ts); }
function setSlot3StoreDown(roundId, ts) { setSlotStore(roundId, 'down_45_ts', ts); }
function setSlot3RecordUp(roundId, ts, price) { setSlotRecord(roundId, 'up_56_elapsed', 's3_price', ts, price); }
function setSlot3RecordDown(roundId, ts, price) { setSlotRecord(roundId, 'down_56_elapsed', 's3_price', ts, price); }

/** Slot 4: 35→44 */
function setSlot4StoreUp(roundId, ts) { setSlotStore(roundId, 'up_35_ts', ts); }
function setSlot4StoreDown(roundId, ts) { setSlotStore(roundId, 'down_35_ts', ts); }
function setSlot4RecordUp(roundId, ts, price) { setSlotRecord(roundId, 'up_44_elapsed', 's4_price', ts, price); }
function setSlot4RecordDown(roundId, ts, price) { setSlotRecord(roundId, 'down_44_elapsed', 's4_price', ts, price); }

/** Slot 5: 43→54 */
function setSlot5StoreUp(roundId, ts) { setSlotStore(roundId, 'up_43_ts', ts); }
function setSlot5StoreDown(roundId, ts) { setSlotStore(roundId, 'down_43_ts', ts); }
function setSlot5RecordUp(roundId, ts, price) { setSlotRecord(roundId, 'up_54_elapsed', 's5_price', ts, price); }
function setSlot5RecordDown(roundId, ts, price) { setSlotRecord(roundId, 'down_54_elapsed', 's5_price', ts, price); }

/**
 * Search/query rounds.
 */
function queryRounds(options = {}) {
  const d = getDb();
  let sql = 'SELECT * FROM market_rounds WHERE 1=1';
  const params = [];

  if (options.coin) {
    sql += ' AND coin = ?';
    params.push(options.coin);
  }
  if (options.duration != null) {
    sql += ' AND duration = ?';
    params.push(options.duration);
  }
  if (options.fromTs != null) {
    sql += ' AND start_ts >= ?';
    params.push(options.fromTs);
  }
  if (options.toTs != null) {
    sql += ' AND end_ts <= ?';
    params.push(options.toTs);
  }

  sql += ' ORDER BY start_ts DESC';
  if (options.offset != null) {
    sql += ' LIMIT ? OFFSET ?';
    params.push(options.limit ?? 20, options.offset);
  } else if (options.limit) {
    sql += ' LIMIT ?';
    params.push(options.limit);
  }

  return d.prepare(sql).all(...params);
}

function countRounds(options = {}) {
  const d = getDb();
  let sql = 'SELECT COUNT(*) as total FROM market_rounds WHERE 1=1';
  const params = [];

  if (options.coin) {
    sql += ' AND coin = ?';
    params.push(options.coin);
  }
  if (options.duration != null) {
    sql += ' AND duration = ?';
    params.push(options.duration);
  }
  if (options.fromTs != null) {
    sql += ' AND start_ts >= ?';
    params.push(options.fromTs);
  }
  if (options.toTs != null) {
    sql += ' AND end_ts <= ?';
    params.push(options.toTs);
  }

  const row = d.prepare(sql).get(...params);
  return row ? row.total : 0;
}

function getCoins() {
  const d = getDb();
  const rows = d.prepare('SELECT DISTINCT coin FROM market_rounds ORDER BY coin').all();
  return rows.map((r) => r.coin);
}

function getRound(roundId) {
  return getDb().prepare('SELECT * FROM market_rounds WHERE id = ?').get(roundId);
}

module.exports = {
  DB_PATH,
  getDb,
  ensureRound,
  setSlot1StoreUp,
  setSlot1StoreDown,
  setSlot1RecordUp,
  setSlot1RecordDown,
  setSlot2StoreUp,
  setSlot2StoreDown,
  setSlot2RecordUp,
  setSlot2RecordDown,
  setSlot3StoreUp,
  setSlot3StoreDown,
  setSlot3RecordUp,
  setSlot3RecordDown,
  setSlot4StoreUp,
  setSlot4StoreDown,
  setSlot4RecordUp,
  setSlot4RecordDown,
  setSlot5StoreUp,
  setSlot5StoreDown,
  setSlot5RecordUp,
  setSlot5RecordDown,
  setSlotStore,
  setSlotRecord,
  queryRounds,
  countRounds,
  getCoins,
  getRound,
};
