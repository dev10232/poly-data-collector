/**
 * SQLite database for Polymarket round data.
 * Append-only design: one record per market round, updated in-place during round lifetime.
 *
 * 6 threshold slots: (store at left, record at right)
 * - s1: 0.30 -> 0.35
 * - s2: 0.35 -> 0.40
 * - s3: 0.40 -> 0.45
 * - s4: 0.45 -> 0.50
 * - s5: 0.50 -> 0.55
 * - s6: 0.55 -> 0.60
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
    'slot1_us INTEGER', 'slot1_ds INTEGER', 'slot1_ur INTEGER', 'slot1_dr INTEGER',
    'slot2_us INTEGER', 'slot2_ds INTEGER', 'slot2_ur INTEGER', 'slot2_dr INTEGER',
    'slot3_us INTEGER', 'slot3_ds INTEGER', 'slot3_ur INTEGER', 'slot3_dr INTEGER',
    'slot4_us INTEGER', 'slot4_ds INTEGER', 'slot4_ur INTEGER', 'slot4_dr INTEGER',
    'slot5_us INTEGER', 'slot5_ds INTEGER', 'slot5_ur INTEGER', 'slot5_dr INTEGER',
    'slot6_us INTEGER', 'slot6_ds INTEGER', 'slot6_ur INTEGER', 'slot6_dr INTEGER',
    's6_price REAL',
    's1_up_price REAL', 's1_down_price REAL',
    's2_up_price REAL', 's2_down_price REAL',
    's3_up_price REAL', 's3_down_price REAL',
    's4_up_price REAL', 's4_down_price REAL',
    's5_up_price REAL', 's5_down_price REAL',
    's6_up_price REAL', 's6_down_price REAL',
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

/**
 * Set record elapsed + side-specific sell price + legacy s{n}_price in one update.
 */
function setSlotRecordWithSides(roundId, elapsedCol, ts, price, n, side) {
  const d = getDb();
  const row = d.prepare('SELECT start_ts FROM market_rounds WHERE id = ?').get(roundId);
  if (!row) return;
  const elapsed = Math.round((ts - row.start_ts) / 1000);
  const upCol = `s${n}_up_price`;
  const downCol = `s${n}_down_price`;
  const legCol = `s${n}_price`;
  const p = price ?? null;
  if (side === 'up') {
    d.prepare(
      `UPDATE market_rounds SET ${elapsedCol} = ?, ${upCol} = ?, ${legCol} = ?, updated_at = ? WHERE id = ?`
    ).run(elapsed, p, p, ts, roundId);
  } else {
    d.prepare(
      `UPDATE market_rounds SET ${elapsedCol} = ?, ${downCol} = ?, ${legCol} = ?, updated_at = ? WHERE id = ?`
    ).run(elapsed, p, p, ts, roundId);
  }
}

/** Slot 1: 0.30→0.35 */
function setSlot1StoreUp(roundId, ts) { setSlotStore(roundId, 'slot1_us', ts); }
function setSlot1StoreDown(roundId, ts) { setSlotStore(roundId, 'slot1_ds', ts); }
function setSlot1RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot1_ur', ts, price, 1, 'up');
}
function setSlot1RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot1_dr', ts, price, 1, 'down');
}

/** Slot 2: 0.35→0.40 */
function setSlot2StoreUp(roundId, ts) { setSlotStore(roundId, 'slot2_us', ts); }
function setSlot2StoreDown(roundId, ts) { setSlotStore(roundId, 'slot2_ds', ts); }
function setSlot2RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot2_ur', ts, price, 2, 'up');
}
function setSlot2RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot2_dr', ts, price, 2, 'down');
}

/** Slot 3: 0.40→0.45 */
function setSlot3StoreUp(roundId, ts) { setSlotStore(roundId, 'slot3_us', ts); }
function setSlot3StoreDown(roundId, ts) { setSlotStore(roundId, 'slot3_ds', ts); }
function setSlot3RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot3_ur', ts, price, 3, 'up');
}
function setSlot3RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot3_dr', ts, price, 3, 'down');
}

/** Slot 4: 0.45→0.50 */
function setSlot4StoreUp(roundId, ts) { setSlotStore(roundId, 'slot4_us', ts); }
function setSlot4StoreDown(roundId, ts) { setSlotStore(roundId, 'slot4_ds', ts); }
function setSlot4RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot4_ur', ts, price, 4, 'up');
}
function setSlot4RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot4_dr', ts, price, 4, 'down');
}

/** Slot 5: 0.50→0.55 */
function setSlot5StoreUp(roundId, ts) { setSlotStore(roundId, 'slot5_us', ts); }
function setSlot5StoreDown(roundId, ts) { setSlotStore(roundId, 'slot5_ds', ts); }
function setSlot5RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot5_ur', ts, price, 5, 'up');
}
function setSlot5RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot5_dr', ts, price, 5, 'down');
}

/** Slot 6: 0.55→0.60 */
function setSlot6StoreUp(roundId, ts) { setSlotStore(roundId, 'slot6_us', ts); }
function setSlot6StoreDown(roundId, ts) { setSlotStore(roundId, 'slot6_ds', ts); }
function setSlot6RecordUp(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot6_ur', ts, price, 6, 'up');
}
function setSlot6RecordDown(roundId, ts, price) {
  setSlotRecordWithSides(roundId, 'slot6_dr', ts, price, 6, 'down');
}

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

/** Slot configs: store%, record%, $2 buy, sell $ at record/store ratio */
const PROFIT_SLOTS = [
  { name: '0.30→0.35', store: 0.3, record: 0.35, cost: 2, sellAmount: 2 * (0.35 / 0.3) },
  { name: '0.35→0.40', store: 0.35, record: 0.4, cost: 2, sellAmount: 2 * (0.4 / 0.35) },
  { name: '0.40→0.45', store: 0.4, record: 0.45, cost: 2, sellAmount: 2 * (0.45 / 0.4) },
  { name: '0.45→0.50', store: 0.45, record: 0.5, cost: 2, sellAmount: 2 * (0.5 / 0.45) },
  { name: '0.50→0.55', store: 0.5, record: 0.55, cost: 2, sellAmount: 2 * (0.55 / 0.5) },
  { name: '0.55→0.60', store: 0.55, record: 0.6, cost: 2, sellAmount: 2 * (0.6 / 0.55) },
];

const PROFIT_SLOT_COLS = [
  ['slot1_us', 'slot1_ds', 'slot1_ur', 'slot1_dr'],
  ['slot2_us', 'slot2_ds', 'slot2_ur', 'slot2_dr'],
  ['slot3_us', 'slot3_ds', 'slot3_ur', 'slot3_dr'],
  ['slot4_us', 'slot4_ds', 'slot4_ur', 'slot4_dr'],
  ['slot5_us', 'slot5_ds', 'slot5_ur', 'slot5_dr'],
  ['slot6_us', 'slot6_ds', 'slot6_ur', 'slot6_dr'],
];

function buildProfitResult(bySlot) {
  const round2 = (n) => Math.round(n * 100) / 100;
  let totalSpent = 0;
  let totalWon = 0;
  for (const s of bySlot) {
    totalSpent += s.spent;
    totalWon += s.won;
  }
  let minCapital = 0;
  for (const slot of bySlot) {
    const positions = slot.positions;
    let maxConcurrent = 0;
    const events = [];
    for (const p of positions) {
      events.push({ t: p.openStart, delta: 1 });
      events.push({ t: p.openEnd, delta: -1 });
    }
    events.sort((a, b) => a.t - b.t || a.delta - b.delta);
    let cur = 0;
    for (const e of events) {
      cur += e.delta;
      if (cur > maxConcurrent) maxConcurrent = cur;
    }
    minCapital += maxConcurrent * 2;
  }
  return {
    totalSpent: round2(totalSpent),
    totalWon: round2(totalWon),
    totalProfit: round2(totalWon - totalSpent),
    minCapital: round2(minCapital),
    bySlot: bySlot.map((s) => ({
      name: s.name,
      spent: round2(s.spent),
      won: round2(s.won),
      profit: round2(s.won - s.spent),
    })),
  };
}

/**
 * Profit stats: upOnly, downOnly, firstOnly (first store wins), total (both sides).
 * @param {{ coin?: string, duration?: number, fromTs?: number, toTs?: number, buyTimeLimit?: number, slots?: number[] }} options
 */
function calculateProfit(options = {}) {
  const { buyTimeLimit, slots: slotsParam, ...queryOpts } = options;
  const rows = queryRounds({ ...queryOpts, limit: 100000 });
  const limitSec =
    buyTimeLimit != null && Number(buyTimeLimit) > 0 ? Number(buyTimeLimit) : null;
  const slotIndices =
    Array.isArray(slotsParam) && slotsParam.length > 0
      ? slotsParam.filter((i) => i >= 0 && i < PROFIT_SLOTS.length)
      : [...Array(PROFIT_SLOTS.length).keys()];

  const selectedSlots = slotIndices.map((i) => ({
    cfg: PROFIT_SLOTS[i],
    cols: PROFIT_SLOT_COLS[i],
    origIdx: i,
  }));

  const initSlots = () =>
    selectedSlots.map(({ cfg }) => ({
      name: cfg.name,
      spent: 0,
      won: 0,
      positions: [],
    }));

  const upOnly = initSlots();
  const downOnly = initSlots();
  const firstOnly = initSlots();
  const totalSlots = initSlots();

  for (const r of rows) {
    const startTs = r.start_ts;
    const endTs = r.end_ts;

    for (let i = 0; i < selectedSlots.length; i++) {
      const { cfg, cols } = selectedSlots[i];
      const s = i;

      const upStore = r[cols[0]];
      const downStore = r[cols[1]];
      const upRecord = r[cols[2]];
      const downRecord = r[cols[3]];
      const upStoreNum = upStore != null && upStore !== '' ? Number(upStore) : null;
      const downStoreNum = downStore != null && downStore !== '' ? Number(downStore) : null;
      const hasUpStore = upStoreNum != null;
      const hasDownStore = downStoreNum != null;
      const hasUpRecord = upRecord != null && upRecord !== '';
      const hasDownRecord = downRecord != null && downRecord !== '';

      const withinBuyWindow = (storeElapsed) => !limitSec || storeElapsed <= limitSec;

      const addPosition = (slotArr, storeVal, hasRecord, recordVal) => {
        const storeElapsed = Number(storeVal) || 0;
        if (!withinBuyWindow(storeElapsed)) return;
        const openStart = startTs + storeElapsed * 1000;
        const openEnd = hasRecord ? startTs + (Number(recordVal) || 0) * 1000 : endTs;
        slotArr[s].positions.push({ openStart, openEnd });
        slotArr[s].spent += cfg.cost;
        if (hasRecord) slotArr[s].won += cfg.sellAmount;
      };

      if (hasUpStore && withinBuyWindow(upStoreNum)) {
        upOnly[s].spent += cfg.cost;
        upOnly[s].won += hasUpRecord ? cfg.sellAmount : 0;
        upOnly[s].positions.push({
          openStart: startTs + upStoreNum * 1000,
          openEnd: hasUpRecord ? startTs + Number(upRecord) * 1000 : endTs,
        });
      }
      if (hasDownStore && withinBuyWindow(downStoreNum)) {
        downOnly[s].spent += cfg.cost;
        downOnly[s].won += hasDownRecord ? cfg.sellAmount : 0;
        downOnly[s].positions.push({
          openStart: startTs + downStoreNum * 1000,
          openEnd: hasDownRecord ? startTs + Number(downRecord) * 1000 : endTs,
        });
      }
      if (hasUpStore || hasDownStore) {
        const totalArr = totalSlots;
        if (hasUpStore) addPosition(totalArr, upStore, hasUpRecord, upRecord);
        if (hasDownStore) addPosition(totalArr, downStore, hasDownRecord, downRecord);
      }

      if (hasUpStore && hasDownStore) {
        const firstIsUp = upStoreNum <= downStoreNum;
        if (firstIsUp) {
          addPosition(firstOnly, upStore, hasUpRecord, upRecord);
        } else {
          addPosition(firstOnly, downStore, hasDownRecord, downRecord);
        }
      } else if (hasUpStore) {
        addPosition(firstOnly, upStore, hasUpRecord, upRecord);
      } else if (hasDownStore) {
        addPosition(firstOnly, downStore, hasDownRecord, downRecord);
      }
    }
  }

  return {
    upOnly: buildProfitResult(upOnly),
    downOnly: buildProfitResult(downOnly),
    firstOnly: buildProfitResult(firstOnly),
    total: buildProfitResult(totalSlots),
  };
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
  setSlot6StoreUp,
  setSlot6StoreDown,
  setSlot6RecordUp,
  setSlot6RecordDown,
  setSlotStore,
  setSlotRecord,
  queryRounds,
  countRounds,
  getCoins,
  getRound,
  calculateProfit,
};
