/**
 * Collects market round data: tracks best_ask/best_bid for 6 threshold slots.
 * Each slot: store at left (best_ask <= left), record at right (best_bid >= right) only if store was seen.
 *
 * Slots: (0.30→0.35), (0.35→0.40), (0.40→0.45), (0.45→0.50), (0.50→0.55), (0.55→0.60)
 */

const db = require('./db');

const TOLERANCE = 0.005;

/** 6 slots: [ storeLeft, recordRight ] in 0-1 scale */
const SLOTS = [
  { store: 0.30, record: 0.35 },
  { store: 0.35, record: 0.40 },
  { store: 0.40, record: 0.45 },
  { store: 0.45, record: 0.50 },
  { store: 0.50, record: 0.55 },
  { store: 0.55, record: 0.60 },
];

/** Setters per slot: [ storeUp, storeDown, recordUp, recordDown ] */
const SLOT_SETTERS = [
  [db.setSlot1StoreUp, db.setSlot1StoreDown, db.setSlot1RecordUp, db.setSlot1RecordDown],
  [db.setSlot2StoreUp, db.setSlot2StoreDown, db.setSlot2RecordUp, db.setSlot2RecordDown],
  [db.setSlot3StoreUp, db.setSlot3StoreDown, db.setSlot3RecordUp, db.setSlot3RecordDown],
  [db.setSlot4StoreUp, db.setSlot4StoreDown, db.setSlot4RecordUp, db.setSlot4RecordDown],
  [db.setSlot5StoreUp, db.setSlot5StoreDown, db.setSlot5RecordUp, db.setSlot5RecordDown],
  [db.setSlot6StoreUp, db.setSlot6StoreDown, db.setSlot6RecordUp, db.setSlot6RecordDown],
];

/** round_id -> { storeTs: [6 slots x 2 outcomes], recordTs: [6 slots x 2 outcomes] } */
const activeRounds = new Map();

/** asset_id -> { round_id, outcome: 'Up'|'Down' } */
const assetToRound = new Map();

/**
 * Register a market round for collection.
 */
function registerRound(market) {
  const { coin, duration_min, end_date, up_id, down_id, slug } = market;
  const round_start_ts = end_date - duration_min * 60 * 1000;
  const round_id = `${coin}-${duration_min}-${round_start_ts}`;

  const round = {
    round_id,
    market_id: slug || round_id,
    coin,
    duration: duration_min,
    round_start_ts,
    round_end_ts: end_date,
    up_token_id: up_id,
    down_token_id: down_id,
    storeTs: Array(6)
      .fill(null)
      .map(() => ({ up: null, down: null })),
    recordTs: Array(6)
      .fill(null)
      .map(() => ({ up: null, down: null })),
  };

  activeRounds.set(round_id, round);
  if (up_id) assetToRound.set(String(up_id), { round_id, outcome: 'Up' });
  if (down_id) assetToRound.set(String(down_id), { round_id, outcome: 'Down' });

  db.ensureRound({
    round_id,
    market_id: round.market_id,
    coin,
    duration: duration_min,
    round_start_ts,
    round_end_ts: end_date,
    up_token_id: up_id,
    down_token_id: down_id,
  });
}

function pruneEndedRounds() {
  const now = Date.now();
  for (const [roundId, round] of activeRounds.entries()) {
    if (round.round_end_ts <= now) {
      activeRounds.delete(roundId);
      if (round.up_token_id) assetToRound.delete(String(round.up_token_id));
      if (round.down_token_id) assetToRound.delete(String(round.down_token_id));
    }
  }
}

/**
 * Process a price update. For each slot: store when ask<=left, record when bid>=right (after store).
 */
function onPriceUpdate(assetId, bestBid, bestAsk, ts = Date.now()) {
  const info = assetToRound.get(String(assetId));
  if (!info) return;
  const round = activeRounds.get(info.round_id);
  if (!round) return;

  const { round_id, outcome } = info;
  const o = outcome === 'Up' ? 'up' : 'down';

  for (let i = 0; i < SLOTS.length; i++) {
    const { store: storeVal, record: recordVal } = SLOTS[i];
    const storeTrigger = storeVal + TOLERANCE;
    const recordTrigger = recordVal - TOLERANCE;
    const [setStoreUp, setStoreDown, setRecordUp, setRecordDown] = SLOT_SETTERS[i];

    if (outcome === 'Up') {
      if (bestAsk <= storeTrigger && round.storeTs[i].up == null) {
        round.storeTs[i].up = ts;
        setStoreUp(round_id, ts);
      }
      if (
        bestBid >= recordTrigger &&
        round.recordTs[i].up == null &&
        round.storeTs[i].up != null
      ) {
        round.recordTs[i].up = ts;
        setRecordUp(round_id, ts, bestBid);
      }
    } else {
      if (bestAsk <= storeTrigger && round.storeTs[i].down == null) {
        round.storeTs[i].down = ts;
        setStoreDown(round_id, ts);
      }
      if (
        bestBid >= recordTrigger &&
        round.recordTs[i].down == null &&
        round.storeTs[i].down != null
      ) {
        round.recordTs[i].down = ts;
        setRecordDown(round_id, ts, bestBid);
      }
    }
  }

  pruneEndedRounds();
}

function registerMarkets(markets) {
  pruneEndedRounds();
  for (const m of markets) {
    const round_id = `${m.coin}-${m.duration_min}-${m.end_date - m.duration_min * 60 * 1000}`;
    if (!activeRounds.has(round_id)) {
      registerRound({
        ...m,
        slug: m.slug,
      });
    }
  }
}

module.exports = {
  registerRound,
  registerMarkets,
  onPriceUpdate,
  pruneEndedRounds,
};
