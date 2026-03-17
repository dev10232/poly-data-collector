/**
 * Polymarket data collector — subscribes to markets and stores round data.
 * No HTTP server, no WebSocket server for clients.
 * Run: node collect.js (standalone) or started via server.js (combined).
 */

const { GammaClient } = require('./gamma_client');
const { MarketWebSocket } = require('./websocket_client');
const collector = require('./collector');

const COINS = ['ETH', 'BTC', 'SOL', 'XRP'];
const INTERVALS = ['5m'];
const ETA_CHECK_INTERVAL_MS = 1000;

let allMarkets = [];

async function subscribeAllMarkets(polyWs) {
  const oldTokenIds = polyWs.getSubscribedTokenIds();

  allMarkets.length = 0;

  const gamma = new GammaClient();
  const tokenIds = [];

  for (const coin of COINS) {
    for (const interval of INTERVALS) {
      const info = await gamma.getMarketInfo(coin, interval);
      if (!info?.token_ids) continue;

      const market = info.raw;
      const ids = info.token_ids;
      const upId = ids.up ? String(ids.up) : null;
      const downId = ids.down ? String(ids.down) : null;
      const endDate = info.end_date;
      const endMs =
        typeof endDate === 'string'
          ? new Date(endDate).getTime()
          : (endDate || 0) * 1000;

      if (upId) tokenIds.push(upId);
      if (downId) tokenIds.push(downId);

      allMarkets.push({
        coin,
        interval,
        slug: market?.slug,
        end_date: endMs,
        duration_min: interval === '5m' ? 5 : 15,
        up_id: upId,
        down_id: downId,
      });
    }
  }

  if (tokenIds.length > 0) {
    if (oldTokenIds.length === 0) {
      await polyWs.subscribe(tokenIds, true);
    } else {
      await polyWs.closeConnection();
      polyWs.subscribe(tokenIds, true);
    }
  }
  collector.registerMarkets(allMarkets);
}

function anyMarketEnded() {
  const now = Date.now();
  return allMarkets.some((m) => (m.end_date || 0) - now <= 0);
}

async function startCollector() {
  const polyWs = new MarketWebSocket();

  polyWs.onPrice((assetId, midPrice, bestBid, bestAsk) => {
    collector.onPriceUpdate(String(assetId), bestBid, bestAsk);
  });

  polyWs.onError((err) => console.error('[Poly WS]', err.message));

  await subscribeAllMarkets(polyWs);
  polyWs.run();

  setInterval(() => {
    if (anyMarketEnded()) {
      void subscribeAllMarkets(polyWs);
    }
  }, ETA_CHECK_INTERVAL_MS);
}

if (require.main === module) {
  startCollector().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { startCollector };
