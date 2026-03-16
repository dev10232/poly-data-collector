const { GammaClient } = require('./gamma_client');
const { MarketWebSocket } = require('./websocket_client');

async function main() {
  const gamma = new GammaClient();

  // Get current market and extract clobTokenIds
  const market = await gamma.getCurrent15mMarket('ETH');
  if (!market) {
    console.log('No active 15m ETH market');
    return;
  }

  const tokenIds = gamma.parseTokenIds(market);
  const clobTokenIds = [tokenIds.up, tokenIds.down].filter(Boolean);
  const tokenIdsWithLabels = {};
  clobTokenIds.forEach((id, index) => {
    tokenIdsWithLabels[id] = index === 0 ? 'eth up' : 'eth down';
  });
  if (clobTokenIds.length === 0) {
    console.log('No token IDs in market');
    return;
  }

  const ws = new MarketWebSocket({ tokenIds: clobTokenIds });

  ws.onPrice((assetId, midPrice, bestBid, bestAsk) => {
    const short = assetId.slice(0, 16) + '...';
    // console.log(
    //   `${short} mid=${midPrice.toFixed(2)} bid=${bestBid.toFixed(2)} ask=${bestAsk.toFixed(2)}`,
    // );
  });

  ws.onError((err) => console.error('WS error:', err.message));
  ws.onConnect(() => console.log('WebSocket connected'));

  setTimeout(() => ws.stop(), 30_000);
  await ws.run();
  await ws.disconnect();
}

main().catch(console.error);
