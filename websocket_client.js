/**
 * WebSocket Client - Real-time Market Data for Polymarket
 *
 * Subscribes to Polymarket CLOB WebSocket and streams real-time prices
 * for given clobTokenIds.
 *
 * Example:
 *   const { MarketWebSocket } = require('./websocket_client');
 *
 *   const tokenIds = ['token_id_1', 'token_id_2'];  // clobTokenIds from Gamma API
 *   const ws = new MarketWebSocket({ tokenIds });
 *
 *   ws.onPrice((assetId, midPrice, bestBid, bestAsk) => {
 *     console.log(`${assetId.slice(0, 20)}... mid=${midPrice}`);
 *   });
 *
 *   await ws.run();
 */

const WebSocket = require('ws');

const WSS_MARKET_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

/**
 * @typedef {Object} OrderbookLevel
 * @property {number} price
 * @property {number} size
 */

/**
 * @typedef {Object} OrderbookSnapshot
 * @property {string} asset_id
 * @property {string} market
 * @property {number} timestamp
 * @property {OrderbookLevel[]} bids
 * @property {OrderbookLevel[]} asks
 * @property {string} hash
 * @property {number} best_bid
 * @property {number} best_ask
 * @property {number} mid_price
 */

/**
 * Create OrderbookSnapshot from WebSocket book message.
 *
 * @param {Record<string, any>} msg
 * @returns {OrderbookSnapshot}
 */
function parseOrderbookSnapshot(msg) {
  const bids = (msg.bids || [])
    .map((b) => ({
      price: Number(b.price),
      size: Number(b.size),
    }))
    .sort((a, b) => b.price - a.price);

  const asks = (msg.asks || [])
    .map((a) => ({
      price: Number(a.price),
      size: Number(a.size),
    }))
    .sort((a, b) => a.price - b.price);

  const bestBid = bids.length > 0 ? bids[0].price : 0;
  const bestAsk = asks.length > 0 ? asks[0].price : 1;

  let midPrice = 0.5;
  if (bestBid > 0 && bestAsk < 1) {
    midPrice = (bestBid + bestAsk) / 2;
  } else if (bestBid > 0) {
    midPrice = bestBid;
  } else if (bestAsk < 1) {
    midPrice = bestAsk;
  }

  return {
    asset_id: msg.asset_id || '',
    market: msg.market || '',
    timestamp: parseInt(msg.timestamp || '0', 10),
    bids,
    asks,
    hash: msg.hash || '',
    best_bid: bestBid,
    best_ask: bestAsk,
    mid_price: midPrice,
  };
}

/**
 * Market WebSocket client for Polymarket.
 *
 * Accepts clobTokenIds, subscribes, and emits real-time prices via callbacks.
 */
class MarketWebSocket {
  /**
   * @param {Object|string[]} [options] - Options object, or tokenIds array for convenience
   * @param {string[]} [options.tokenIds] - clobTokenIds to subscribe to
   * @param {string} [options.url] - WebSocket URL
   * @param {number} [options.reconnectInterval=5000] - ms between reconnect attempts
   * @param {number} [options.pingInterval=20000] - ms between pings
   * @param {boolean} [options.customFeatureEnabled=true] - enable best_bid_ask, new_market, market_resolved
   */
  constructor(options = {}) {
    const opts = Array.isArray(options) ? { tokenIds: options } : options;
    const {
      tokenIds = [],
      url = WSS_MARKET_URL,
      reconnectInterval = 5000,
      pingInterval = 20000,
      customFeatureEnabled = true,
    } = opts;

    this.url = url;
    this.reconnectInterval = reconnectInterval;
    this.pingInterval = pingInterval;
    this.customFeatureEnabled = customFeatureEnabled;

    this._ws = null;
    this._running = false;
    this._subscribedTokenIds = new Set(tokenIds);
    this._orderbooks = {};
    this._pingTimer = null;
    this._reconnectTimer = null;

    this._onPrice = null;
    this._onBook = null;
    this._onTrade = null;
    this._onError = null;
    this._onConnect = null;
    this._onDisconnect = null;
  }

  /** @returns {boolean} */
  get isConnected() {
    return this._ws !== null && this._ws.readyState === WebSocket.OPEN;
  }

  /** @returns {string[]} Currently subscribed token IDs */
  getSubscribedTokenIds() {
    return Array.from(this._subscribedTokenIds);
  }

  /** @returns {Record<string, OrderbookSnapshot>} Cached orderbooks by asset_id */
  get orderbooks() {
    return { ...this._orderbooks };
  }

  /**
   * Get cached orderbook for an asset.
   *
   * @param {string} assetId
   * @returns {OrderbookSnapshot | undefined}
   */
  getOrderbook(assetId) {
    return this._orderbooks[assetId];
  }

  /**
   * Get mid price for an asset.
   *
   * @param {string} assetId
   * @returns {number}
   */
  getMidPrice(assetId) {
    const ob = this._orderbooks[assetId];
    return ob ? ob.mid_price : 0;
  }

  /**
   * Set callback for price updates. Receives: (assetId, midPrice, bestBid, bestAsk).
   *
   * @param {(assetId: string, midPrice: number, bestBid: number, bestAsk: number) => void} cb
   * @returns {(assetId: string, midPrice: number, bestBid: number, bestAsk: number) => void}
   */
  onPrice(cb) {
    this._onPrice = cb;
    return cb;
  }

  /**
   * Set callback for full orderbook updates.
   *
   * @param {(snapshot: OrderbookSnapshot) => void} cb
   * @returns {(snapshot: OrderbookSnapshot) => void}
   */
  onBook(cb) {
    this._onBook = cb;
    return cb;
  }

  /**
   * Set callback for last trade events.
   *
   * @param {(data: Record<string, any>) => void} cb
   * @returns {(data: Record<string, any>) => void}
   */
  onTrade(cb) {
    this._onTrade = cb;
    return cb;
  }

  /**
   * Set callback for errors.
   *
   * @param {(err: Error) => void} cb
   * @returns {(err: Error) => void}
   */
  onError(cb) {
    this._onError = cb;
    return cb;
  }

  /**
   * Set callback for connect.
   *
   * @param {() => void} cb
   * @returns {() => void}
   */
  onConnect(cb) {
    this._onConnect = cb;
    return cb;
  }

  /**
   * Set callback for disconnect.
   *
   * @param {() => void} cb
   * @returns {() => void}
   */
  onDisconnect(cb) {
    this._onDisconnect = cb;
    return cb;
  }

  /**
   * Subscribe to market data for given clobTokenIds.
   *
   * @param {string[]} tokenIds - clobTokenIds (asset_ids)
   * @param {boolean} [replace=false] - if true, replace existing subscriptions
   * @returns {Promise<boolean>}
   */
  async subscribe(tokenIds, replace = false) {
    if (!tokenIds || tokenIds.length === 0) return false;

    if (replace) {
      this._subscribedTokenIds.clear();
      this._orderbooks = {};
    }

    tokenIds.forEach((id) => this._subscribedTokenIds.add(id));

    if (!this.isConnected) return true;

    const msg = JSON.stringify({
      assets_ids: Array.from(this._subscribedTokenIds),
      type: 'market',
      ...(this.customFeatureEnabled && { custom_feature_enabled: true }),
    });

    return new Promise((resolve) => {
      try {
        this._ws.send(msg);
        resolve(true);
      } catch (e) {
        this._emitError(e);
        resolve(false);
      }
    });
  }

  /**
   * Subscribe to additional token IDs.
   *
   * @param {string[]} tokenIds
   * @returns {Promise<boolean>}
   */
  async subscribeMore(tokenIds) {
    if (!tokenIds || tokenIds.length === 0) return false;

    tokenIds.forEach((id) => this._subscribedTokenIds.add(id));

    if (!this.isConnected) return true;

    const msg = JSON.stringify({
      assets_ids: tokenIds,
      operation: 'subscribe',
      ...(this.customFeatureEnabled && { custom_feature_enabled: true }),
    });

    return new Promise((resolve) => {
      try {
        this._ws.send(msg);
        resolve(true);
      } catch (e) {
        this._emitError(e);
        resolve(false);
      }
    });
  }

  /**
   * Unsubscribe from token IDs.
   *
   * @param {string[]} tokenIds
   * @returns {Promise<boolean>}
   */
  async unsubscribe(tokenIds) {
    if (!this.isConnected || !tokenIds || tokenIds.length === 0) return false;

    tokenIds.forEach((id) => this._subscribedTokenIds.delete(id));

    const msg = JSON.stringify({
      assets_ids: tokenIds,
      operation: 'unsubscribe',
    });

    return new Promise((resolve) => {
      try {
        this._ws.send(msg);
        tokenIds.forEach((id) => delete this._orderbooks[id]);
        resolve(true);
      } catch (e) {
        this._emitError(e);
        resolve(false);
      }
    });
  }

  /**
   * Connect to WebSocket.
   *
   * @returns {Promise<boolean>}
   */
  connect() {
    return new Promise((resolve) => {
      let resolved = false;
      const doResolve = (ok) => {
        if (!resolved) {
          resolved = true;
          resolve(ok);
        }
      };

      try {
        this._ws = new WebSocket(this.url);

        this._ws.once('open', () => {
          this._startPing();
          if (this._onConnect) this._onConnect();
          doResolve(true);
        });

        this._ws.once('error', (err) => {
          this._emitError(err);
          doResolve(false);
        });

        this._ws.once('close', () => {
          if (!resolved) doResolve(false);
          this._stopPing();
          this._ws = null;
          if (this._onDisconnect) this._onDisconnect();
        });

        this._ws.on('message', (data) => this._handleMessage(data));
      } catch (e) {
        this._emitError(e);
        doResolve(false);
      }
    });
  }

  /**
   * Close the connection without stopping. The run() loop will reconnect.
   * Returns a Promise that resolves when the connection is fully closed.
   * Use this to force reconnect with updated subscriptions.
   */
  closeConnection() {
    this._stopPing();
    if (!this._ws) return Promise.resolve();
    const ws = this._ws;
    this._ws = null;
    if (this._onDisconnect) this._onDisconnect();
    return new Promise((resolve) => {
      ws.once('close', resolve);
      ws.close();
    });
  }

  /**
   * Disconnect and stop the client.
   */
  async disconnect() {
    this._running = false;
    this._stopPing();
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
    if (this._onDisconnect) this._onDisconnect();
  }

  /**
   * Run the client: connect, subscribe, and process messages.
   * Reconnects automatically on disconnect.
   *
   * @param {boolean} [autoReconnect=true]
   */
  async run(autoReconnect = true) {
    this._running = true;

    while (this._running) {
      const connected = await this.connect();
      if (!connected) {
        if (autoReconnect) {
          await this._sleep(this.reconnectInterval);
          continue;
        }
        break;
      }

      if (this._subscribedTokenIds.size > 0) {
        await this.subscribe(Array.from(this._subscribedTokenIds));
      }

      await new Promise((resolve) => {
        if (!this._ws) {
          resolve();
          return;
        }
        this._ws.once('close', resolve);
      });

      if (!this._running) break;

      if (autoReconnect) {
        await this._sleep(this.reconnectInterval);
      } else {
        break;
      }
    }
  }

  /** Stop the client. */
  stop() {
    this._running = false;
  }

  _emitError(err) {
    console.log(err);
    if (this._onError)
      this._onError(err instanceof Error ? err : new Error(String(err)));
  }

  _startPing() {
    this._stopPing();
    this._pingTimer = setInterval(() => {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        this._ws.ping();
      }
    }, this.pingInterval);
  }

  _stopPing() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  _handleMessage(data) {
    let parsed;
    try {
      parsed = JSON.parse(data.toString());
    } catch (e) {
      console.log(data.toString());
      this._emitError(new Error(`Failed to parse message: ${e.message}`));
      return;
    }

    const items = Array.isArray(parsed) ? parsed : [parsed];

    for (const item of items) {
      this._handleEvent(item);
    }
  }

  _handleEvent(data) {
    const eventType = data.event_type || '';

    switch (eventType) {
      case 'book': {
        const snapshot = parseOrderbookSnapshot(data);
        this._orderbooks[snapshot.asset_id] = snapshot;
        this._emitPrice(
          snapshot.asset_id,
          snapshot.mid_price,
          snapshot.best_bid,
          snapshot.best_ask,
        );
        if (this._onBook) this._onBook(snapshot);
        break;
      }

      case 'price_change': {
        const changes = data.price_changes || [];
        for (const pc of changes) {
          const assetId = pc.asset_id || '';
          const bestBid = Number(pc.best_bid ?? 0);
          const bestAsk = Number(pc.best_ask ?? 1);
          const mid =
            bestBid > 0 && bestAsk < 1
              ? (bestBid + bestAsk) / 2
              : bestBid > 0
                ? bestBid
                : bestAsk < 1
                  ? bestAsk
                  : 0.5;
          this._emitPrice(assetId, mid, bestBid, bestAsk);
          if (this._orderbooks[assetId]) {
            this._orderbooks[assetId] = {
              ...this._orderbooks[assetId],
              best_bid: bestBid,
              best_ask: bestAsk,
              mid_price: mid,
            };
          }
        }
        break;
      }

      case 'best_bid_ask': {
        const assetId = data.asset_id || '';
        const bestBid = Number(data.best_bid ?? 0);
        const bestAsk = Number(data.best_ask ?? 1);
        const mid =
          bestBid > 0 && bestAsk < 1
            ? (bestBid + bestAsk) / 2
            : bestBid > 0
              ? bestBid
              : bestAsk < 1
                ? bestAsk
                : 0.5;
        this._emitPrice(assetId, mid, bestBid, bestAsk);
        if (this._orderbooks[assetId]) {
          this._orderbooks[assetId] = {
            ...this._orderbooks[assetId],
            best_bid: bestBid,
            best_ask: bestAsk,
            mid_price: mid,
          };
        }
        break;
      }

      case 'last_trade_price':
        if (this._onTrade) this._onTrade(data);
        break;

      case 'tick_size_change':
        break;

      default:
        break;
    }
  }

  _emitPrice(assetId, midPrice, bestBid, bestAsk) {
    if (this._onPrice) {
      try {
        this._onPrice(assetId, midPrice, bestBid, bestAsk);
      } catch (e) {
        this._emitError(e);
      }
    }
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = {
  MarketWebSocket,
  WSS_MARKET_URL,
  parseOrderbookSnapshot,
};
