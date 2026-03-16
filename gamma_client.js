/**
 * Gamma API Client - Market Discovery for Polymarket
 *
 * Provides access to the Gamma API for discovering active markets,
 * including 5-minute and 15-minute Up/Down markets for crypto assets.
 *
 * Example:
 *   const { GammaClient } = require('./gamma_client');
 *
 *   const client = new GammaClient();
 *   const market = client.getCurrent15mMarket('ETH');
 *   console.log(market?.slug, market?.clobTokenIds);
 *
 *   // 5-minute markets
 *   const market5m = client.getCurrent5mMarket('BTC');
 */

const DEFAULT_HOST = 'https://gamma-api.polymarket.com';

/** Supported coins and their slug prefixes by interval */
const COIN_SLUGS = {
  5: {
    BTC: 'btc-updown-5m',
    ETH: 'eth-updown-5m',
    SOL: 'sol-updown-5m',
    XRP: 'xrp-updown-5m',
  },
  15: {
    BTC: 'btc-updown-15m',
    ETH: 'eth-updown-15m',
    SOL: 'sol-updown-15m',
    XRP: 'xrp-updown-15m',
  },
};

class GammaClient {
  /**
   * Client for Polymarket's Gamma API.
   *
   * Used to discover markets and get market metadata.
   *
   * @param {string} host - Gamma API host URL
   * @param {number} timeout - Request timeout in ms
   */
  constructor(host = DEFAULT_HOST, timeout = 10000) {
    this.host = host.replace(/\/$/, '');
    this.timeout = timeout;
  }

  /**
   * Get market data by slug.
   *
   * @param {string} slug - Market slug (e.g., "eth-updown-15m-1766671200")
   * @returns {Promise<Record<string, any> | null>} Market data or null if not found
   */
  async getMarketBySlug(slug) {
    const url = `${this.host}/markets/slug/${slug}`;
    try {
      const controller = new AbortController();
      const id = setTimeout(() => controller.abort(), this.timeout);
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(id);
      if (res.ok) return res.json();
      return null;
    } catch {
      return null;
    }
  }

  /**
   * Get the current active 15-minute market for a coin.
   *
   * @param {string} coin - Coin symbol (BTC, ETH, SOL, XRP)
   * @returns {Promise<Record<string, any> | null>}
   */
  async getCurrent15mMarket(coin) {
    return this._getCurrentMarket(coin, 15);
  }

  /**
   * Get the current active 5-minute market for a coin.
   *
   * @param {string} coin - Coin symbol (BTC, ETH, SOL, XRP)
   * @returns {Promise<Record<string, any> | null>}
   */
  async getCurrent5mMarket(coin) {
    return this._getCurrentMarket(coin, 5);
  }

  /**
   * Get the current active market for a coin and interval.
   *
   * @param {string} coin - Coin symbol
   * @param {5|15} interval - Window size in minutes (5 or 15)
   * @returns {Promise<Record<string, any> | null>}
   */
  async _getCurrentMarket(coin, interval) {
    const upper = coin.toUpperCase();
    const slugs = COIN_SLUGS[interval];
    if (!slugs || !(upper in slugs)) {
      throw new Error(
        `Unsupported coin: ${coin}. Use: ${Object.keys(slugs || COIN_SLUGS[15]).join(', ')}`
      );
    }
    const prefix = slugs[upper];
    const windowSec = interval * 60;

    const now = new Date();
    const minute = Math.floor(now.getUTCMinutes() / interval) * interval;
    const currentWindow = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      now.getUTCHours(),
      minute,
      0,
      0
    ));
    let currentTs = Math.floor(currentWindow.getTime() / 1000);

    for (const delta of [0, windowSec, -windowSec]) {
      const ts = currentTs + delta;
      const slug = `${prefix}-${ts}`;
      const market = await this.getMarketBySlug(slug);
      if (market?.acceptingOrders) return market;
    }
    return null;
  }

  /**
   * Get the next upcoming 15-minute market for a coin.
   *
   * @param {string} coin - Coin symbol
   * @returns {Promise<Record<string, any> | null>}
   */
  async getNext15mMarket(coin) {
    return this._getNextMarket(coin, 15);
  }

  /**
   * Get the next upcoming 5-minute market for a coin.
   *
   * @param {string} coin - Coin symbol
   * @returns {Promise<Record<string, any> | null>}
   */
  async getNext5mMarket(coin) {
    return this._getNextMarket(coin, 5);
  }

  /**
   * Get the next upcoming market for a coin and interval.
   *
   * @param {string} coin - Coin symbol
   * @param {5|15} interval - Window size in minutes
   * @returns {Promise<Record<string, any> | null>}
   */
  async _getNextMarket(coin, interval) {
    const upper = coin.toUpperCase();
    const slugs = COIN_SLUGS[interval];
    if (!slugs || !(upper in slugs)) {
      throw new Error(`Unsupported coin: ${coin}`);
    }
    const prefix = slugs[upper];
    const now = new Date();

    let nextMinute = (Math.floor(now.getUTCMinutes() / interval) + 1) * interval;
    let h = now.getUTCHours();
    if (nextMinute >= 60) {
      nextMinute = 0;
      h += 1;
    }
    const nextWindow = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      h,
      nextMinute,
      0,
      0
    ));
    const nextTs = Math.floor(nextWindow.getTime() / 1000);
    const slug = `${prefix}-${nextTs}`;
    return this.getMarketBySlug(slug);
  }

  /**
   * Parse token IDs from market data.
   *
   * @param {Record<string, any>} market - Market data
   * @returns {Record<string, string>} "up" and "down" token IDs
   */
  parseTokenIds(market) {
    const clobTokenIds = market?.clobTokenIds ?? '[]';
    const tokenIds = this._parseJsonField(clobTokenIds);
    const outcomes = market?.outcomes ?? '["Up", "Down"]';
    const outcomeList = this._parseJsonField(outcomes);
    return this._mapOutcomes(outcomeList, tokenIds, (v) => String(v));
  }

  /**
   * Parse current prices from market data.
   *
   * @param {Record<string, any>} market - Market data
   * @returns {Record<string, number>} "up" and "down" prices
   */
  parsePrices(market) {
    const outcomePrices = market?.outcomePrices ?? '["0.5", "0.5"]';
    const prices = this._parseJsonField(outcomePrices);
    const outcomes = market?.outcomes ?? '["Up", "Down"]';
    const outcomeList = this._parseJsonField(outcomes);
    return this._mapOutcomes(outcomeList, prices, (v) => Number(v));
  }

  /**
   * @param {any} value - JSON string or array
   * @returns {any[]}
   */
  _parseJsonField(value) {
    if (typeof value === 'string') return JSON.parse(value);
    return value ?? [];
  }

  /**
   * @param {any[]} outcomes
   * @param {any[]} values
   * @param {(v: any) => any} cast
   * @returns {Record<string, any>}
   */
  _mapOutcomes(outcomes, values, cast = (v) => v) {
    const result = {};
    for (let i = 0; i < outcomes.length; i++) {
      if (i < values.length) {
        result[String(outcomes[i]).toLowerCase()] = cast(values[i]);
      }
    }
    return result;
  }

  /**
   * Get comprehensive market info for current market.
   *
   * @param {string} coin - Coin symbol
   * @param {'5m'|'15m'} [interval='15m'] - Window size
   * @returns {Promise<Record<string, any> | null>}
   */
  async getMarketInfo(coin, interval = '15m') {
    const mins = interval === '5m' ? 5 : 15;
    const market = await this._getCurrentMarket(coin.toUpperCase(), mins);
    if (!market) return null;

    const tokenIds = this.parseTokenIds(market);
    const prices = this.parsePrices(market);

    return {
      slug: market.slug,
      question: market.question,
      end_date: market.endDate,
      token_ids: tokenIds,
      prices,
      accepting_orders: market.acceptingOrders ?? false,
      best_bid: market.bestBid,
      best_ask: market.bestAsk,
      spread: market.spread,
      raw: market,
    };
  }
}

module.exports = { GammaClient, DEFAULT_HOST, COIN_SLUGS };
