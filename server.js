/**
 * HTTP server for Polymarket rounds dashboard.
 * Serves static files from public/ and provides API for rounds data.
 * Market subscription & WebSocket disabled - dashboard-only mode.
 */

const http = require('http');
const path = require('path');
const fs = require('fs');

const db = require('./db');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const PAGE_SIZE = 20;

const server = http.createServer(async (req, res) => {
  const url = req.url === '/' ? '/dashboard.html' : req.url;
  const filePath = path.join(PUBLIC_DIR, url.split('?')[0]);

  if (url.startsWith('/api/')) {
    const route = url.slice(5).split('?')[0];
    const params = new URL(req.url, 'http://x').searchParams;

    if (route === 'rounds' && req.method === 'GET') {
      try {
        const coin = params.get('coin') || undefined;
        const page = Math.max(1, parseInt(params.get('page'), 10) || 1);
        const limit = Math.min(100, Math.max(1, parseInt(params.get('limit'), 10) || PAGE_SIZE));
        const offset = (page - 1) * limit;

        const opts = { limit, offset };
        if (coin) opts.coin = coin;

        const rows = db.queryRounds(opts);
        const total = db.countRounds(coin ? { coin } : {});
        res.setHeader('Content-Type', 'application/json');
        res.end(
          JSON.stringify({
            rounds: rows,
            total,
            page,
            pageSize: limit,
            totalPages: Math.ceil(total / limit),
          })
        );
      } catch (e) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }

    if (route === 'coins' && req.method === 'GET') {
      try {
        const coins = db.getCoins();
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ coins }));
      } catch (e) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }
  }

  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.statusCode = 403;
    res.end('Forbidden');
    return;
  }

  try {
    const stat = await fs.promises.stat(filePath);
    if (!stat.isFile()) {
      res.statusCode = 404;
      res.end('Not found');
      return;
    }
    const ext = path.extname(filePath);
    const types = {
      '.html': 'text/html',
      '.js': 'application/javascript',
      '.css': 'text/css',
      '.ico': 'image/x-icon',
      '.json': 'application/json',
      '.svg': 'image/svg+xml',
    };
    res.setHeader('Content-Type', types[ext] || 'application/octet-stream');
    fs.createReadStream(filePath).pipe(res);
  } catch (e) {
    if (e.code === 'ENOENT') {
      res.statusCode = 404;
      res.end('Not found');
    } else {
      res.statusCode = 500;
      res.end('Internal error');
    }
  }
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
