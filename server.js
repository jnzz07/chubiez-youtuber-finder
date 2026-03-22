'use strict';
require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const {
  startScheduler, executeBatch, getState, getLastResults,
  generateExcel, initDb, getApiKeys, getLogs, pushToInstantly,
  getManualSentBatches, toggleManualSent, markInstantlySent, generatePersonalization, enrichNewCreators,
} = require('./scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { etag: false, lastModified: false, setHeaders: (res) => res.setHeader('Cache-Control', 'no-store') }));

// Ensure runtime dirs exist
['data', 'logs'].forEach(dir => {
  const p = path.join(__dirname, dir);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
});

// ─── STATUS ───────────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
  const state = getState();
  const keys = getApiKeys();
  res.json({
    ...state,
    hasApiKey: keys.length > 0,
    apiKeyCount: keys.length,
  });
});

// ─── LIVE PROGRESS ────────────────────────────────────────────────────────────
app.get('/api/progress', (req, res) => {
  const state = getState();
  res.json(state.progress || { phase: 'Idle', done: 0, total: 0, currentName: '', foundSoFar: 0 });
});

// ─── LOGS ─────────────────────────────────────────────────────────────────────
app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 80;
  const logs = getLogs();
  res.json({ logs: logs.slice(-limit) });
});

// ─── DEBUG ───────────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
  const state = getState();
  const keys = getApiKeys();
  const logs = getLogs();
  res.json({
    state,
    keyCount: keys.length,
    keyNames: Array.from({ length: 10 }, (_, i) => `YOUTUBE_API_KEY_${i + 1}`)
      .filter(name => !!process.env[name])
      .concat(process.env.YOUTUBE_API_KEY ? ['YOUTUBE_API_KEY'] : []),
    dbUrl: process.env.DATABASE_URL ? process.env.DATABASE_URL.replace(/:([^:@]+)@/, ':***@') : 'NOT SET',
    recentLogs: logs.slice(-30),
  });
});

// ─── TRIGGER ─────────────────────────────────────────────────────────────────
app.post('/api/trigger', async (req, res) => {
  const state = getState();
  if (state.isRunning) return res.status(409).json({ error: 'Batch already running' });
  const keys = getApiKeys();
  if (keys.length === 0) return res.status(400).json({ error: 'No YouTube API keys configured' });
  res.json({ success: true, message: `Batch started with ${keys.length} key(s)` });
  executeBatch(keys).catch(e => console.error('Batch error:', e.message));
});

// ─── RESULTS ─────────────────────────────────────────────────────────────────
app.get('/api/results', async (req, res) => {
  try {
    const results = await getLastResults(parseInt(req.query.limit) || 50);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/results/all', async (req, res) => {
  try {
    const results = await getLastResults(10000);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── DOWNLOADS ───────────────────────────────────────────────────────────────
app.get('/api/download', async (req, res) => {
  try {
    const rows = await getLastResults();
    if (rows.length === 0) return res.status(404).json({ error: 'No results yet' });
    const XLSX = require('xlsx');
    const wb = generateExcel(rows);
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="chubiez-creators-${Date.now()}.xlsx"`);
    res.send(buf);
    const emails = rows.map(r => r.email).filter(Boolean);
    markInstantlySent(emails).catch(() => {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/download/csv', async (req, res) => {
  try {
    const batch = req.query.batch;
    const all = await getLastResults();
    const data = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    if (data.length === 0) return res.status(404).json({ error: 'No results found' });

    const cols = [
      'first_name', 'handle', 'email', 'niche', 'subscriber_count',
      'avg_views', 'avg_likes', 'avg_comments', 'like_ratio', 'comment_ratio',
      'country', 'upload_frequency', 'total_views', 'video_count',
      'channel_url', 'thumbnail_url', 'date_found', 'batch_number',
      'vibe', 'praise', 'looking_forward',
    ];
    const headers = [
      'Name', 'Handle', 'Email', 'Niche', 'Subscribers',
      'Avg Views', 'Avg Likes', 'Avg Comments', 'Like Ratio', 'Comment Ratio',
      'Country', 'Uploads/Mo', 'Total Views', 'Video Count',
      'Channel URL', 'Thumbnail URL', 'Date Found', 'Batch',
      'VIBE', 'PRAISE', 'LOOKING FORWARD',
    ];

    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const csv = [
      headers.map(esc).join(','),
      ...data.map(r => cols.map(c => {
        const v = r[c];
        if (c === 'like_ratio' || c === 'comment_ratio') return esc(v != null ? (v * 100).toFixed(2) + '%' : '');
        return esc(v ?? '');
      }).join(',')),
    ].join('\n');

    const filename = batch ? `chubiez-batch-${batch}.csv` : 'chubiez-all-creators.csv';
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
    const emails = data.map(r => r.email).filter(Boolean);
    markInstantlySent(emails).catch(() => {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/enrich', (req, res) => {
  enrichNewCreators().catch(e => console.error('enrichNewCreators failed:', e.message));
  res.json({ ok: true, message: 'Enrichment running in background — takes a few minutes for large sets' });
});

app.get('/api/enrich/debug', async (req, res) => {
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    const rows = await getLastResults();
    const withoutVibe = rows.filter(r => !r.vibe).length;
    res.json({
      apiKeySet: !!(apiKey && apiKey.trim()),
      apiKeyLength: apiKey ? apiKey.trim().length : 0,
      totalCreators: rows.length,
      creatorsWithoutVibe: withoutVibe,
      sample: rows.slice(0, 2).map(r => ({ handle: r.handle, vibe: r.vibe, praise: r.praise })),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── INSTANTLY AI ─────────────────────────────────────────────────────────────
app.post('/api/instantly/push', async (req, res) => {
  const apiKey = process.env.INSTANTLY_API_KEY;
  if (!apiKey) return res.status(400).json({ error: 'INSTANTLY_API_KEY must be set in .env' });
  try {
    const { batch } = req.body;
    const all = await getLastResults(10000);
    const creators = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    if (creators.length === 0) return res.status(404).json({ error: 'No creators found' });
    const batchLabel = batch ? `Batch ${batch}` : 'All Creators';
    const result = await pushToInstantly(creators, apiKey, batchLabel);
    res.json({ success: true, ...result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── MANUAL SEND TRACKING ────────────────────────────────────────────────────
app.get('/api/batches/manual-sent', (req, res) => {
  res.json({ batches: getManualSentBatches() });
});

app.post('/api/batches/:batch/toggle-manual-sent', async (req, res) => {
  const batch = parseInt(req.params.batch);
  if (isNaN(batch)) return res.status(400).json({ error: 'Invalid batch number' });
  try {
    const isSent = await toggleManualSent(batch);
    res.json({ batchNumber: batch, manuallySent: isSent });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── SPA FALLBACK ─────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── START ────────────────────────────────────────────────────────────────────
initDb()
  .then(() => {
    app.listen(PORT, () => {
      const keys = getApiKeys();
      console.log(`\n Chubiez YouTuber Finder running at http://localhost:${PORT}`);
      console.log(` ${keys.length} API key(s) loaded`);
      if (keys.length === 0) console.log(' WARNING: No YouTube API keys found!');
      startScheduler();
    });
  })
  .catch(e => {
    console.error('DB init error:', e.message);
    // Start anyway — server works without DB (in-memory mode)
    app.listen(PORT, () => {
      console.log(`\n Server running on port ${PORT} (no DB — in-memory mode)`);
      startScheduler();
    });
  });
