require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');

let API_KEY = '';
try {
  const envContent = fs.readFileSync(path.join(__dirname, '.env'), 'utf8');
  const match = envContent.match(/YOUTUBE_API_KEY=(.+)/);
  if (match) API_KEY = match[1].trim();
} catch(e) {}
if (!API_KEY) API_KEY = process.env.YOUTUBE_API_KEY || '';

const { startScheduler, executeBatch, getState, getLastResults, generateExcel, initDb, RESULTS_PATH, getApiKeys } = require('./scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

['data','logs'].forEach(dir => {
  const p = path.join(__dirname, dir);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
});

app.get('/api/status', async (req, res) => {
  const state = await getState();
  const keys = getApiKeys();
  res.json({
    batchNumber: state.batchNumber,
    totalFound: state.totalFound,
    lastRunAt: state.lastRunAt,
    isRunning: state.isRunning,
    hasApiKey: keys.length > 0,
    apiKeyCount: keys.length,
    keyPreview: keys.length > 0 ? keys[0].slice(0,8) : 'none'
  });
});

app.get('/api/download', async (req, res) => {
  try {
    const rows = await getLastResults();
    if (rows.length === 0) return res.status(404).json({ error: 'No results yet.' });
    const XLSX = require('xlsx');
    const wb = generateExcel(rows);
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="chubiez-youtubers.xlsx"');
    res.send(buf);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/download/csv', async (req, res) => {
  try {
    const batch = req.query.batch;
    const all = await getLastResults();
    const data = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    if (data.length === 0) return res.status(404).json({ error: 'No results found' });
    const headers = ['first_name','handle','email','avg_views','avg_likes','avg_comments','like_ratio','comment_ratio','subscriber_count','niche','channel_url','date_found','batch_number'];
    const csv = [headers.join(','), ...data.map(r => headers.map(h => `"${String(r[h]||'').replace(/"/g,'""')}"`).join(','))].join('\n');
    const filename = batch ? `chubiez-batch-${batch}.csv` : 'chubiez-all-creators.csv';
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/trigger', async (req, res) => {
  const keys = getApiKeys();
  if (keys.length === 0) return res.status(400).json({ error: 'No YouTube API keys configured' });
  const state = await getState();
  if (state.isRunning) return res.status(409).json({ error: 'A batch is already running' });
  res.json({ message: 'Batch started', apiKeys: keys.length });
  executeBatch(keys).catch(err => console.log(`Manual batch error: ${err.message}`));
});

app.get('/api/results/all', async (req, res) => {
  const results = await getLastResults(10000);
  res.json(results);
});

app.get('/api/results', async (req, res) => {
  const results = await getLastResults(parseInt(req.query.limit) || 50);
  res.json(results);
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

initDb().then(() => {
  app.listen(PORT, () => {
    const keys = getApiKeys();
    console.log(`\n Chubiez YouTuber Finder running at http://localhost:${PORT}`);
    console.log(` ${keys.length} API key(s) loaded\n`);
    if (keys.length === 0) console.log(' WARNING: No YouTube API keys found!');
    startScheduler();
  });
}).catch(e => {
  console.error('Failed to init DB:', e.message, e.stack);
  process.exit(1);
});
