const path = require('path');
const fs = require('fs');

// Read key directly from .env file - no dotenv needed
const envPath = path.join(__dirname, '.env');
let API_KEY = '';
try {
  const envContent = fs.readFileSync(envPath, 'utf8');
  const match = envContent.match(/YOUTUBE_API_KEY=(.+)/);
  if (match) API_KEY = match[1].trim();
} catch(e) {}

// Also try process.env as fallback
if (!API_KEY) API_KEY = process.env.YOUTUBE_API_KEY || '';

const express = require('express');
const { startScheduler, executeBatch, getState, getLastResults, RESULTS_PATH } = require('./scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

['data', 'logs'].forEach(dir => {
  const p = path.join(__dirname, dir);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
});

app.get('/api/status', (req, res) => {
  const state = getState();
  res.json({
    batchNumber: state.batchNumber,
    totalFound: state.totalFound,
    lastRunAt: state.lastRunAt,
    nextRunAt: state.nextRunAt,
    isRunning: state.isRunning,
    hasApiKey: !!API_KEY,
    keyPreview: API_KEY ? API_KEY.slice(0,8) : 'none'
  });
});

app.get('/api/download', (req, res) => {
  if (!fs.existsSync(RESULTS_PATH)) {
    return res.status(404).json({ error: 'No results file yet. Run a batch first.' });
  }
  res.download(RESULTS_PATH, 'chubiez-youtubers.xlsx');
});

app.post('/api/trigger', async (req, res) => {
  console.log('Trigger called, API_KEY exists:', !!API_KEY, 'starts with:', API_KEY.slice(0,8));
  if (!API_KEY) {
    return res.status(400).json({ error: 'YouTube API key not configured' });
  }
  const state = getState();
  if (state.isRunning) {
    return res.status(409).json({ error: 'A batch is already running' });
  }
  res.json({ message: 'Batch started' });
  executeBatch(API_KEY).catch(err => console.log(`Manual batch error: ${err.message}`));
});

app.get('/api/results', (req, res) => {
  const results = getLastResults(parseInt(req.query.limit) || 50);
  res.json(results);
});

app.get('/api/results/all', (req, res) => {
  res.json(getLastResults(10000));
});

app.get('/api/download/csv', (req, res) => {
  const batch = req.query.batch;
  const all = getLastResults(10000);
  const data = batch ? all.filter(r => String(r.batch_number || r['Batch #']) === String(batch)) : all;
  if (data.length === 0) return res.status(404).json({ error: 'No results found' });
  const headers = ['first_name','handle','email','avg_views','avg_likes','avg_comments','like_ratio','comment_ratio','subscriber_count','niche','channel_url','date_found','batch_number'];
  const csv = [headers.join(','), ...data.map(r => headers.map(h => {
    const val = r[h] || r[h.split('_').map((w,i) => i===0?w:w[0].toUpperCase()+w.slice(1)).join('')] || '';
    return `"${String(val).replace(/"/g,'""')}"`;
  }).join(','))].join('\n');
  const filename = batch ? `chubiez-batch-${batch}.csv` : 'chubiez-all-creators.csv';
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(csv);
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`\n  Chubiez YouTuber Finder running at http://localhost:${PORT}\n`);
  if (!API_KEY) {
    console.log('  WARNING: YOUTUBE_API_KEY not found\n');
  } else {
    console.log(`  API key loaded: ${API_KEY.slice(0,8)}...\n`);
    startScheduler(API_KEY);
  }
});
