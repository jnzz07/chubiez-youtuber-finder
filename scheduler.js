require('dotenv').config({ path: require('path').resolve(__dirname, '.env') });
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const { runBatch, log } = require('./scraper');

const RESULTS_PATH = path.join(__dirname, 'data', 'results.xlsx');
const STATE_PATH = path.join(__dirname, 'data', 'state.json');

const COLUMNS = [
  'first_name', 'handle', 'email', 'avg_views', 'avg_likes',
  'avg_comments', 'like_ratio', 'comment_ratio', 'subscriber_count',
  'niche', 'channel_url', 'date_found', 'batch_number',
];

const COLUMN_HEADERS = {
  first_name: 'First Name',
  handle: 'Handle',
  email: 'Email',
  avg_views: 'Avg Views',
  avg_likes: 'Avg Likes',
  avg_comments: 'Avg Comments',
  like_ratio: 'Like Ratio',
  comment_ratio: 'Comment Ratio',
  subscriber_count: 'Subscribers',
  niche: 'Niche',
  channel_url: 'Channel URL',
  date_found: 'Date Found',
  batch_number: 'Batch #',
};

let state = {
  batchNumber: 0,
  totalFound: 0,
  lastRunAt: null,
  nextRunAt: null,
  isRunning: false,
};

function loadState() {
  try {
    if (fs.existsSync(STATE_PATH)) {
      state = { ...state, ...JSON.parse(fs.readFileSync(STATE_PATH, 'utf8')) };
    }
  } catch (e) {
    log(`Error loading state: ${e.message}`);
  }
  return state;
}

function saveState() {
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

function getState() {
  return { ...state };
}

function loadExistingResults() {
  try {
    if (fs.existsSync(RESULTS_PATH)) {
  const wb = XLSX.readFile(RESULTS_PATH);
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });
  if (rows.length <= 1) return [];
  const headers = rows[0];
  return rows.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row[i]; });
    return obj;
  });
}
```

Save, then push to GitHub:
```

  } catch (e) {
    log(`Error loading existing results: ${e.message}`);
  }
  return [];
}

function appendToSpreadsheet(newRows) {
  let existing = loadExistingResults();
  const all = [...existing, ...newRows];

  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet(all, { header: COLUMNS });

  // Set header names
  COLUMNS.forEach((col, i) => {
    const cellRef = XLSX.utils.encode_cell({ r: 0, c: i });
    if (ws[cellRef]) {
      ws[cellRef].v = COLUMN_HEADERS[col];
    }
  });

  // Set column widths
  ws['!cols'] = COLUMNS.map(col => {
    if (col === 'channel_url') return { wch: 40 };
    if (col === 'email') return { wch: 30 };
    if (col === 'niche') return { wch: 20 };
    return { wch: 15 };
  });

  XLSX.utils.book_append_sheet(wb, ws, 'YouTubers');
  XLSX.writeFile(wb, RESULTS_PATH);
  log(`Spreadsheet updated: ${all.length} total rows`);
}

function getLastResults(limit = 50) {
  const all = loadExistingResults();
  return all.slice(-limit);
}

async function executeBatch(apiKey) {
  if (state.isRunning) {
    log('Batch already running, skipping');
    return { success: false, message: 'Already running' };
  }

  state.isRunning = true;
  state.lastRunAt = new Date().toISOString();
  saveState();

  try {
    const rows = await runBatch(apiKey);
    const batchNum = state.batchNumber + 1;

    // Assign batch number to all rows
    rows.forEach(row => { row.batch_number = batchNum; });

    if (rows.length > 0) {
      appendToSpreadsheet(rows);
    }

    state.batchNumber = batchNum;
    state.totalFound = (state.totalFound || 0) + rows.length;
    state.isRunning = false;
    saveState();

    log(`Batch ${batchNum} complete: ${rows.length} new channels`);
    return { success: true, found: rows.length, batchNumber: batchNum };
  } catch (err) {
    state.isRunning = false;
    saveState();
    log(`Batch failed: ${err.message}`);
    return { success: false, message: err.message };
  }
}

function computeNextRun() {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(2, 0, 0, 0);
  if (next <= now) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next.toISOString();
}

function startScheduler(apiKey) {
  loadState();
  state.nextRunAt = computeNextRun();
  saveState();

  // Run every day at 02:00 UTC
  cron.schedule('0 2 * * *', async () => {
    log('Scheduled batch triggered');
    await executeBatch(apiKey);
    state.nextRunAt = computeNextRun();
    saveState();
  }, { timezone: 'UTC' });

  log('Scheduler started: runs daily at 02:00 UTC');
}

module.exports = {
  startScheduler,
  executeBatch,
  getState,
  getLastResults,
  loadState,
  RESULTS_PATH,
};
