require('dotenv').config();
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway.internal') ? false : { rejectUnauthorized: false }
});

const RESULTS_PATH = path.join(__dirname, 'data', 'results.xlsx');

const log = (msg) => {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${msg}`);
  try {
    const logPath = path.join(__dirname, 'logs', 'scheduler.log');
    fs.appendFileSync(logPath, `[${timestamp}] ${msg}\n`);
  } catch(e) {}
};

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS creators (
      id SERIAL PRIMARY KEY,
      first_name TEXT,
      handle TEXT UNIQUE,
      email TEXT,
      avg_views NUMERIC,
      avg_likes NUMERIC,
      avg_comments NUMERIC,
      like_ratio NUMERIC,
      comment_ratio NUMERIC,
      subscriber_count NUMERIC,
      niche TEXT,
      channel_url TEXT,
      date_found TEXT,
      batch_number INTEGER
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS seen_channels (
      channel_id TEXT PRIMARY KEY
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);
  log('Database initialized');
}

async function getState() {
  try {
    const res = await pool.query('SELECT key, value FROM app_state');
    const state = { batchNumber: 0, totalFound: 0, lastRunAt: null, isRunning: false };
    res.rows.forEach(r => {
      if (r.key === 'batchNumber') state.batchNumber = parseInt(r.value) || 0;
      if (r.key === 'totalFound') state.totalFound = parseInt(r.value) || 0;
      if (r.key === 'lastRunAt') state.lastRunAt = r.value;
      if (r.key === 'isRunning') state.isRunning = r.value === 'true';
    });
    return state;
  } catch(e) {
    return { batchNumber: 0, totalFound: 0, lastRunAt: null, isRunning: false };
  }
}

async function saveState(state) {
  for (const [key, value] of Object.entries(state)) {
    await pool.query(
      'INSERT INTO app_state (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2',
      [key, String(value)]
    );
  }
}

async function getSeenChannels() {
  try {
    const res = await pool.query('SELECT channel_id FROM seen_channels');
    const seen = {};
    res.rows.forEach(r => { seen[r.channel_id] = true; });
    return seen;
  } catch(e) { return {}; }
}

async function addSeenChannel(channelId) {
  try {
    await pool.query('INSERT INTO seen_channels (channel_id) VALUES ($1) ON CONFLICT DO NOTHING', [channelId]);
  } catch(e) {}
}

async function getLastResults(limit = 10000) {
  try {
    const res = await pool.query('SELECT * FROM creators ORDER BY batch_number ASC, id ASC LIMIT $1', [limit]);
    return res.rows;
  } catch(e) { return []; }
}

async function saveCreators(rows) {
  for (const r of rows) {
    try {
      await pool.query(`
        INSERT INTO creators (first_name, handle, email, avg_views, avg_likes, avg_comments, like_ratio, comment_ratio, subscriber_count, niche, channel_url, date_found, batch_number)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT (handle) DO NOTHING
      `, [r.first_name, r.handle, r.email, r.avg_views, r.avg_likes, r.avg_comments, r.like_ratio, r.comment_ratio, r.subscriber_count, r.niche, r.channel_url, r.date_found, r.batch_number]);
    } catch(e) { log('Error saving creator: ' + e.message); }
  }
}

function generateExcel(rows) {
  const COLUMNS = ['first_name','handle','email','avg_views','avg_likes','avg_comments','like_ratio','comment_ratio','subscriber_count','niche','channel_url','date_found','batch_number'];
  const HEADERS = ['First Name','Handle','Email','Avg Views','Avg Likes','Avg Comments','Like Ratio','Comment Ratio','Subscribers','Niche','Channel URL','Date Found','Batch #'];
  const wb = XLSX.utils.book_new();
  const wsData = [HEADERS, ...rows.map(r => COLUMNS.map(c => r[c] || ''))];
  const ws = XLSX.utils.aoa_to_sheet(wsData);
  ws['!cols'] = COLUMNS.map(col => {
    if (col === 'channel_url') return { wch: 40 };
    if (col === 'email') return { wch: 30 };
    if (col === 'niche') return { wch: 20 };
    return { wch: 15 };
  });
  XLSX.utils.book_append_sheet(wb, ws, 'YouTubers');
  return wb;
}

const SEARCH_QUERIES = [
  'mental health tips','anxiety relief','depression support','self care routine',
  'mindfulness meditation','therapy talk','emotional wellness','mental health vlog',
  'healing journey','mental health awareness','cozy lifestyle vlog','kawaii collection',
  'plush toy collection','stuffed animal collection','comfort items','anxiety comfort',
  'grwm get ready with me','grwm vlog','tarot reading','tarot card reading 2026',
  'daily tarot','makeup tutorial','makeup grwm','soft girl makeup','drugstore makeup',
  'nail art tutorial','hair tutorial','fashion haul','outfit of the day',
  'aesthetic vlog','day in my life aesthetic','study with me','journaling vlog',
  'manifestation routine','astrology reading','spiritual vlog'
];

async function fetchVideoStats(apiKey, videoIds) {
  const url = `https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${videoIds.join(',')}&key=${apiKey}`;
  const res = await fetch(url);
  const data = await res.json();
  return data.items || [];
}

async function fetchChannelDetails(apiKey, channelId) {
  const url = `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,brandingSettings&id=${channelId}&key=${apiKey}`;
  const res = await fetch(url);
  const data = await res.json();
  return data.items?.[0] || null;
}

async function searchVideos(apiKey, query) {
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(query)}&type=video&maxResults=10&order=relevance&key=${apiKey}`;
  const res = await fetch(url);
  const data = await res.json();
  return data.items || [];
}

async function getChannelVideos(apiKey, channelId) {
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channelId}&type=video&maxResults=10&order=date&key=${apiKey}`;
  const res = await fetch(url);
  const data = await res.json();
  return data.items || [];
}

function extractEmail(text) {
  if (!text) return null;
  const match = text.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/);
  return match ? match[0] : null;
}

function getNiche(title, description, tags) {
  const text = ((title || '') + ' ' + (description || '') + ' ' + (tags || '')).toLowerCase();
  if (text.match(/mental health|anxiety|depression|therapy|mindful|wellness|healing/)) return 'mental health';
  if (text.match(/tarot|astrology|spiritual|manifestation|zodiac/)) return 'spiritual';
  if (text.match(/makeup|beauty|skincare|nail|hair|grwm|get ready/)) return 'beauty';
  if (text.match(/fashion|outfit|ootd|style|haul/)) return 'fashion';
  if (text.match(/study|journal|aesthetic|vlog|day in my life/)) return 'aesthetic vlog';
  if (text.match(/cozy|kawaii|plush|stuffed|comfort/)) return 'cozy lifestyle';
  if (text.match(/meditation|self.?care|mindfulness/)) return 'self care';
  return 'lifestyle';
}

async function runBatch(apiKey) {
  const seen = await getSeenChannels();
  const results = [];
  const shuffled = SEARCH_QUERIES.sort(() => Math.random() - 0.5);

  for (const query of shuffled) {
    if (results.length >= 50) break;
    try {
      const videos = await searchVideos(apiKey, query);
      for (const video of videos) {
        if (results.length >= 50) break;
        const channelId = video.snippet?.channelId;
        if (!channelId || seen[channelId]) continue;

        const channel = await fetchChannelDetails(apiKey, channelId);
        if (!channel) continue;

        const subs = parseInt(channel.statistics?.subscriberCount || 0);
        if (subs < 1000 || subs > 500000) { await addSeenChannel(channelId); continue; }

        const channelVideos = await getChannelVideos(apiKey, channelId);
        if (channelVideos.length < 3) { await addSeenChannel(channelId); continue; }

        const videoIds = channelVideos.map(v => v.id?.videoId).filter(Boolean);
        if (videoIds.length === 0) { await addSeenChannel(channelId); continue; }

        const stats = await fetchVideoStats(apiKey, videoIds);
        if (stats.length === 0) { await addSeenChannel(channelId); continue; }

        const avgViews = stats.reduce((s, v) => s + parseInt(v.statistics?.viewCount || 0), 0) / stats.length;
        const avgLikes = stats.reduce((s, v) => s + parseInt(v.statistics?.likeCount || 0), 0) / stats.length;
        const avgComments = stats.reduce((s, v) => s + parseInt(v.statistics?.commentCount || 0), 0) / stats.length;

        if (avgViews < 1000) { await addSeenChannel(channelId); continue; }

        const likeRatio = avgViews > 0 ? avgLikes / avgViews : 0;
        const commentRatio = avgViews > 0 ? avgComments / avgViews : 0;

        if (likeRatio < 0.05) { await addSeenChannel(channelId); continue; }
        if (commentRatio < 0.005) { await addSeenChannel(channelId); continue; }

        const desc = channel.snippet?.description || '';
        const handle = channel.snippet?.customUrl || ('@' + channel.snippet?.title?.replace(/\s+/g,'').toLowerCase());
        const email = extractEmail(desc) || extractEmail(channel.brandingSettings?.channel?.description);
        const niche = getNiche(channel.snippet?.title, desc, '');
        const firstName = channel.snippet?.title?.split(' ')[0] || 'Unknown';

        results.push({
          first_name: firstName,
          handle,
          email: email || 'Not listed',
          avg_views: Math.round(avgViews),
          avg_likes: Math.round(avgLikes),
          avg_comments: Math.round(avgComments),
          like_ratio: parseFloat(likeRatio.toFixed(4)),
          comment_ratio: parseFloat(commentRatio.toFixed(4)),
          subscriber_count: subs,
          niche,
          channel_url: `https://youtube.com/${handle}`,
          date_found: new Date().toISOString().split('T')[0],
        });

        await addSeenChannel(channelId);
        log(`Found: ${firstName} (${handle}) - ${Math.round(avgViews)} avg views`);
      }
    } catch(e) {
      log(`Query "${query}" error: ${e.message}`);
    }
  }
  return results;
}

async function executeBatch(apiKey) {
  const state = await getState();
  if (state.isRunning) return { success: false, message: 'Already running' };

  state.isRunning = true;
  await saveState(state);

  try {
    const rows = await runBatch(apiKey);
    const batchNum = (state.batchNumber || 0) + 1;
    rows.forEach(r => r.batch_number = batchNum);

    if (rows.length > 0) await saveCreators(rows);

    const newState = {
      batchNumber: batchNum,
      totalFound: (state.totalFound || 0) + rows.length,
      lastRunAt: new Date().toISOString(),
      isRunning: false
    };
    await saveState(newState);

    // Also save Excel to disk as backup
    try {
      const all = await getLastResults();
      const wb = generateExcel(all);
      const dir = path.join(__dirname, 'data');
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      XLSX.writeFile(wb, RESULTS_PATH);
    } catch(e) {}

    log(`Batch ${batchNum} complete: ${rows.length} new channels`);
    return { success: true, found: rows.length, batchNumber: batchNum };
  } catch(err) {
    await saveState({ ...await getState(), isRunning: false });
    log(`Batch failed: ${err.message}`);
    return { success: false, message: err.message };
  }
}

function startScheduler() {
  initDb().then(() => {
    log('Scheduler ready. Runs daily at 2:00 AM UTC');
    setInterval(async () => {
      const now = new Date();
      if (now.getUTCHours() === 2 && now.getUTCMinutes() === 0) {
        const apiKey = process.env.YOUTUBE_API_KEY;
        if (apiKey) {
          log('Starting scheduled batch...');
          await executeBatch(apiKey);
        }
      }
    }, 60000);
  }).catch(e => log('DB init error: ' + e.message));
}

module.exports = { startScheduler, executeBatch, getState, getLastResults, generateExcel, initDb, RESULTS_PATH };
