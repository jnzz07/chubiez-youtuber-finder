require('dotenv').config();
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
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

// ─── API KEY ROTATION ───────────────────────────────────────────────────────
function getApiKeys() {
  const keys = [];
  for (let i = 1; i <= 10; i++) {
    const key = process.env[`YOUTUBE_API_KEY_${i}`];
    if (key) keys.push(key);
  }
  // also support legacy single key
  if (process.env.YOUTUBE_API_KEY && !keys.includes(process.env.YOUTUBE_API_KEY)) {
    keys.push(process.env.YOUTUBE_API_KEY);
  }
  return keys;
}

let currentKeyIndex = 0;
let exhaustedKeys = new Set();

function getNextKey(keys) {
  const available = keys.filter((_, i) => !exhaustedKeys.has(i));
  if (available.length === 0) return null;
  const idx = keys.indexOf(available[0]);
  currentKeyIndex = idx;
  return keys[idx];
}

function markKeyExhausted(keys, key) {
  const idx = keys.indexOf(key);
  if (idx !== -1) {
    exhaustedKeys.add(idx);
    log(`API key ${idx + 1} quota exhausted, switching to next key...`);
  }
}

// ─── SEARCH QUERIES (100+) ────────────────────────────────────────────────
const SEARCH_QUERIES = [
  // Mental health core
  'mental health tips','anxiety relief','depression support','therapy talk',
  'mental health vlog','healing journey','mental health awareness','anxiety vlog',
  'depression recovery','mental health day in my life','therapy session vlog',
  'coping with anxiety','panic attack help','social anxiety tips',
  'mental health check in','living with depression','anxiety and depression',
  'mental health for teens','young adult mental health','college mental health',

  // Self care & wellness
  'self care routine','self care sunday','self care day vlog',
  'wellness routine','morning wellness routine','night routine self care',
  'glow up routine','that girl morning routine','5am morning routine',
  'healthy habits routine','slow morning routine','reset routine vlog',
  'mindfulness for beginners','mindfulness meditation','guided meditation',
  'breathing exercises anxiety','journaling for mental health','gratitude journal',
  'emotional healing vlog','inner child healing','shadow work journal',

  // ASMR
  'asmr relaxing','asmr sleep','asmr anxiety relief','asmr soft spoken',
  'asmr tapping','asmr plushie','asmr stuffed animals','asmr cozy',
  'asmr night routine','asmr self care','asmr gentle whispering',

  // Cozy lifestyle
  'cozy vlog','cozy day in my life','cozy lifestyle','cozy night routine',
  'cottagecore vlog','cottagecore lifestyle','slow living vlog',
  'hygge lifestyle','cozy autumn vlog','cozy winter vlog',
  'soft life vlog','soft life aesthetic','cozy apartment vlog',
  'cozy gaming vlog','cozy study vlog','cozy reading vlog',

  // Kawaii & plush
  'kawaii collection','kawaii haul','plush collection','stuffed animal collection',
  'squishmallow collection','sanrio collection','cute plushie haul',
  'kawaii room tour','kawaii lifestyle','kawaii aesthetic vlog',
  'plushie unboxing','jellycat collection','build a bear collection',

  // Neurodivergent
  'adhd vlog','adhd day in my life','adhd tips','living with adhd',
  'autism vlog','autism day in my life','autistic creator','adhd and anxiety',
  'neurodivergent vlog','adhd productivity','adhd self care',

  // Chronic illness & invisible illness
  'chronic illness vlog','chronic pain vlog','invisible illness','fibromyalgia vlog',
  'chronic fatigue vlog','spoonie life','spoonie vlog','chronic illness day in my life',
  'living with chronic illness','endometriosis vlog','ibs vlog',

  // Loneliness & introvert
  'introvert vlog','introvert day in my life','being an introvert',
  'loneliness vlog','alone time vlog','solo living vlog',
  'living alone vlog','single life vlog','independent woman vlog',

  // Grief & emotional
  'grief vlog','loss and healing','heartbreak recovery','breakup vlog',
  'emotional healing journey','attachment style','anxious attachment',
  'toxic relationship recovery','narcissist recovery','self love journey',

  // Beauty & soft girl
  'soft girl makeup','soft girl aesthetic','that girl makeup',
  'grwm get ready with me','grwm vlog','get ready with me aesthetic',
  'makeup tutorial beginner','everyday makeup routine','no makeup makeup',
  'skincare routine','skincare for beginners','glass skin routine',
  'nail art tutorial','nail art for beginners','aesthetic nail art',
  'hair tutorial','hair care routine','curly hair routine',

  // Fashion & aesthetic
  'aesthetic outfits','outfit of the day','fashion haul',
  'thrift flip','thrift haul','sustainable fashion',
  'dark feminine aesthetic','dark academia aesthetic','light academia aesthetic',
  'coquette aesthetic','balletcore aesthetic','fairycore aesthetic',
  'y2k fashion','y2k aesthetic vlog','vintage fashion haul',

  // Spiritual & tarot
  'tarot reading','daily tarot','tarot card reading 2026',
  'astrology reading','birth chart reading','zodiac vlog',
  'spiritual vlog','spiritual awakening','manifestation routine',
  'law of attraction','angel numbers','spiritual self care',
  'crystal collection','crystal healing','witchtok vlog',

  // Books & journaling
  'booktube','book recommendations','reading vlog',
  'bullet journal','journal with me','journaling vlog',
  'stationery haul','stationery collection','desk setup aesthetic',
  'study with me','study vlog','productive day in my life',
  'college study vlog','student vlog','university vlog',

  // Hobbies with same demographic
  'crochet for beginners','crochet vlog','knitting vlog',
  'embroidery vlog','sewing vlog','craft vlog',
  'painting vlog','art vlog','sketchbook tour',
  'anime vlog','anime collection','studio ghibli collection',
  'pet vlog','cat vlog','dog vlog',

  // Relationship & dating
  'relationship advice','dating advice for women','situationship vlog',
  'boundaries in relationships','people pleasing recovery',
  'codependency healing','therapy helped me',

  // Food & comfort
  'comfort food recipes','cozy cooking vlog','baking vlog',
  'cafe vlog','coffee vlog','matcha vlog',

  // College & young adult stress
  'college stress vlog','college anxiety','first year college vlog',
  'college day in my life','dorm room tour','apartment tour college',
  'adulting vlog','quarter life crisis','20s vlog'
];

// ─── DATABASE ────────────────────────────────────────────────────────────────
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
  await pool.query(`CREATE TABLE IF NOT EXISTS seen_channels (channel_id TEXT PRIMARY KEY)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT)`);
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
  } catch(e) { return { batchNumber: 0, totalFound: 0, lastRunAt: null, isRunning: false }; }
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

// ─── EXCEL EXPORT ────────────────────────────────────────────────────────────
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

// ─── YOUTUBE API ─────────────────────────────────────────────────────────────
async function ytFetch(url, keys) {
  let key = getNextKey(keys);
  while (key) {
    try {
      const res = await fetch(url + `&key=${key}`);
      const data = await res.json();
      if (data.error) {
        if (data.error.code === 403 || data.error.message?.includes('quota')) {
          markKeyExhausted(keys, key);
          key = getNextKey(keys);
          if (!key) return null;
          continue;
        }
        return null;
      }
      return data;
    } catch(e) { return null; }
  }
  return null;
}

async function searchVideos(query, keys) {
  const data = await ytFetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(query)}&type=video&maxResults=15&order=relevance`, keys);
  return data?.items || [];
}

async function fetchChannelDetails(channelId, keys) {
  const data = await ytFetch(`https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,brandingSettings&id=${channelId}`, keys);
  return data?.items?.[0] || null;
}

async function getChannelVideos(channelId, keys) {
  const data = await ytFetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channelId}&type=video&maxResults=10&order=date`, keys);
  return data?.items || [];
}

async function fetchVideoStats(videoIds, keys) {
  const data = await ytFetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${videoIds.join(',')}`, keys);
  return data?.items || [];
}

function extractEmail(text) {
  if (!text) return null;
  const match = text.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/);
  return match ? match[0] : null;
}

function getNiche(title, description) {
  const text = ((title || '') + ' ' + (description || '')).toLowerCase();
  if (text.match(/asmr/)) return 'asmr';
  if (text.match(/mental health|anxiety|depression|therapy|mindful|wellness|healing/)) return 'mental health';
  if (text.match(/tarot|astrology|spiritual|manifestation|zodiac|crystal|witch/)) return 'spiritual';
  if (text.match(/adhd|autism|neurodivergent/)) return 'neurodivergent';
  if (text.match(/chronic illness|chronic pain|spoonie|fibro/)) return 'chronic illness';
  if (text.match(/kawaii|plush|squishmallow|sanrio|jellycat/)) return 'kawaii';
  if (text.match(/cottagecore|cottage|fairy|goblin|dark academia/)) return 'aesthetic';
  if (text.match(/cozy|hygge|slow living|soft life/)) return 'cozy lifestyle';
  if (text.match(/makeup|beauty|skincare|nail|grwm|get ready/)) return 'beauty';
  if (text.match(/fashion|outfit|ootd|style|haul|thrift/)) return 'fashion';
  if (text.match(/book|read|journal|bullet journal|stationery/)) return 'books & journaling';
  if (text.match(/crochet|knit|sew|craft|embroid/)) return 'crafts';
  if (text.match(/anime|ghibli|manga/)) return 'anime';
  if (text.match(/study|student|college|university|school/)) return 'student life';
  if (text.match(/self.?care|self love|glow up|that girl/)) return 'self care';
  if (text.match(/introvert|alone|solo|lonely/)) return 'introvert';
  if (text.match(/grief|heartbreak|breakup|healing/)) return 'emotional healing';
  if (text.match(/pet|cat|dog|kitten/)) return 'pets';
  if (text.match(/coffee|matcha|cafe|bak|cook/)) return 'food & cozy';
  return 'lifestyle';
}

// ─── MAIN BATCH ───────────────────────────────────────────────────────────────
async function runBatch(keys) {
  exhaustedKeys = new Set();
  const seen = await getSeenChannels();
  const results = [];
  const target = 500;
  const shuffled = [...SEARCH_QUERIES].sort(() => Math.random() - 0.5);

  for (const query of shuffled) {
    if (results.length >= target) break;
    if (getNextKey(keys) === null) { log('All API keys exhausted'); break; }

    try {
      log(`Searching: "${query}" (${results.length}/${target} found)`);
      const videos = await searchVideos(query, keys);

      for (const video of videos) {
        if (results.length >= target) break;
        if (getNextKey(keys) === null) break;

        const channelId = video.snippet?.channelId;
        if (!channelId || seen[channelId]) continue;
        seen[channelId] = true;

        const channel = await fetchChannelDetails(channelId, keys);
        if (!channel) continue;

        const subs = parseInt(channel.statistics?.subscriberCount || 0);
        if (subs < 1000 || subs > 500000) { await addSeenChannel(channelId); continue; }

        const channelVideos = await getChannelVideos(channelId, keys);
        if (channelVideos.length < 3) { await addSeenChannel(channelId); continue; }

        const videoIds = channelVideos.map(v => v.id?.videoId).filter(Boolean);
        if (videoIds.length === 0) { await addSeenChannel(channelId); continue; }

        const stats = await fetchVideoStats(videoIds, keys);
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
        const handle = channel.snippet?.customUrl || ('@' + (channel.snippet?.title || '').replace(/\s+/g,'').toLowerCase());
        const email = extractEmail(desc) || extractEmail(channel.brandingSettings?.channel?.description);
        const niche = getNiche(channel.snippet?.title, desc);
        const firstName = (channel.snippet?.title || 'Unknown').split(' ')[0];

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
        log(`✓ Found: ${firstName} (${handle}) | ${Math.round(avgViews)} avg views | ${niche}`);
      }
    } catch(e) {
      log(`Query "${query}" error: ${e.message}`);
    }
  }

  log(`Batch complete: ${results.length} creators found`);
  return results;
}

async function executeBatch(apiKeyOrKeys) {
  const keys = Array.isArray(apiKeyOrKeys) ? apiKeyOrKeys : getApiKeys();
  if (keys.length === 0) return { success: false, message: 'No API keys configured' };

  const state = await getState();
  if (state.isRunning) return { success: false, message: 'Already running' };

  state.isRunning = true;
  await saveState(state);

  try {
    const rows = await runBatch(keys);
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
    const s = await getState();
    await saveState({ ...s, isRunning: false });
    log(`Batch failed: ${err.message}`);
    return { success: false, message: err.message };
  }
}

function startScheduler() {
  initDb().then(() => {
    log(`Scheduler ready. ${getApiKeys().length} API key(s) loaded. Runs daily at 2:00 AM UTC`);
    setInterval(async () => {
      const now = new Date();
      if (now.getUTCHours() === 2 && now.getUTCMinutes() === 0) {
        const keys = getApiKeys();
        if (keys.length > 0) {
          log('Starting scheduled batch...');
          await executeBatch(keys);
        }
      }
    }, 60000);
  }).catch(e => log('DB init error: ' + e.message));
}

module.exports = { startScheduler, executeBatch, getState, getLastResults, generateExcel, initDb, RESULTS_PATH, getApiKeys };
