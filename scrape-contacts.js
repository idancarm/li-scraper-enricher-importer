import https from 'node:https';
import fs from 'node:fs';
import { loadEnv, sleep, randomDelay } from './lib.js';

const env = loadEnv();
const API_KEY = env.UNIPILE_API_KEY;
const DSN = env.UNIPILE_DSN;
const ACCOUNT_ID = env.UNIPILE_ACCOUNT_ID;
const DATA_FILE = 'data/contacts.json';

function request(cursor) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({ account_id: ACCOUNT_ID, limit: '100' });
    if (cursor) params.set('cursor', cursor);

    const url = `https://${DSN}/api/v1/users/relations?${params}`;
    const req = https.get(url, { headers: { 'X-API-KEY': API_KEY } }, (res) => {
      let body = '';
      res.on('data', (chunk) => (body += chunk));
      res.on('end', () => {
        if (res.statusCode === 429 || res.statusCode === 403) {
          reject(new Error(`Rate limited or forbidden: ${res.statusCode}`));
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}`));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function extractContact(item) {
  return {
    first_name: item.first_name || '',
    last_name: item.last_name || '',
    headline: item.headline || '',
    public_profile_url: item.public_profile_url || '',
    public_identifier: item.public_identifier || '',
  };
}

async function main() {
  // Load existing contacts for resume support
  let contacts = [];
  if (fs.existsSync(DATA_FILE)) {
    contacts = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
    console.log(`Resuming — ${contacts.length} contacts already saved`);
  }

  // Find the last cursor if we saved it
  let cursor = null;
  const cursorFile = 'data/.scrape-cursor';
  if (fs.existsSync(cursorFile)) {
    cursor = fs.readFileSync(cursorFile, 'utf-8').trim() || null;
    if (cursor) console.log(`Resuming from cursor: ${cursor}`);
  }

  let page = 0;

  while (true) {
    page++;
    console.log(`Fetching page ${page}... (cursor: ${cursor || 'start'})`);

    let data;
    try {
      data = await request(cursor);
    } catch (err) {
      console.error(`HALTING: ${err.message}`);
      console.error(`Saved ${contacts.length} contacts so far. Re-run to resume.`);
      break;
    }

    const items = data.items || data.data || [];
    if (items.length === 0) {
      console.log('No more items returned.');
      break;
    }

    for (const item of items) {
      contacts.push(extractContact(item));
    }

    // Save after each page (crash-safe)
    fs.writeFileSync(DATA_FILE, JSON.stringify(contacts, null, 2));

    cursor = data.cursor || null;
    // Save cursor for resume
    fs.writeFileSync(cursorFile, cursor || '');

    console.log(`  Got ${items.length} contacts (total: ${contacts.length})`);

    if (!cursor) {
      console.log('No more pages (cursor is null).');
      break;
    }

    // Randomized 2-5s delay between pages
    await randomDelay(2000, 5000);
  }

  console.log(`\nDone. Total contacts saved: ${contacts.length}`);
}

main();
