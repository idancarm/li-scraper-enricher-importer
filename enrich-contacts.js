import https from 'node:https';
import { loadEnv, sleep, readJSON, writeJSON } from './lib.js';

const env = loadEnv();
const APOLLO_KEY = env.APOLLO_ENRICH_API_KEY;
const CARGO_API_KEY = env.CARGO_API_KEY;

const CONTACTS_FILE = 'data/filtered.json';
const ENRICHED_FILE = 'data/enriched.json';
const UNENRICHED_FILE = 'data/unenriched.json';

// --- HTTP helpers ---

function postJSON(hostname, path, headers, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const opts = {
      hostname,
      path,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
    };
    const req = https.request(opts, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        if (res.statusCode === 429) {
          reject(new Error('RATE_LIMITED'));
          return;
        }
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode}: ${data}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch {
          resolve({ raw: data });
        }
      });
    });
    req.setTimeout(60000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// --- Cargo (primary — finds email) ---

async function tryCargo(contact) {
  if (!CARGO_API_KEY) return null;

  const data = await postJSON(
    'api.getcargo.io',
    '/v1/tools/017fd330-fc34-42a5-b608-25897c94ba28/execute',
    { Authorization: `Bearer ${CARGO_API_KEY}` },
    { linkedinUrl: contact.public_profile_url }
  );

  let email =
    data.email ||
    data.data?.email ||
    data.result?.email ||
    data.result?.output?.email ||
    data.output?.email ||
    null;

  if (!email && typeof data.output === 'string' && data.output.includes('@')) {
    email = data.output.trim();
  }

  if (!email || !email.includes('@')) return null;

  return { email };
}

// --- Apollo (supplement — finds company + title metadata) ---

async function getApolloMeta(contact) {
  if (!APOLLO_KEY) return null;

  try {
    const data = await postJSON(
      'api.apollo.io',
      '/api/v1/people/match',
      { 'x-api-key': APOLLO_KEY, 'Cache-Control': 'no-cache' },
      {
        linkedin_url: contact.public_profile_url,
        first_name: contact.first_name,
        last_name: contact.last_name,
      }
    );

    const person = data.person;
    if (!person) return null;

    return {
      company: person.organization?.name || '',
      jobtitle: person.title || '',
    };
  } catch {
    return null;
  }
}

// --- Headline parser (fallback for company/title) ---

function parseHeadline(headline) {
  if (!headline) return { jobtitle: '', company: '' };
  const atMatch = headline.match(/^(.+?)\s+at\s+(.+)$/i);
  if (atMatch) return { jobtitle: atMatch[1].trim(), company: atMatch[2].trim() };
  const sepMatch = headline.match(/^(.+?)\s*[|\-–—]\s*(.+)$/);
  if (sepMatch) return { jobtitle: sepMatch[1].trim(), company: sepMatch[2].trim() };
  return { jobtitle: headline, company: '' };
}

// --- Main ---

async function main() {
  const contacts = readJSON(CONTACTS_FILE);
  if (contacts.length === 0) {
    console.error('No contacts found. Run filter-contacts.js first.');
    process.exit(1);
  }

  const enriched = readJSON(ENRICHED_FILE);
  const unenriched = readJSON(UNENRICHED_FILE);

  const processed = new Set([
    ...enriched.map((c) => c.linkedin_url),
    ...unenriched.map((c) => c.linkedin_url),
  ]);

  const remaining = contacts.filter((c) => !processed.has(c.public_profile_url));
  console.log(`Total: ${contacts.length} | Already processed: ${processed.size} | Remaining: ${remaining.length}`);

  let cargoCount = 0;
  let failCount = 0;

  for (let i = 0; i < remaining.length; i++) {
    const contact = remaining[i];
    const label = `[${processed.size + i + 1}/${contacts.length}] ${contact.first_name} ${contact.last_name}`;

    // 1. Try Cargo for email
    let email = null;
    try {
      const cargoResult = await tryCargo(contact);
      if (cargoResult) email = cargoResult.email;
    } catch (err) {
      if (err.message === 'RATE_LIMITED') {
        console.error(`\nCARGO RATE LIMITED. Halting. Re-run to resume.`);
        console.log(`Progress: Enriched=${cargoCount} Failed=${failCount}`);
        process.exit(1);
      }
      console.log(`${label} — Cargo error: ${err.message}`);
    }

    if (email) {
      // 2. Try Apollo for company/title metadata
      const meta = await getApolloMeta(contact);
      const parsed = parseHeadline(contact.headline);

      enriched.push({
        first_name: contact.first_name,
        last_name: contact.last_name,
        email,
        company: meta?.company || parsed.company,
        jobtitle: meta?.jobtitle || parsed.jobtitle,
        linkedin_url: contact.public_profile_url,
        enriched_by: meta?.company ? 'cargo+apollo' : 'cargo',
      });

      cargoCount++;
      console.log(`${label} — ${email} (${meta?.company || parsed.company || 'no company'})`);
    } else {
      failCount++;
      unenriched.push({
        first_name: contact.first_name,
        last_name: contact.last_name,
        headline: contact.headline,
        linkedin_url: contact.public_profile_url,
        reason: 'no_email_found',
      });
      console.log(`${label} — UNENRICHED`);
    }

    // Save after each contact (crash-safe)
    writeJSON(ENRICHED_FILE, enriched);
    writeJSON(UNENRICHED_FILE, unenriched);

    // 1.5s delay between contacts (Cargo rate limit)
    await sleep(1500);
  }

  console.log(`\nDone.`);
  console.log(`  Enriched: ${cargoCount}`);
  console.log(`  Unenriched: ${failCount}`);
  console.log(`  Total enriched: ${enriched.length}`);
  console.log(`  Total unenriched: ${unenriched.length}`);
}

main();
