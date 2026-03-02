import https from 'node:https';
import { loadEnv, sleep, readJSON, writeJSON } from './lib.js';

const env = loadEnv();
const APOLLO_API_KEY = env.APOLLO_API_KEY;
const CARGO_API_KEY = env.CARGO_API_KEY;

const CONTACTS_FILE = 'data/contacts.json';
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

// --- Apollo ---

async function tryApollo(contact) {
  if (!APOLLO_API_KEY) return null;

  const params = new URLSearchParams({
    linkedin_url: contact.public_profile_url,
    first_name: contact.first_name,
    last_name: contact.last_name,
    reveal_personal_emails: 'false',
  });

  const data = await postJSON(
    'api.apollo.io',
    `/api/v1/people/match?${params}`,
    { 'x-api-key': APOLLO_API_KEY, 'Cache-Control': 'no-cache' },
    {}
  );

  const person = data.person;
  if (!person) return null;
  if (!person.email) return null;
  if (person.email_status === 'bounced') return null;
  if (!person.organization) return null;

  return {
    email: person.email,
    company: person.organization.name || '',
    jobtitle: person.title || '',
  };
}

// --- Cargo ---

async function tryCargo(contact) {
  if (!CARGO_API_KEY) return null;

  const data = await postJSON(
    'api.getcargo.io',
    '/v1/tools/017fd330-fc34-42a5-b608-25897c94ba28/execute',
    { Authorization: `Bearer ${CARGO_API_KEY}` },
    { linkedinUrl: contact.public_profile_url }
  );

  // Parse email from various response shapes
  let email =
    data.email ||
    data.data?.email ||
    data.result?.email ||
    data.output?.email ||
    null;

  if (!email && typeof data.output === 'string' && data.output.includes('@')) {
    email = data.output.trim();
  }

  if (!email || !email.includes('@')) return null;

  return { email };
}

// --- Headline parser ---

function parseHeadline(headline) {
  if (!headline) return { jobtitle: '', company: '' };
  // Try "Title at Company" pattern
  const atMatch = headline.match(/^(.+?)\s+at\s+(.+)$/i);
  if (atMatch) return { jobtitle: atMatch[1].trim(), company: atMatch[2].trim() };
  // Try "Title | Company" or "Title - Company"
  const sepMatch = headline.match(/^(.+?)\s*[|\-–—]\s*(.+)$/);
  if (sepMatch) return { jobtitle: sepMatch[1].trim(), company: sepMatch[2].trim() };
  return { jobtitle: headline, company: '' };
}

// --- Main ---

async function main() {
  const contacts = readJSON(CONTACTS_FILE);
  if (contacts.length === 0) {
    console.error('No contacts found. Run scrape-contacts.js first.');
    process.exit(1);
  }

  const enriched = readJSON(ENRICHED_FILE);
  const unenriched = readJSON(UNENRICHED_FILE);

  // Build set of already-processed identifiers for resume
  const processed = new Set([
    ...enriched.map((c) => c.linkedin_url),
    ...unenriched.map((c) => c.linkedin_url),
  ]);

  const remaining = contacts.filter((c) => !processed.has(c.public_profile_url));
  console.log(`Total: ${contacts.length} | Already processed: ${processed.size} | Remaining: ${remaining.length}`);

  let apolloCount = 0;
  let cargoCount = 0;
  let failCount = 0;

  for (let i = 0; i < remaining.length; i++) {
    const contact = remaining[i];
    const label = `[${processed.size + i + 1}/${contacts.length}] ${contact.first_name} ${contact.last_name}`;

    // 1. Try Apollo
    let result = null;
    try {
      result = await tryApollo(contact);
      if (result) {
        console.log(`${label} — Apollo ✓`);
        apolloCount++;
      }
    } catch (err) {
      if (err.message === 'RATE_LIMITED') {
        console.error(`\nAPOLLO RATE LIMITED. Halting. Re-run to resume.`);
        console.log(`Progress: Apollo=${apolloCount} Cargo=${cargoCount} Failed=${failCount}`);
        process.exit(1);
      }
      console.log(`${label} — Apollo error: ${err.message}`);
    }

    // 2. Try Cargo if Apollo didn't work
    if (!result) {
      await sleep(500); // brief pause between APIs
      try {
        result = await tryCargo(contact);
        if (result) {
          console.log(`${label} — Cargo ✓`);
          cargoCount++;
        }
      } catch (err) {
        if (err.message === 'RATE_LIMITED') {
          console.error(`\nCARGO RATE LIMITED. Halting. Re-run to resume.`);
          console.log(`Progress: Apollo=${apolloCount} Cargo=${cargoCount} Failed=${failCount}`);
          process.exit(1);
        }
        console.log(`${label} — Cargo error: ${err.message}`);
      }
    }

    if (result) {
      const parsed = parseHeadline(contact.headline);
      enriched.push({
        first_name: contact.first_name,
        last_name: contact.last_name,
        email: result.email,
        company: result.company || parsed.company,
        jobtitle: result.jobtitle || parsed.jobtitle,
        linkedin_url: contact.public_profile_url,
        enriched_by: result.company ? 'apollo' : 'cargo',
      });
    } else {
      console.log(`${label} — UNENRICHED`);
      failCount++;
      unenriched.push({
        first_name: contact.first_name,
        last_name: contact.last_name,
        headline: contact.headline,
        linkedin_url: contact.public_profile_url,
        reason: 'no_email_found',
      });
    }

    // Save after each contact (crash-safe)
    writeJSON(ENRICHED_FILE, enriched);
    writeJSON(UNENRICHED_FILE, unenriched);

    // Rate limiting delay
    if (result?.company) {
      // Apollo was used — 500ms
      await sleep(500);
    } else {
      // Cargo was used or both failed — 1.5s
      await sleep(1500);
    }
  }

  console.log(`\nDone.`);
  console.log(`  Enriched (Apollo): ${apolloCount}`);
  console.log(`  Enriched (Cargo):  ${cargoCount}`);
  console.log(`  Unenriched:        ${failCount}`);
  console.log(`  Total enriched:    ${enriched.length}`);
  console.log(`  Total unenriched:  ${unenriched.length}`);
}

main();
