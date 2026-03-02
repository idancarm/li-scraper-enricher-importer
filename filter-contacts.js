import https from 'node:https';
import { loadEnv, sleep, readJSON, writeJSON } from './lib.js';

const env = loadEnv();
const HUBSPOT_TOKEN = env.HUBSPOT_API_TOKEN;

const CONTACTS_FILE = 'data/contacts.json';
const FILTERED_FILE = 'data/filtered.json';
const EXCLUDED_FILE = 'data/excluded.json';
const PROGRESS_FILE = 'data/.filter-progress';

// --- Headline blocklist ---

const HEADLINE_PATTERNS = [
  // Consultants
  { pattern: /consultant/i, keyword: 'consultant' },
  { pattern: /consulting/i, keyword: 'consulting' },
  { pattern: /advisory/i, keyword: 'advisory' },
  { pattern: /advisor/i, keyword: 'advisor' },
  // Freelancers
  { pattern: /freelance/i, keyword: 'freelance' },
  { pattern: /freelancer/i, keyword: 'freelancer' },
  { pattern: /independent/i, keyword: 'independent' },
  // Agency
  { pattern: /agency/i, keyword: 'agency' },
  { pattern: /we help/i, keyword: 'we help' },
  { pattern: /we build/i, keyword: 'we build' },
  { pattern: /helping\s+.*?\s*(companies|businesses|brands|startups)/i, keyword: 'helping companies/businesses' },
  // Service sellers
  { pattern: /for hire/i, keyword: 'for hire' },
  { pattern: /available for/i, keyword: 'available for' },
  { pattern: /open to work/i, keyword: 'open to work' },
  { pattern: /seeking opportunities/i, keyword: 'seeking opportunities' },
  // HubSpot ecosystem
  { pattern: /hubspot consultant/i, keyword: 'hubspot consultant' },
  { pattern: /hubspot partner/i, keyword: 'hubspot partner' },
  { pattern: /hubspot certified/i, keyword: 'hubspot certified' },
  { pattern: /hubspot expert/i, keyword: 'hubspot expert' },
  { pattern: /hubspot implementation/i, keyword: 'hubspot implementation' },
  { pattern: /revops agency/i, keyword: 'revops agency' },
  { pattern: /martech agency/i, keyword: 'martech agency' },
  { pattern: /crm consultant/i, keyword: 'crm consultant' },
  { pattern: /crm implementation/i, keyword: 'crm implementation' },
];

function checkHeadline(headline) {
  if (!headline) return null;
  for (const { pattern, keyword } of HEADLINE_PATTERNS) {
    if (pattern.test(headline)) return keyword;
  }
  return null;
}

// --- HubSpot search ---

function searchHubSpot(query) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      query,
      limit: 10,
      properties: ['firstname', 'lastname'],
    });
    const opts = {
      hostname: 'api.hubapi.com',
      path: '/crm/v3/objects/contacts/search',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${HUBSPOT_TOKEN}`,
      },
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
          resolve({ total: 0, results: [] });
        }
      });
    });
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function checkHubSpotExists(firstName, lastName) {
  const query = `${firstName} ${lastName}`;
  const data = await searchHubSpot(query);

  if (!data.total || data.total === 0) return null;

  for (const result of data.results || []) {
    const props = result.properties || {};
    const hsFirst = (props.firstname || '').toLowerCase();
    const hsLast = (props.lastname || '').toLowerCase();
    if (hsFirst === firstName.toLowerCase() && hsLast === lastName.toLowerCase()) {
      return result.id;
    }
  }
  return null;
}

// --- Main ---

async function main() {
  const contacts = readJSON(CONTACTS_FILE);
  if (contacts.length === 0) {
    console.error('No contacts found. Run scrape-contacts.js first.');
    process.exit(1);
  }

  let filtered = readJSON(FILTERED_FILE);
  let excluded = readJSON(EXCLUDED_FILE);

  // Resume support: track which contacts have been processed
  const processedUrls = new Set([
    ...filtered.map((c) => c.public_profile_url),
    ...excluded.map((c) => c.public_profile_url),
  ]);

  const remaining = contacts.filter((c) => !processedUrls.has(c.public_profile_url));
  console.log(`Total: ${contacts.length} | Already processed: ${processedUrls.size} | Remaining: ${remaining.length}`);

  let headlineExcluded = 0;
  let hubspotExcluded = 0;
  let passed = 0;

  for (let i = 0; i < remaining.length; i++) {
    const contact = remaining[i];
    const label = `[${processedUrls.size + i + 1}/${contacts.length}] ${contact.first_name} ${contact.last_name}`;

    // Filter A: Headline check
    const matchedKeyword = checkHeadline(contact.headline);
    if (matchedKeyword) {
      excluded.push({
        ...contact,
        reason: 'headline_filter',
        matched_keyword: matchedKeyword,
      });
      headlineExcluded++;
      console.log(`${label} — EXCLUDED (headline: "${matchedKeyword}")`);
      writeJSON(EXCLUDED_FILE, excluded);
      continue;
    }

    // Filter B: HubSpot dedup
    try {
      const hubspotId = await checkHubSpotExists(contact.first_name, contact.last_name);
      if (hubspotId) {
        excluded.push({
          ...contact,
          reason: 'already_in_hubspot',
          hubspot_id: hubspotId,
        });
        hubspotExcluded++;
        console.log(`${label} — EXCLUDED (already in HubSpot, ID: ${hubspotId})`);
        writeJSON(EXCLUDED_FILE, excluded);
        await sleep(100);
        continue;
      }
    } catch (err) {
      if (err.message === 'RATE_LIMITED') {
        console.error(`\nHubSpot RATE LIMITED. Halting. Re-run to resume.`);
        console.log(`Progress: ${headlineExcluded} headline + ${hubspotExcluded} hubspot excluded, ${passed} passed`);
        process.exit(1);
      }
      console.log(`${label} — HubSpot lookup error: ${err.message} (keeping contact)`);
    }

    // Passed both filters
    filtered.push(contact);
    passed++;
    console.log(`${label} — PASSED`);

    writeJSON(FILTERED_FILE, filtered);
    await sleep(100);
  }

  console.log(`\nDone.`);
  console.log(`  Total:                ${contacts.length}`);
  console.log(`  Excluded (headline):  ${headlineExcluded}`);
  console.log(`  Excluded (HubSpot):   ${hubspotExcluded}`);
  console.log(`  Ready for enrichment: ${passed}`);
  console.log(`  Total filtered:       ${filtered.length}`);
  console.log(`  Total excluded:       ${excluded.length}`);
}

main();
