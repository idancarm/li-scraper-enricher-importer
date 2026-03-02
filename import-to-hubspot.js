import https from 'node:https';
import { loadEnv, sleep, readJSON, writeJSON } from './lib.js';

const env = loadEnv();
const HUBSPOT_TOKEN = env.HUBSPOT_API_TOKEN;
const ENRICHED_FILE = 'data/enriched.json';
const PROGRESS_FILE = 'data/.import-progress';

const BATCH_SIZE = 10;
const BATCH_DELAY = 1000;

function batchCreate(contacts) {
  const inputs = contacts.map((c) => ({
    properties: {
      email: c.email,
      firstname: c.first_name,
      lastname: c.last_name,
      jobtitle: c.jobtitle || '',
      company: c.company || '',
      hs_linkedin_url: c.linkedin_url || '',
      do_not_email: 'true',
    },
  }));

  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({ inputs });
    const opts = {
      hostname: 'api.hubapi.com',
      path: '/crm/v3/objects/contacts/batch/create',
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
        try {
          const parsed = JSON.parse(data);
          resolve({ status: res.statusCode, body: parsed });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  const enriched = readJSON(ENRICHED_FILE);
  if (enriched.length === 0) {
    console.error('No enriched contacts found. Run enrich-contacts.js first.');
    process.exit(1);
  }

  // Resume support: load last completed batch index
  let startIndex = 0;
  try {
    const saved = readJSON(PROGRESS_FILE);
    if (typeof saved === 'number') startIndex = saved;
  } catch {}

  const totalBatches = Math.ceil(enriched.length / BATCH_SIZE);
  const startBatch = Math.floor(startIndex / BATCH_SIZE);
  console.log(`Importing ${enriched.length} contacts in ${totalBatches} batches of ${BATCH_SIZE}`);
  if (startBatch > 0) console.log(`Resuming from batch ${startBatch + 1}`);

  let created = 0;
  let duplicates = 0;
  let errors = 0;

  for (let batch = startBatch; batch < totalBatches; batch++) {
    const start = batch * BATCH_SIZE;
    const end = Math.min(start + BATCH_SIZE, enriched.length);
    const slice = enriched.slice(start, end);

    console.log(`Batch ${batch + 1}/${totalBatches} (contacts ${start + 1}-${end})...`);

    let retries = 0;
    while (true) {
      try {
        const res = await batchCreate(slice);

        if (res.status === 201) {
          created += slice.length;
          console.log(`  ✓ Created ${slice.length} contacts`);
        } else if (res.status === 409 || res.status === 207) {
          // Partial success or conflicts
          const results = res.body.results || [];
          const errs = res.body.errors || [];
          created += results.length;
          duplicates += errs.length;
          console.log(`  Partial: ${results.length} created, ${errs.length} duplicates/errors`);
        } else {
          console.log(`  Unexpected status ${res.status}: ${JSON.stringify(res.body).slice(0, 200)}`);
          errors += slice.length;
        }
        break;
      } catch (err) {
        if (err.message === 'RATE_LIMITED') {
          retries++;
          const backoff = Math.min(1000 * Math.pow(2, retries), 60000);
          console.log(`  Rate limited. Backing off ${backoff / 1000}s...`);
          await sleep(backoff);
          continue;
        }
        console.error(`  Error: ${err.message}`);
        errors += slice.length;
        break;
      }
    }

    // Save progress
    writeJSON(PROGRESS_FILE, end);
    await sleep(BATCH_DELAY);
  }

  console.log(`\nDone.`);
  console.log(`  Created:    ${created}`);
  console.log(`  Duplicates: ${duplicates}`);
  console.log(`  Errors:     ${errors}`);
  console.log(`\nNext: In HubSpot UI, filter contacts by contact_source = "linkedin-import" and set them as non-marketing.`);
}

main();
