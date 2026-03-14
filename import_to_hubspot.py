#!/usr/bin/env python3
"""Batch import enriched contacts into HubSpot CRM."""

import math
import os
import sys

import requests

from lib import init_env, read_json, sleep_ms, write_json

init_env()

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_API_TOKEN', '')
ENRICHED_FILE = 'data/enriched.json'
PROGRESS_FILE = 'data/.import-progress'

BATCH_SIZE = 10
BATCH_DELAY = 1000


def batch_create(contacts):
    inputs = [
        {
            'properties': {
                'email': c['email'],
                'firstname': c['first_name'],
                'lastname': c['last_name'],
                'jobtitle': c.get('jobtitle', ''),
                'company': c.get('company', ''),
                'hs_linkedin_url': c.get('linkedin_url', ''),
                'do_not_email': 'true',
            }
        }
        for c in contacts
    ]

    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/contacts/batch/create',
        json={'inputs': inputs},
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        },
        timeout=30,
    )
    if resp.status_code == 429:
        raise Exception('RATE_LIMITED')
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return {'status': resp.status_code, 'body': body}


def main():
    enriched = read_json(ENRICHED_FILE)
    if not enriched:
        print('No enriched contacts found. Run enrich_contacts.py first.')
        sys.exit(1)

    # Resume support
    start_index = 0
    try:
        saved = read_json(PROGRESS_FILE)
        if isinstance(saved, int):
            start_index = saved
    except Exception:
        pass

    total_batches = math.ceil(len(enriched) / BATCH_SIZE)
    start_batch = start_index // BATCH_SIZE
    print(f'Importing {len(enriched)} contacts in {total_batches} batches of {BATCH_SIZE}')
    if start_batch > 0:
        print(f'Resuming from batch {start_batch + 1}')

    created = 0
    duplicates = 0
    errors = 0

    for batch_num in range(start_batch, total_batches):
        start = batch_num * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(enriched))
        batch = enriched[start:end]

        print(f'Batch {batch_num + 1}/{total_batches} (contacts {start + 1}-{end})...')

        retries = 0
        while True:
            try:
                res = batch_create(batch)

                if res['status'] == 201:
                    created += len(batch)
                    print(f'  Created {len(batch)} contacts')
                elif res['status'] in (409, 207):
                    results = res['body'].get('results', []) if isinstance(res['body'], dict) else []
                    errs = res['body'].get('errors', []) if isinstance(res['body'], dict) else []
                    created += len(results)
                    duplicates += len(errs)
                    print(f'  Partial: {len(results)} created, {len(errs)} duplicates/errors')
                else:
                    body_str = str(res['body'])[:200]
                    print(f'  Unexpected status {res["status"]}: {body_str}')
                    errors += len(batch)
                break
            except Exception as err:
                if str(err) == 'RATE_LIMITED':
                    retries += 1
                    backoff = min(1000 * (2 ** retries), 60000)
                    print(f'  Rate limited. Backing off {backoff / 1000}s...')
                    sleep_ms(backoff)
                    continue
                print(f'  Error: {err}')
                errors += len(batch)
                break

        write_json(PROGRESS_FILE, end)
        sleep_ms(BATCH_DELAY)

    print(f'\nDone.')
    print(f'  Created:    {created}')
    print(f'  Duplicates: {duplicates}')
    print(f'  Errors:     {errors}')
    print(f'\nNext: In HubSpot UI, filter contacts by contact_source = "linkedin-import" and set them as non-marketing.')


if __name__ == '__main__':
    main()
