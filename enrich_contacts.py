#!/usr/bin/env python3
"""Enrich filtered contacts with email (Cargo) and metadata (Apollo)."""

import os
import re
import sys
from pathlib import Path

import requests

from lib import init_env, read_json, sleep_ms, write_json

init_env()

APOLLO_KEY = os.environ.get('APOLLO_ENRICH_API_KEY', '')
CARGO_API_KEY = os.environ.get('CARGO_API_KEY', '')

CONTACTS_FILE = 'data/filtered.json'
ENRICHED_FILE = 'data/enriched.json'
UNENRICHED_FILE = 'data/unenriched.json'
PROCESSED_FILE = 'data/.processed-urls'
MAX_CONTACTS = int(sys.argv[1]) if len(sys.argv) > 1 else float('inf')

PERSONAL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aol.com', 'icloud.com', 'me.com', 'mail.com',
    'yahoo.co.uk', 'hotmail.co.uk', 'live.com', 'msn.com',
}


def is_personal_email(email):
    domain = email.split('@')[-1].lower()
    return domain in PERSONAL_DOMAINS


# --- Persistent processed URL tracking ---

def load_processed_urls():
    p = Path(PROCESSED_FILE)
    if not p.exists():
        return set()
    return set(line for line in p.read_text().splitlines() if line)


def mark_processed(url):
    with open(PROCESSED_FILE, 'a') as f:
        f.write(url + '\n')


# --- Cargo (primary — finds email) ---

def try_cargo(contact):
    if not CARGO_API_KEY:
        return None

    resp = requests.post(
        'https://api.getcargo.io/v1/tools/017fd330-fc34-42a5-b608-25897c94ba28/execute',
        json={'linkedinUrl': contact['public_profile_url']},
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {CARGO_API_KEY}',
        },
        timeout=60,
    )
    if resp.status_code == 429:
        raise Exception('RATE_LIMITED')
    resp.raise_for_status()
    data = resp.json()

    email = (
        data.get('email')
        or (data.get('data') or {}).get('email')
        or (data.get('result') or {}).get('email')
        or ((data.get('result') or {}).get('output') or {}).get('email')
        or (data.get('output') if isinstance(data.get('output'), dict) else {}).get('email')
    )

    if not email and isinstance(data.get('output'), str) and '@' in data['output']:
        email = data['output'].strip()

    if not email or '@' not in email:
        return None

    return {'email': email}


# --- Apollo (supplement — finds company + title metadata) ---

def get_apollo_meta(contact):
    if not APOLLO_KEY:
        return None

    try:
        resp = requests.post(
            'https://api.apollo.io/api/v1/people/match',
            json={
                'linkedin_url': contact['public_profile_url'],
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
            },
            headers={
                'Content-Type': 'application/json',
                'x-api-key': APOLLO_KEY,
                'Cache-Control': 'no-cache',
            },
            timeout=60,
        )
        resp.raise_for_status()
        person = resp.json().get('person')
        if not person:
            return None
        return {
            'company': (person.get('organization') or {}).get('name', ''),
            'jobtitle': person.get('title', ''),
        }
    except Exception:
        return None


# --- Headline parser (fallback for company/title) ---

def parse_headline(headline):
    if not headline:
        return {'jobtitle': '', 'company': ''}
    m = re.match(r'^(.+?)\s+at\s+(.+)$', headline, re.I)
    if m:
        return {'jobtitle': m.group(1).strip(), 'company': m.group(2).strip()}
    m = re.match(r'^(.+?)\s*[|\-\u2013\u2014]\s*(.+)$', headline)
    if m:
        return {'jobtitle': m.group(1).strip(), 'company': m.group(2).strip()}
    return {'jobtitle': headline, 'company': ''}


# --- Main ---

def main():
    contacts = read_json(CONTACTS_FILE)
    if not contacts:
        print('No contacts found. Run filter_contacts.py first.')
        sys.exit(1)

    enriched = read_json(ENRICHED_FILE)
    unenriched = read_json(UNENRICHED_FILE)

    persistent_urls = load_processed_urls()
    processed = set(
        [c['linkedin_url'] for c in enriched] +
        [c['linkedin_url'] for c in unenriched] +
        list(persistent_urls)
    )

    remaining = [c for c in contacts if c['public_profile_url'] not in processed]
    batch = remaining[:int(MAX_CONTACTS)] if MAX_CONTACTS != float('inf') else remaining
    print(f'Total: {len(contacts)} | Already processed: {len(processed)} | Remaining: {len(remaining)} | Batch: {len(batch)}')

    cargo_count = 0
    fail_count = 0

    for i, contact in enumerate(batch):
        label = f'[{len(processed) + i + 1}/{len(contacts)}] {contact["first_name"]} {contact["last_name"]}'

        # 1. Try Cargo for email
        email = None
        try:
            cargo_result = try_cargo(contact)
            if cargo_result:
                email = cargo_result['email']
        except Exception as err:
            if str(err) == 'RATE_LIMITED':
                print(f'\nCARGO RATE LIMITED. Halting. Re-run to resume.')
                print(f'Progress: Enriched={cargo_count} Failed={fail_count}')
                sys.exit(1)
            print(f'{label} — Cargo error: {err}')

        if email and is_personal_email(email):
            fail_count += 1
            unenriched.append({
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'headline': contact['headline'],
                'linkedin_url': contact['public_profile_url'],
                'reason': 'personal_email',
                'email': email,
            })
            print(f'{label} — SKIPPED (personal email: {email})')
        elif email:
            # 2. Try Apollo for company/title metadata
            meta = get_apollo_meta(contact)
            parsed = parse_headline(contact['headline'])

            enriched.append({
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'email': email,
                'company': (meta or {}).get('company') or parsed['company'],
                'jobtitle': (meta or {}).get('jobtitle') or parsed['jobtitle'],
                'linkedin_url': contact['public_profile_url'],
                'enriched_by': 'cargo+apollo' if (meta or {}).get('company') else 'cargo',
            })
            cargo_count += 1
            print(f'{label} — {email} ({(meta or {}).get("company") or parsed["company"] or "no company"})')
        else:
            fail_count += 1
            unenriched.append({
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'headline': contact['headline'],
                'linkedin_url': contact['public_profile_url'],
                'reason': 'no_email_found',
            })
            print(f'{label} — UNENRICHED')

        # Save after each contact (crash-safe)
        write_json(ENRICHED_FILE, enriched)
        write_json(UNENRICHED_FILE, unenriched)
        mark_processed(contact['public_profile_url'])

        # 1.5s delay between contacts (Cargo rate limit)
        sleep_ms(1500)

    print(f'\nDone.')
    print(f'  Enriched: {cargo_count}')
    print(f'  Unenriched: {fail_count}')
    print(f'  Total enriched: {len(enriched)}')
    print(f'  Total unenriched: {len(unenriched)}')


if __name__ == '__main__':
    main()
