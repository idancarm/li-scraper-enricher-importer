#!/usr/bin/env python3
"""Filter scraped contacts by headline patterns and HubSpot dedup."""

import os
import re
import sys

import requests

from lib import init_env, read_json, sleep_ms, write_json

init_env()

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_API_TOKEN', '')

CONTACTS_FILE = 'data/contacts.json'
FILTERED_FILE = 'data/filtered.json'
EXCLUDED_FILE = 'data/excluded.json'

# --- Headline blocklist ---

HEADLINE_PATTERNS = [
    # Consultants
    (re.compile(r'consultant', re.I), 'consultant'),
    (re.compile(r'consulting', re.I), 'consulting'),
    (re.compile(r'advisory', re.I), 'advisory'),
    (re.compile(r'advisor', re.I), 'advisor'),
    (re.compile(r'adviser', re.I), 'adviser'),
    # Freelancers
    (re.compile(r'freelance', re.I), 'freelance'),
    (re.compile(r'freelancer', re.I), 'freelancer'),
    (re.compile(r'independent', re.I), 'independent'),
    # Agency
    (re.compile(r'agency', re.I), 'agency'),
    (re.compile(r'we help', re.I), 'we help'),
    (re.compile(r'we build', re.I), 'we build'),
    (re.compile(r'we engineer', re.I), 'we engineer'),
    (re.compile(r'we design', re.I), 'we design'),
    (re.compile(r'we create', re.I), 'we create'),
    (re.compile(r'we deliver', re.I), 'we deliver'),
    (re.compile(r'we scale', re.I), 'we scale'),
    (re.compile(r'helping\s+.*?\s*(companies|businesses|brands|startups|teams|agencies)', re.I), 'helping companies/businesses'),
    # Pitch-style / service seller language
    (re.compile(r'for hire', re.I), 'for hire'),
    (re.compile(r'available for', re.I), 'available for'),
    (re.compile(r'open to work', re.I), 'open to work'),
    (re.compile(r'seeking opportunities', re.I), 'seeking opportunities'),
    (re.compile(r'seeking for opportunities', re.I), 'seeking for opportunities'),
    (re.compile(r'open for projects', re.I), 'open for projects'),
    (re.compile(r'open for clients', re.I), 'open for clients'),
    (re.compile(r'\bstrategist\b', re.I), 'strategist'),
    (re.compile(r'grow(ing)? your', re.I), 'grow(ing) your'),
    (re.compile(r'scale your', re.I), 'scale your'),
    (re.compile(r'boost your', re.I), 'boost your'),
    (re.compile(r'your sales', re.I), 'your sales'),
    (re.compile(r'your business', re.I), 'your business'),
    (re.compile(r'your revenue', re.I), 'your revenue'),
    (re.compile(r'your growth', re.I), 'your growth'),
    (re.compile(r'your customers', re.I), 'your customers'),
    (re.compile(r'done for you', re.I), 'done for you'),
    (re.compile(r'done with you', re.I), 'done with you'),
    (re.compile(r'on autopilot', re.I), 'on autopilot'),
    (re.compile(r'book meetings', re.I), 'book meetings'),
    (re.compile(r'book demos', re.I), 'book demos'),
    (re.compile(r'lead gen\b', re.I), 'lead gen'),
    (re.compile(r'lead generation', re.I), 'lead generation'),
    (re.compile(r'your onboarding', re.I), 'your onboarding'),
    (re.compile(r'into revenue', re.I), 'into revenue'),
    (re.compile(r'into conversions', re.I), 'into conversions'),
    (re.compile(r"let's talk", re.I), "let's talk"),
    (re.compile(r'\bi help\b', re.I), 'i help'),
    (re.compile(r'\binbound\b', re.I), 'inbound'),
    (re.compile(r'scaling\s+.*?(companies|businesses|startups|saas)', re.I), 'scaling companies'),
    (re.compile(r'\bcoach\b', re.I), 'coach'),
    (re.compile(r'\bspeaker\b', re.I), 'speaker'),
    (re.compile(r'\bevangelist\b', re.I), 'evangelist'),
    (re.compile(r'\bentrepreneur\b', re.I), 'entrepreneur'),
    (re.compile(r'\bempowering\b', re.I), 'empowering'),
    (re.compile(r'managing partner', re.I), 'managing partner'),
    (re.compile(r'revenue architects?', re.I), 'revenue architect(s)'),
    (re.compile(r'for visionary', re.I), 'for visionary'),
    (re.compile(r'solutions partner', re.I), 'solutions partner'),
    (re.compile(r'partner für', re.I), 'partner für'),
    (re.compile(r'try\s+\S+\.(com|io|co|ai)', re.I), 'try [product URL]'),
    # HubSpot ecosystem
    (re.compile(r'hubspot consultant', re.I), 'hubspot consultant'),
    (re.compile(r'hubspot\s+.*?partner', re.I), 'hubspot partner'),
    (re.compile(r'hubspot coach', re.I), 'hubspot coach'),
    (re.compile(r'hubspot certified', re.I), 'hubspot certified'),
    (re.compile(r'hubspot expert', re.I), 'hubspot expert'),
    (re.compile(r'hubspot implementation', re.I), 'hubspot implementation'),
    (re.compile(r'hubspot app', re.I), 'hubspot app'),
    (re.compile(r'build(ing)? hubspot', re.I), 'build(ing) hubspot'),
    (re.compile(r'revops agency', re.I), 'revops agency'),
    (re.compile(r'revops fixer', re.I), 'revops fixer'),
    (re.compile(r'martech agency', re.I), 'martech agency'),
    (re.compile(r'crm consultant', re.I), 'crm consultant'),
    (re.compile(r'crm implementation', re.I), 'crm implementation'),
]


def check_headline(headline):
    if not headline:
        return None
    for pattern, keyword in HEADLINE_PATTERNS:
        if pattern.search(headline):
            return keyword
    return None


# --- HubSpot search ---

def search_hubspot(query):
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/contacts/search',
        json={'query': query, 'limit': 10, 'properties': ['firstname', 'lastname']},
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        },
        timeout=30,
    )
    if resp.status_code == 429:
        raise Exception('RATE_LIMITED')
    resp.raise_for_status()
    return resp.json()


def check_hubspot_exists(first_name, last_name):
    query = f'{first_name} {last_name}'
    data = search_hubspot(query)

    if not data.get('total'):
        return None

    for result in data.get('results', []):
        props = result.get('properties', {})
        hs_first = (props.get('firstname') or '').lower()
        hs_last = (props.get('lastname') or '').lower()
        if hs_first == first_name.lower() and hs_last == last_name.lower():
            return result['id']
    return None


# --- Main ---

def main():
    contacts = read_json(CONTACTS_FILE)
    if not contacts:
        print('No contacts found. Run scrape_contacts.py first.')
        sys.exit(1)

    filtered = read_json(FILTERED_FILE)
    excluded = read_json(EXCLUDED_FILE)

    processed_urls = set(
        [c['public_profile_url'] for c in filtered] +
        [c['public_profile_url'] for c in excluded]
    )

    remaining = [c for c in contacts if c['public_profile_url'] not in processed_urls]
    print(f'Total: {len(contacts)} | Already processed: {len(processed_urls)} | Remaining: {len(remaining)}')

    headline_excluded = 0
    hubspot_excluded = 0
    passed = 0

    for i, contact in enumerate(remaining):
        label = f'[{len(processed_urls) + i + 1}/{len(contacts)}] {contact["first_name"]} {contact["last_name"]}'

        # Filter A: Headline check
        matched_keyword = check_headline(contact['headline'])
        if matched_keyword:
            excluded.append({
                **contact,
                'reason': 'headline_filter',
                'matched_keyword': matched_keyword,
            })
            headline_excluded += 1
            print(f'{label} — EXCLUDED (headline: "{matched_keyword}")')
            write_json(EXCLUDED_FILE, excluded)
            continue

        # Filter B: HubSpot dedup
        try:
            hubspot_id = check_hubspot_exists(contact['first_name'], contact['last_name'])
            if hubspot_id:
                excluded.append({
                    **contact,
                    'reason': 'already_in_hubspot',
                    'hubspot_id': hubspot_id,
                })
                hubspot_excluded += 1
                print(f'{label} — EXCLUDED (already in HubSpot, ID: {hubspot_id})')
                write_json(EXCLUDED_FILE, excluded)
                sleep_ms(100)
                continue
        except Exception as err:
            if str(err) == 'RATE_LIMITED':
                print(f'\nHubSpot RATE LIMITED. Halting. Re-run to resume.')
                print(f'Progress: {headline_excluded} headline + {hubspot_excluded} hubspot excluded, {passed} passed')
                sys.exit(1)
            print(f'{label} — HubSpot lookup error: {err} (keeping contact)')

        # Passed both filters
        filtered.append(contact)
        passed += 1
        print(f'{label} — PASSED')

        write_json(FILTERED_FILE, filtered)
        sleep_ms(100)

    print(f'\nDone.')
    print(f'  Total:                {len(contacts)}')
    print(f'  Excluded (headline):  {headline_excluded}')
    print(f'  Excluded (HubSpot):   {hubspot_excluded}')
    print(f'  Ready for enrichment: {passed}')
    print(f'  Total filtered:       {len(filtered)}')
    print(f'  Total excluded:       {len(excluded)}')


if __name__ == '__main__':
    main()
