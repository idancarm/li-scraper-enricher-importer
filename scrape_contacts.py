#!/usr/bin/env python3
"""Scrape LinkedIn contacts via Unipile API."""

import json
import os
import sys
from pathlib import Path

import requests

from lib import init_env, random_delay, write_json

init_env()

API_KEY = os.environ.get('UNIPILE_API_KEY', '')
DSN = os.environ.get('UNIPILE_DSN', '')
ACCOUNT_ID = os.environ.get('UNIPILE_ACCOUNT_ID', '')
DATA_FILE = 'data/contacts.json'
CURSOR_FILE = 'data/.scrape-cursor'
MAX_PAGES = int(sys.argv[1]) if len(sys.argv) > 1 else float('inf')


def fetch_page(cursor=None):
    params = {'account_id': ACCOUNT_ID, 'limit': '100'}
    if cursor:
        params['cursor'] = cursor

    url = f'https://{DSN}/api/v1/users/relations'
    resp = requests.get(url, params=params, headers={'X-API-KEY': API_KEY}, timeout=30)

    if resp.status_code in (429, 403):
        raise Exception(f'Rate limited or forbidden: {resp.status_code}')
    resp.raise_for_status()
    return resp.json()


def extract_contact(item):
    return {
        'first_name': item.get('first_name', ''),
        'last_name': item.get('last_name', ''),
        'headline': item.get('headline', ''),
        'public_profile_url': item.get('public_profile_url', ''),
        'public_identifier': item.get('public_identifier', ''),
    }


def main():
    contacts = []
    data_path = Path(DATA_FILE)
    if data_path.exists():
        contacts = json.loads(data_path.read_text())
        print(f'Resuming — {len(contacts)} contacts already saved')

    cursor = None
    cursor_path = Path(CURSOR_FILE)
    if cursor_path.exists():
        cursor = cursor_path.read_text().strip() or None
        if cursor:
            print(f'Resuming from cursor: {cursor}')

    page = 0
    while True:
        page += 1
        print(f'Fetching page {page}... (cursor: {cursor or "start"})')

        try:
            data = fetch_page(cursor)
        except Exception as err:
            print(f'HALTING: {err}')
            print(f'Saved {len(contacts)} contacts so far. Re-run to resume.')
            break

        items = data.get('items') or data.get('data') or []
        if not items:
            print('No more items returned.')
            break

        for item in items:
            contacts.append(extract_contact(item))

        # Save after each page (crash-safe)
        write_json(DATA_FILE, contacts)

        cursor = data.get('cursor')
        cursor_path.write_text(cursor or '')

        print(f'  Got {len(items)} contacts (total: {len(contacts)})')

        if not cursor:
            print('No more pages (cursor is null).')
            break

        if page >= MAX_PAGES:
            print(f'Reached page limit ({MAX_PAGES}). Re-run to continue.')
            break

        random_delay(2000, 5000)

    print(f'\nDone. Total contacts saved: {len(contacts)}')


if __name__ == '__main__':
    main()
