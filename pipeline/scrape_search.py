"""Scrape LinkedIn Sales Navigator search results via Unipile API."""

from __future__ import annotations

import random
import time

import requests

from .config import (
    DEFAULT_UNIPILE_DSN,
    SEARCH_DELAY_MIN_MS,
    SEARCH_DELAY_MAX_MS,
)
from .supabase_client import (
    insert_contacts,
    get_existing_linkedin_urls,
    update_pipeline_run,
)


def _fetch_search_page(actor: dict, search_url: str, cursor: str | None = None) -> dict:
    api_key = actor["unipile_api_key"]
    dsn = actor.get("unipile_dsn") or DEFAULT_UNIPILE_DSN
    account_id = actor["unipile_account_id"]

    params = {"account_id": account_id}
    if cursor:
        params["cursor"] = cursor

    url = f"https://{dsn}/api/v1/linkedin/search"
    resp = requests.post(
        url,
        params=params,
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"url": search_url},
        timeout=30,
    )

    if resp.status_code in (429, 403):
        raise Exception(f"Rate limited or forbidden: {resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def _extract_search_contact(item: dict) -> dict:
    contact = {
        "first_name": item.get("first_name", ""),
        "last_name": item.get("last_name", ""),
        "headline": item.get("headline", ""),
        "public_profile_url": item.get("public_profile_url", ""),
        "public_identifier": item.get("public_identifier", ""),
    }

    positions = item.get("current_positions") or []
    if positions:
        pos = positions[0]
        contact["company"] = pos.get("company", "")
        contact["jobtitle"] = pos.get("title", "")

    return contact


def scrape_search_results(
    actor: dict, run_id: str, search_url: str, max_pages: int | None = None
) -> dict:
    """Scrape Sales Navigator search results and insert into Supabase.

    Returns dict with stats: total_scraped, new_contacts, skipped_existing.
    """
    actor_id = actor["id"]
    existing_urls = get_existing_linkedin_urls(actor_id)
    print(f"  Found {len(existing_urls)} existing contacts for this actor")

    cursor = None
    page = 0
    total_scraped = 0
    new_contacts = 0
    skipped = 0
    page_limit = max_pages or float("inf")

    while True:
        page += 1
        print(f"  Fetching search page {page}... (cursor: {cursor or 'start'})")

        try:
            data = _fetch_search_page(actor, search_url, cursor)
        except Exception as err:
            print(f"  HALTING: {err}")
            update_pipeline_run(run_id, scrape_cursor=cursor or "")
            break

        items = data.get("items") or data.get("data") or []
        if not items:
            print("  No more items returned.")
            break

        batch = []
        for item in items:
            contact = _extract_search_contact(item)
            total_scraped += 1
            if contact["public_profile_url"] in existing_urls:
                skipped += 1
                continue
            existing_urls.add(contact["public_profile_url"])
            batch.append(contact)

        if batch:
            insert_contacts(batch, run_id, actor_id)
            new_contacts += len(batch)

        cursor = data.get("cursor")
        update_pipeline_run(run_id, scrape_cursor=cursor or "", total_scraped=total_scraped)

        paging = data.get("paging") or {}
        total_available = paging.get("total_count", "?")
        print(f"    Got {len(items)} contacts ({len(batch)} new, {len(items) - len(batch)} existing) — total available: {total_available}")

        if not cursor:
            print("  No more pages (cursor is null).")
            break

        if page >= page_limit:
            print(f"  Reached page limit ({max_pages}). Re-run to continue.")
            break

        time.sleep(random.uniform(SEARCH_DELAY_MIN_MS, SEARCH_DELAY_MAX_MS) / 1000)

    stats = {
        "total_scraped": total_scraped,
        "new_contacts": new_contacts,
        "skipped_existing": skipped,
    }
    print(f"  Search scrape done: {total_scraped} total, {new_contacts} new, {skipped} existing")
    return stats
