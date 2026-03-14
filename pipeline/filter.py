"""Filter contacts by headline patterns and HubSpot dedup."""

from __future__ import annotations

import time

import requests

from .config import HUBSPOT_SEARCH_URL, HUBSPOT_SEARCH_DELAY_MS
from .headline_patterns import check_headline
from .supabase_client import get_contacts_by_status, update_contact_status


def _search_hubspot(query: str, hubspot_token: str) -> dict:
    resp = requests.post(
        HUBSPOT_SEARCH_URL,
        json={"query": query, "limit": 10, "properties": ["firstname", "lastname"]},
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {hubspot_token}",
        },
        timeout=30,
    )
    if resp.status_code == 429:
        raise Exception("RATE_LIMITED")
    resp.raise_for_status()
    return resp.json()


def _check_hubspot_exists(first_name: str, last_name: str, hubspot_token: str) -> str | None:
    query = f"{first_name} {last_name}"
    data = _search_hubspot(query, hubspot_token)

    if not data.get("total"):
        return None

    for result in data.get("results", []):
        props = result.get("properties", {})
        hs_first = (props.get("firstname") or "").lower()
        hs_last = (props.get("lastname") or "").lower()
        if hs_first == first_name.lower() and hs_last == last_name.lower():
            return result["id"]
    return None


def filter_contacts(actor: dict, run_id: str) -> dict:
    """Filter scraped contacts: headline blocklist + HubSpot dedup.

    Reads contacts with status='scraped', updates to 'filtered' or 'excluded'.
    Returns dict with stats.
    """
    actor_id = actor["id"]
    hubspot_token = actor.get("hubspot_token", "")

    contacts = get_contacts_by_status(actor_id, "scraped", pipeline_run_id=run_id)
    if not contacts:
        print("  No scraped contacts to filter.")
        return {"total": 0, "headline_excluded": 0, "hubspot_excluded": 0, "passed": 0}

    print(f"  Filtering {len(contacts)} scraped contacts...")

    headline_excluded = 0
    hubspot_excluded = 0
    passed = 0

    for i, contact in enumerate(contacts):
        label = f"  [{i + 1}/{len(contacts)}] {contact['first_name']} {contact['last_name']}"

        # Filter A: Headline check
        matched_keyword = check_headline(contact["headline"])
        if matched_keyword:
            update_contact_status(
                contact["id"], "excluded",
                exclusion_reason="headline_filter",
                matched_keyword=matched_keyword,
            )
            headline_excluded += 1
            print(f'{label} — EXCLUDED (headline: "{matched_keyword}")')
            continue

        # Filter B: HubSpot dedup
        if hubspot_token:
            try:
                hubspot_id = _check_hubspot_exists(
                    contact["first_name"], contact["last_name"], hubspot_token
                )
                if hubspot_id:
                    update_contact_status(
                        contact["id"], "excluded",
                        exclusion_reason="already_in_hubspot",
                        hubspot_id=hubspot_id,
                    )
                    hubspot_excluded += 1
                    print(f"{label} — EXCLUDED (already in HubSpot, ID: {hubspot_id})")
                    time.sleep(HUBSPOT_SEARCH_DELAY_MS / 1000)
                    continue
            except Exception as err:
                if str(err) == "RATE_LIMITED":
                    print(f"\n  HubSpot RATE LIMITED. Halting filter.")
                    break
                print(f"{label} — HubSpot lookup error: {err} (keeping contact)")

            time.sleep(HUBSPOT_SEARCH_DELAY_MS / 1000)

        # Passed both filters
        update_contact_status(contact["id"], "filtered")
        passed += 1
        print(f"{label} — PASSED")

    stats = {
        "total": len(contacts),
        "headline_excluded": headline_excluded,
        "hubspot_excluded": hubspot_excluded,
        "passed": passed,
    }
    print(f"  Filter done: {headline_excluded} headline, {hubspot_excluded} hubspot excluded, {passed} passed")
    return stats
