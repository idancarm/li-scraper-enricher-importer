"""Batch import enriched contacts into HubSpot CRM."""

from __future__ import annotations

import math
import time

import requests

from .config import HUBSPOT_BATCH_CREATE_URL, HUBSPOT_BATCH_SIZE, HUBSPOT_BATCH_DELAY_MS
from .supabase_client import get_contacts_by_status, batch_update_contacts


def _batch_create(contacts: list[dict], hubspot_token: str) -> dict:
    inputs = [
        {
            "properties": {
                "email": c["email"],
                "firstname": c["first_name"],
                "lastname": c["last_name"],
                "jobtitle": c.get("jobtitle", ""),
                "company": c.get("company", ""),
                "hs_linkedin_url": c.get("linkedin_url", ""),
                "do_not_email": "true",
            }
        }
        for c in contacts
    ]

    resp = requests.post(
        HUBSPOT_BATCH_CREATE_URL,
        json={"inputs": inputs},
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {hubspot_token}",
        },
        timeout=30,
    )
    if resp.status_code == 429:
        raise Exception("RATE_LIMITED")
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return {"status": resp.status_code, "body": body}


def import_contacts(actor: dict, run_id: str) -> dict:
    """Import enriched contacts into HubSpot.

    Reads contacts with status='enriched', updates to 'imported'.
    Returns dict with stats.
    """
    hubspot_token = actor.get("hubspot_token", "")
    if not hubspot_token:
        print("  No hubspot_token on actor. Skipping import.")
        return {"created": 0, "duplicates": 0, "errors": 0}

    contacts = get_contacts_by_status(actor["id"], "enriched", pipeline_run_id=run_id)
    if not contacts:
        print("  No enriched contacts to import.")
        return {"created": 0, "duplicates": 0, "errors": 0}

    total_batches = math.ceil(len(contacts) / HUBSPOT_BATCH_SIZE)
    print(f"  Importing {len(contacts)} contacts in {total_batches} batches of {HUBSPOT_BATCH_SIZE}")

    created = 0
    duplicates = 0
    errors = 0

    for batch_num in range(total_batches):
        start = batch_num * HUBSPOT_BATCH_SIZE
        end = min(start + HUBSPOT_BATCH_SIZE, len(contacts))
        batch = contacts[start:end]

        print(f"  Batch {batch_num + 1}/{total_batches} (contacts {start + 1}-{end})...")

        retries = 0
        while True:
            try:
                res = _batch_create(batch, hubspot_token)

                if res["status"] == 201:
                    created += len(batch)
                    batch_update_contacts([c["id"] for c in batch], status="imported")
                    print(f"    Created {len(batch)} contacts")
                elif res["status"] in (409, 207):
                    results = res["body"].get("results", []) if isinstance(res["body"], dict) else []
                    errs = res["body"].get("errors", []) if isinstance(res["body"], dict) else []
                    created += len(results)
                    duplicates += len(errs)
                    # Mark all as imported (even partial)
                    batch_update_contacts([c["id"] for c in batch], status="imported")
                    print(f"    Partial: {len(results)} created, {len(errs)} duplicates/errors")
                else:
                    body_str = str(res["body"])[:200]
                    print(f"    Unexpected status {res['status']}: {body_str}")
                    errors += len(batch)
                break
            except Exception as err:
                if str(err) == "RATE_LIMITED":
                    retries += 1
                    backoff = min(1000 * (2 ** retries), 60000)
                    print(f"    Rate limited. Backing off {backoff / 1000}s...")
                    time.sleep(backoff / 1000)
                    continue
                print(f"    Error: {err}")
                errors += len(batch)
                break

        time.sleep(HUBSPOT_BATCH_DELAY_MS / 1000)

    stats = {"created": created, "duplicates": duplicates, "errors": errors}
    print(f"  Import done: {created} created, {duplicates} duplicates, {errors} errors")
    return stats
