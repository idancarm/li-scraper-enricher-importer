"""Enrich filtered contacts with email (Cargo) and metadata (Apollo)."""

from __future__ import annotations

import re
import time

import requests

from .config import (
    CARGO_TOOL_URL,
    APOLLO_MATCH_URL,
    ENRICH_DELAY_MS,
    PERSONAL_EMAIL_DOMAINS,
)
from .supabase_client import get_contacts_by_status, update_contact_enrichment


def _is_personal_email(email: str) -> bool:
    domain = email.split("@")[-1].lower()
    return domain in PERSONAL_EMAIL_DOMAINS


def _try_cargo(linkedin_url: str, cargo_api_key: str) -> str | None:
    """Try Cargo enrichment. Returns email or None."""
    if not cargo_api_key:
        return None

    resp = requests.post(
        CARGO_TOOL_URL,
        json={"linkedinUrl": linkedin_url},
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cargo_api_key}",
        },
        timeout=60,
    )
    if resp.status_code == 429:
        raise Exception("RATE_LIMITED")
    resp.raise_for_status()
    data = resp.json()

    email = (
        data.get("email")
        or (data.get("data") or {}).get("email")
        or (data.get("result") or {}).get("email")
        or ((data.get("result") or {}).get("output") or {}).get("email")
        or (data.get("output") if isinstance(data.get("output"), dict) else {}).get("email")
    )

    if not email and isinstance(data.get("output"), str) and "@" in data["output"]:
        email = data["output"].strip()

    if not email or "@" not in email:
        return None

    return email


def _get_apollo_meta(contact: dict, apollo_api_key: str) -> dict | None:
    """Get company + title from Apollo. Returns dict or None."""
    if not apollo_api_key:
        return None

    try:
        resp = requests.post(
            APOLLO_MATCH_URL,
            json={
                "linkedin_url": contact["linkedin_url"],
                "first_name": contact["first_name"],
                "last_name": contact["last_name"],
            },
            headers={
                "Content-Type": "application/json",
                "x-api-key": apollo_api_key,
                "Cache-Control": "no-cache",
            },
            timeout=60,
        )
        resp.raise_for_status()
        person = resp.json().get("person")
        if not person:
            return None
        return {
            "company": (person.get("organization") or {}).get("name", ""),
            "jobtitle": person.get("title", ""),
        }
    except Exception:
        return None


def _parse_headline(headline: str) -> dict:
    """Fallback parser for company/title from headline."""
    if not headline:
        return {"jobtitle": "", "company": ""}
    m = re.match(r"^(.+?)\s+at\s+(.+)$", headline, re.I)
    if m:
        return {"jobtitle": m.group(1).strip(), "company": m.group(2).strip()}
    m = re.match(r"^(.+?)\s*[|\-\u2013\u2014]\s*(.+)$", headline)
    if m:
        return {"jobtitle": m.group(1).strip(), "company": m.group(2).strip()}
    return {"jobtitle": headline, "company": ""}


def enrich_contacts(actor: dict, run_id: str, max_contacts: int | None = None) -> dict:
    """Enrich filtered contacts with email and metadata.

    Reads contacts with status='filtered', updates to 'enriched' or 'unenriched'.
    Returns dict with stats.
    """
    actor_id = actor["id"]
    cargo_api_key = actor.get("cargo_api_key", "")
    apollo_api_key = actor.get("apollo_api_key", "")

    contacts = get_contacts_by_status(actor_id, "filtered", pipeline_run_id=run_id)
    if not contacts:
        print("  No filtered contacts to enrich.")
        return {"total": 0, "enriched": 0, "unenriched": 0}

    batch = contacts[:max_contacts] if max_contacts else contacts
    print(f"  Enriching {len(batch)} contacts (of {len(contacts)} filtered)...")

    enriched_count = 0
    unenriched_count = 0

    for i, contact in enumerate(batch):
        label = f"  [{i + 1}/{len(batch)}] {contact['first_name']} {contact['last_name']}"

        # 1. Try Cargo for email
        email = None
        try:
            email = _try_cargo(contact["linkedin_url"], cargo_api_key)
        except Exception as err:
            if str(err) == "RATE_LIMITED":
                print(f"\n  CARGO RATE LIMITED. Halting enrichment.")
                break
            print(f"{label} — Cargo error: {err}")

        if email and _is_personal_email(email):
            update_contact_enrichment(
                contact["id"],
                status="unenriched",
                exclusion_reason="personal_email",
                email=email,
            )
            unenriched_count += 1
            print(f"{label} — SKIPPED (personal email: {email})")
        elif email:
            # 2. Try Apollo for company/title metadata
            meta = _get_apollo_meta(contact, apollo_api_key)
            parsed = _parse_headline(contact.get("headline", ""))

            company = (meta or {}).get("company") or parsed["company"]
            jobtitle = (meta or {}).get("jobtitle") or parsed["jobtitle"]
            enriched_by = "cargo+apollo" if (meta or {}).get("company") else "cargo"

            update_contact_enrichment(
                contact["id"],
                status="enriched",
                email=email,
                company=company,
                jobtitle=jobtitle,
                enriched_by=enriched_by,
            )
            enriched_count += 1
            print(f"{label} — {email} ({company or 'no company'})")
        else:
            update_contact_enrichment(
                contact["id"],
                status="unenriched",
                exclusion_reason="no_email_found",
            )
            unenriched_count += 1
            print(f"{label} — UNENRICHED")

        time.sleep(ENRICH_DELAY_MS / 1000)

    stats = {
        "total": len(batch),
        "enriched": enriched_count,
        "unenriched": unenriched_count,
    }
    print(f"  Enrich done: {enriched_count} enriched, {unenriched_count} unenriched")
    return stats
