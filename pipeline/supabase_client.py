"""Supabase client and DB operations for the scraper pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from supabase import create_client, Client

# Load .env from project root if present
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

SUPABASE_URL = "https://sqdguogtadmplzoukmls.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        if not SUPABASE_KEY:
            raise RuntimeError("SUPABASE_SERVICE_KEY env var not set")
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


# --- Actors ---

def get_active_actors() -> list[dict]:
    resp = get_client().table("scraper_actors").select("*").eq("active", True).execute()
    return resp.data


def get_actor_by_id(actor_id: str) -> dict | None:
    resp = get_client().table("scraper_actors").select("*").eq("id", actor_id).execute()
    return resp.data[0] if resp.data else None


# --- Pipeline runs ---

def create_pipeline_run(actor_id: str) -> dict:
    resp = (
        get_client()
        .table("scraper_pipeline_runs")
        .insert({"actor_id": actor_id, "status": "running"})
        .execute()
    )
    return resp.data[0]


def update_pipeline_run(run_id: str, **fields) -> dict:
    resp = (
        get_client()
        .table("scraper_pipeline_runs")
        .update(fields)
        .eq("id", run_id)
        .execute()
    )
    return resp.data[0] if resp.data else {}


# --- Contacts ---

def insert_contacts(contacts: list[dict], pipeline_run_id: str, actor_id: str) -> list[dict]:
    """Insert contact rows with status='scraped'. Returns inserted rows."""
    if not contacts:
        return []
    rows = []
    for c in contacts:
        row = {
            "pipeline_run_id": pipeline_run_id,
            "actor_id": actor_id,
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "headline": c.get("headline", ""),
            "linkedin_url": c.get("public_profile_url", "") or c.get("linkedin_url", ""),
            "public_identifier": c.get("public_identifier", ""),
            "status": "scraped",
        }
        if c.get("company"):
            row["company"] = c["company"]
        if c.get("jobtitle"):
            row["jobtitle"] = c["jobtitle"]
        rows.append(row)
    resp = get_client().table("scraper_contacts").insert(rows).execute()
    return resp.data


def get_contacts_by_status(actor_id: str, status: str, pipeline_run_id: str | None = None) -> list[dict]:
    """Fetch contacts filtered by actor and status. Optionally filter by run."""
    q = get_client().table("scraper_contacts").select("*").eq("actor_id", actor_id).eq("status", status)
    if pipeline_run_id:
        q = q.eq("pipeline_run_id", pipeline_run_id)
    resp = q.execute()
    return resp.data


def update_contact_status(contact_id: str, status: str, **extra_fields) -> dict:
    """Update a single contact's status and any extra fields."""
    fields = {"status": status, **extra_fields}
    resp = (
        get_client()
        .table("scraper_contacts")
        .update(fields)
        .eq("id", contact_id)
        .execute()
    )
    return resp.data[0] if resp.data else {}


def update_contact_enrichment(contact_id: str, **fields) -> dict:
    """Update enrichment fields on a contact (email, company, jobtitle, enriched_by, status)."""
    resp = (
        get_client()
        .table("scraper_contacts")
        .update(fields)
        .eq("id", contact_id)
        .execute()
    )
    return resp.data[0] if resp.data else {}


def get_existing_linkedin_urls(actor_id: str) -> set[str]:
    """Return all linkedin_urls already stored for this actor (any status)."""
    resp = (
        get_client()
        .table("scraper_contacts")
        .select("linkedin_url")
        .eq("actor_id", actor_id)
        .execute()
    )
    return {row["linkedin_url"] for row in resp.data if row.get("linkedin_url")}


def batch_update_contacts(contact_ids: list[str], **fields) -> None:
    """Update multiple contacts with the same fields."""
    client = get_client()
    for cid in contact_ids:
        client.table("scraper_contacts").update(fields).eq("id", cid).execute()
