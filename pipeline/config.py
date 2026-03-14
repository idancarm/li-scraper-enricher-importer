"""Centralized constants and configuration."""

from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"

# Supabase
SUPABASE_URL = "https://sqdguogtadmplzoukmls.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Unipile defaults
DEFAULT_UNIPILE_DSN = "api17.unipile.com:14742"

# Cargo enrichment endpoint
CARGO_TOOL_URL = "https://api.getcargo.io/v1/tools/017fd330-fc34-42a5-b608-25897c94ba28/execute"

# Apollo
APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"

# HubSpot
HUBSPOT_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/contacts/search"
HUBSPOT_BATCH_CREATE_URL = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"
HUBSPOT_BATCH_SIZE = 10
HUBSPOT_BATCH_DELAY_MS = 1000

# Rate limit delays (ms)
SCRAPE_DELAY_MIN_MS = 2000
SCRAPE_DELAY_MAX_MS = 5000
SEARCH_DELAY_MIN_MS = 3000
SEARCH_DELAY_MAX_MS = 7000
ENRICH_DELAY_MS = 1500
HUBSPOT_SEARCH_DELAY_MS = 100

# Claude API for ICP review
ICP_REVIEW_MODEL = "claude-haiku-4-5-20251001"
ICP_REVIEW_BATCH_SIZE = 20
ICP_CONFIGS_DIR = BASE_DIR / "icp_configs"

# Personal email domains to exclude
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "me.com", "mail.com",
    "yahoo.co.uk", "hotmail.co.uk", "live.com", "msn.com",
}


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
