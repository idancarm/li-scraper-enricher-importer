# LinkedIn Scraper Pipeline — User Guide

A Python pipeline that scrapes your LinkedIn connections via Unipile, filters out noise, enriches contacts with email + company data, and imports them into HubSpot CRM. State is managed via Supabase (shared instance with LI Engagement Research).

## Architecture

```
pipeline/
├── config.py              # Centralized constants + API URLs
├── supabase_client.py     # All DB operations
├── scrape.py              # Unipile connections scraping
├── scrape_search.py       # Sales Navigator search scraping
├── headline_patterns.py   # Headline blocklist patterns
├── filter.py              # Headline + HubSpot dedup
├── enrich.py              # Cargo + Apollo enrichment
├── hubspot.py             # Batch import
├── csv_utils.py           # CSV export helper
├── icp_review.py          # Claude AI lead qualification
└── run_pipeline.py        # Single orchestrator entry point
```

### Data Flow

```
scrape → filter → [icp_review] → enrich → import
```

All state is tracked in Supabase via the `scraper_contacts` table with a `status` column:

```
scraped → filtered/excluded → enriched/unenriched → imported
```

### Key Tables

| Table | Purpose |
|-------|---------|
| `scraper_actors` | One row per LinkedIn account (non-secret config) |
| `scraper_pipeline_runs` | Tracks each pipeline execution with stats |
| `scraper_contacts` | All contacts with status-based progression |

## Setup

### 1. Install dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Create Supabase tables

Run `supabase_schema.sql` in the Supabase SQL editor. This creates the tables and inserts an initial actor row.

### 3. Configure environment

Copy `.env.example` to `.env` and fill in all API keys:

```bash
cp .env.example .env
# Edit .env and add all credentials
```

All secrets (Unipile, Cargo, Apollo, HubSpot, Anthropic) are loaded from `.env`. Supabase only stores non-secret actor config (name, active flag, ICP config URL).

### 4. Verify connectivity

```bash
python -c "from pipeline.supabase_client import get_active_actors; print(get_active_actors())"
```

## Usage

All steps are run through the single orchestrator:

```bash
python -m pipeline.run_pipeline --actor-id <UUID> [options]
```

### Options

| Flag | Description |
|------|-------------|
| `--actor-id UUID` | Target a specific actor (default: all active actors) |
| `--search-url URL` | Scrape a Sales Navigator search URL instead of connections |
| `--max-pages N` | Limit scraping to N pages |
| `--max-enrich N` | Limit enrichment to N contacts |
| `--skip-scrape` | Skip the scrape step |
| `--skip-filter` | Skip the filter step |
| `--skip-enrich` | Skip the enrich step |
| `--skip-import` | Skip the HubSpot import step |
| `--icp-review` | Run AI ICP review before enrichment |
| `--export-csv` | Export results to CSV in `output/` |
| `--review` | Review mode: scrape → filter → ICP review → export CSV (no enrich/import) |

### Examples

```bash
# Full pipeline, 1 page, max 3 enrichments
python -m pipeline.run_pipeline --actor-id <UUID> --max-pages 1 --max-enrich 3

# Just scrape
python -m pipeline.run_pipeline --actor-id <UUID> --skip-filter --skip-enrich --skip-import --max-pages 1

# Just filter (after scraping)
python -m pipeline.run_pipeline --actor-id <UUID> --skip-scrape --skip-enrich --skip-import

# Enrich with ICP review
python -m pipeline.run_pipeline --actor-id <UUID> --skip-scrape --skip-filter --icp-review --max-enrich 10

# Import only
python -m pipeline.run_pipeline --actor-id <UUID> --skip-scrape --skip-filter --skip-enrich

# Scrape Sales Navigator search results (1 page)
python -m pipeline.run_pipeline --actor-id <UUID> --search-url "https://www.linkedin.com/sales/search/people?query=..." --max-pages 1

# Full pipeline from Sales Navigator search
python -m pipeline.run_pipeline --actor-id <UUID> --search-url "https://www.linkedin.com/sales/search/people?query=..."

# Review mode: scrape + filter + ICP review + CSV export (no enrich/import)
python -m pipeline.run_pipeline --actor-id <UUID> --search-url "https://www.linkedin.com/sales/search/people?query=..." --review

# Review mode with page limit
python -m pipeline.run_pipeline --actor-id <UUID> --search-url "https://www.linkedin.com/sales/search/people?query=..." --review --max-pages 3
```

## Pipeline Steps

### Step 1: Scrape
**Connections mode** (default): Pulls LinkedIn connections from Unipile API (`GET /api/v1/users/relations`).

**Search mode** (`--search-url`): Scrapes Sales Navigator search results via Unipile API (`POST /api/v1/linkedin/search`). Pass any Sales Navigator search URL, saved search, or lead list URL. The account must have an active Sales Navigator subscription. Search results include company and job title from `current_positions`, which are stored directly and can reduce the need for Apollo enrichment. LinkedIn caps Sales Navigator search results at 2,500 per query.

Both modes deduplicate against existing contacts in Supabase per actor. Contacts are inserted with `status=scraped`.

### Step 2: Filter
**Headline blocklist**: ~80 regex patterns filtering out consultants, freelancers, agencies, service sellers, coaches, HubSpot ecosystem partners, etc.

**HubSpot dedup**: Searches existing HubSpot contacts by name to avoid duplicates.

Contacts are updated to `status=filtered` (passed) or `status=excluded` with a reason.

### Step 2b: ICP Review (optional)
Uses Claude AI to evaluate filtered contacts against an ICP configuration. Contacts that don't match are excluded. Requires `ANTHROPIC_API_KEY` in `.env` and `icp_config_url` on the actor.

### Step 3: Enrich
**Cargo** (primary): Finds business email by LinkedIn URL.
**Apollo** (supplement): Adds company name and job title.
**Headline parser** (fallback): Parses "Title at Company" patterns.

Personal emails are automatically excluded. Contacts are updated to `status=enriched` or `status=unenriched`.

### Step 4: Import
Batch-creates contacts in HubSpot with `do_not_email=true`. Handles rate limiting with exponential backoff. Updates contacts to `status=imported`.

## Multi-Actor Support

Each actor in `scraper_actors` represents a LinkedIn account. API keys are shared across all actors via `.env`. The pipeline can process all active actors or target a specific one with `--actor-id`. Contacts are isolated per actor via the `actor_id` foreign key.

## Resume Support

All state is in Supabase. If the pipeline fails mid-run, re-running with the same flags will pick up where it left off — contacts already processed won't be re-processed since their status has already advanced.

## Tests

```bash
python -m pytest tests/
```
