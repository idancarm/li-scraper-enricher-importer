-- Supabase schema for li-scraper pipeline
-- Run this in the Supabase SQL editor (same instance as LI Engagement Research)

-- Actors table (one row per LinkedIn account)
-- API keys/secrets are stored in .env, not in this table.
CREATE TABLE IF NOT EXISTS scraper_actors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    active BOOLEAN DEFAULT true,
    icp_config_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Pipeline runs table
CREATE TABLE IF NOT EXISTS scraper_pipeline_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID REFERENCES scraper_actors(id),
    status TEXT DEFAULT 'running',
    scrape_cursor TEXT,
    total_scraped INT DEFAULT 0,
    headline_excluded INT DEFAULT 0,
    hubspot_excluded INT DEFAULT 0,
    passed_filter INT DEFAULT 0,
    enriched_count INT DEFAULT 0,
    unenriched_count INT DEFAULT 0,
    imported_count INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ
);

-- Contacts table (single table replaces all JSON files)
CREATE TABLE IF NOT EXISTS scraper_contacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_run_id UUID REFERENCES scraper_pipeline_runs(id),
    actor_id UUID REFERENCES scraper_actors(id),
    first_name TEXT,
    last_name TEXT,
    headline TEXT,
    linkedin_url TEXT,
    public_identifier TEXT,
    status TEXT DEFAULT 'scraped',
    exclusion_reason TEXT,
    matched_keyword TEXT,
    hubspot_id TEXT,
    email TEXT,
    company TEXT,
    jobtitle TEXT,
    enriched_by TEXT,
    icp_recommendation TEXT,
    icp_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Unique constraint: one linkedin_url per actor
CREATE UNIQUE INDEX IF NOT EXISTS scraper_contacts_actor_linkedin
    ON scraper_contacts (actor_id, linkedin_url);

-- Index for status-based queries
CREATE INDEX IF NOT EXISTS scraper_contacts_status_idx
    ON scraper_contacts (actor_id, status);

-- Insert initial actor row
INSERT INTO scraper_actors (name) VALUES ('Idan');

-- Migration: drop secret columns from existing scraper_actors table
-- Run this if you already have the old schema with secrets in the table:
--
-- ALTER TABLE scraper_actors
--   DROP COLUMN IF EXISTS unipile_api_key,
--   DROP COLUMN IF EXISTS unipile_dsn,
--   DROP COLUMN IF EXISTS unipile_account_id,
--   DROP COLUMN IF EXISTS cargo_api_key,
--   DROP COLUMN IF EXISTS apollo_api_key,
--   DROP COLUMN IF EXISTS hubspot_token,
--   DROP COLUMN IF EXISTS anthropic_api_key;
