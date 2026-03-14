"""Single orchestrator entry point for the LinkedIn scraper pipeline.

Usage:
    python -m pipeline.run_pipeline --actor-id <UUID> [options]

Options:
    --max-pages N       Limit scraping to N pages
    --max-enrich N      Limit enrichment to N contacts
    --skip-scrape       Skip the scrape step
    --skip-filter       Skip the filter step
    --skip-enrich       Skip the enrich step
    --skip-import       Skip the HubSpot import step
    --icp-review        Run AI ICP review before enrichment
    --export-csv        Export enriched contacts to CSV
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import ensure_output_dir, OUTPUT_DIR, ICP_CONFIGS_DIR
from .supabase_client import (
    get_active_actors,
    get_actor_by_id,
    create_pipeline_run,
    update_pipeline_run,
)
from .scrape import scrape_contacts
from .scrape_search import scrape_search_results
from .filter import filter_contacts
from .enrich import enrich_contacts
from .hubspot import import_contacts
from .csv_utils import write_csv
from .icp_review import review_contacts


def run(actor: dict, args: argparse.Namespace):
    name = actor["name"]
    actor_id = actor["id"]
    print(f"\n{'=' * 60}")
    print(f"Pipeline: {name}")
    print(f"{'=' * 60}")

    run = create_pipeline_run(actor_id)
    run_id = run["id"]
    print(f"Run ID: {run_id}")

    try:
        # Step 1: Scrape
        if not args.skip_scrape:
            if args.search_url:
                print("\nStep 1: Scraping Sales Navigator search results...")
                scrape_stats = scrape_search_results(
                    actor, run_id, args.search_url, max_pages=args.max_pages
                )
            else:
                print("\nStep 1: Scraping contacts...")
                scrape_stats = scrape_contacts(actor, run_id, max_pages=args.max_pages)
            update_pipeline_run(run_id, total_scraped=scrape_stats["total_scraped"])
        else:
            print("\nStep 1: Scrape — SKIPPED")

        # Step 2: Filter
        if not args.skip_filter:
            print("\nStep 2: Filtering contacts...")
            filter_stats = filter_contacts(actor, run_id)
            update_pipeline_run(
                run_id,
                headline_excluded=filter_stats["headline_excluded"],
                hubspot_excluded=filter_stats["hubspot_excluded"],
                passed_filter=filter_stats["passed"],
            )
        else:
            print("\nStep 2: Filter — SKIPPED")

        # Step 2b: ICP review (optional)
        if args.icp_review:
            print("\nStep 2b: AI ICP review...")
            icp_config_url = actor.get("icp_config_url", "")
            if icp_config_url:
                icp_path = Path(icp_config_url)
                if not icp_path.is_absolute():
                    icp_path = ICP_CONFIGS_DIR / icp_config_url
                if icp_path.exists():
                    icp_config = icp_path.read_text()
                    try:
                        review_contacts(actor, run_id, icp_config)
                    except Exception as e:
                        print(f"  ICP review failed (non-fatal): {e}")
                else:
                    print(f"  Warning: ICP config not found at {icp_path}")
            else:
                print("  No icp_config_url on actor. Skipping ICP review.")

        # Step 3: Enrich
        if not args.skip_enrich:
            print("\nStep 3: Enriching contacts...")
            enrich_stats = enrich_contacts(actor, run_id, max_contacts=args.max_enrich)
            update_pipeline_run(
                run_id,
                enriched_count=enrich_stats["enriched"],
                unenriched_count=enrich_stats["unenriched"],
            )
        else:
            print("\nStep 3: Enrich — SKIPPED")

        # Step 4: Import to HubSpot
        if not args.skip_import:
            print("\nStep 4: Importing to HubSpot...")
            import_stats = import_contacts(actor, run_id)
            update_pipeline_run(run_id, imported_count=import_stats["created"])
        else:
            print("\nStep 4: Import — SKIPPED")

        # Optional: Export CSV
        if args.export_csv:
            ensure_output_dir()
            from .supabase_client import get_contacts_by_status
            enriched = get_contacts_by_status(actor_id, "enriched", pipeline_run_id=run_id)
            imported = get_contacts_by_status(actor_id, "imported", pipeline_run_id=run_id)
            all_done = enriched + imported
            if all_done:
                date_str = datetime.now().strftime("%Y-%m-%d")
                csv_path = OUTPUT_DIR / f"{name.replace(' ', '_')}_{date_str}.csv"
                write_csv(csv_path, all_done)
                print(f"\nCSV exported: {csv_path}")

        # Mark run as completed
        update_pipeline_run(
            run_id,
            status="completed",
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
        print(f"\nPipeline complete for {name}.")

    except Exception as exc:
        update_pipeline_run(
            run_id,
            status="failed",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error_message=str(exc)[:500],
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="LinkedIn scraper pipeline")
    parser.add_argument("--actor-id", type=str, default=None,
                        help="UUID of a specific actor (default: all active actors)")
    parser.add_argument("--search-url", type=str, default=None,
                        help="Sales Navigator search URL to scrape (instead of connections)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max pages to scrape")
    parser.add_argument("--max-enrich", type=int, default=None,
                        help="Max contacts to enrich")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip the scrape step")
    parser.add_argument("--skip-filter", action="store_true",
                        help="Skip the filter step")
    parser.add_argument("--skip-enrich", action="store_true",
                        help="Skip the enrich step")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip the HubSpot import step")
    parser.add_argument("--icp-review", action="store_true",
                        help="Run AI ICP review before enrichment")
    parser.add_argument("--export-csv", action="store_true",
                        help="Export results to CSV")
    args = parser.parse_args()

    if args.actor_id:
        actor = get_actor_by_id(args.actor_id)
        if not actor:
            print(f"ERROR: No actor found with id '{args.actor_id}'", file=sys.stderr)
            sys.exit(1)
        actors = [actor]
        print(f"Targeting actor: {actor['name']}")
    else:
        actors = get_active_actors()
        print(f"Loaded {len(actors)} active actor(s) from Supabase")

    for actor in actors:
        try:
            run(actor, args)
        except Exception as e:
            print(f"\nERROR processing {actor.get('name', '?')}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            continue

    print(f"\n{'=' * 60}")
    print("All done.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
