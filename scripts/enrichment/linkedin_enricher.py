#!/usr/bin/env python3
"""
Automated LinkedIn contact enrichment via browser automation.

Fetches contacts from the CRM API that need LinkedIn data, visits their
LinkedIn profiles in a real browser, extracts title and company info,
and patches the CRM via API.

Runs as a scheduled daily job via macOS launchd.

Usage:
    python -m scripts.enrichment.linkedin_enricher              # Full run
    python -m scripts.enrichment.linkedin_enricher --setup      # One-time login
    python -m scripts.enrichment.linkedin_enricher --dry-run    # Preview only
    python -m scripts.enrichment.linkedin_enricher --limit 5    # Process N contacts
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure project root is on PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env before reading env vars
from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from scripts.enrichment.browser import LinkedInBrowser  # noqa: E402
from scripts.enrichment.crm_client import ContactToEnrich, CRMClient  # noqa: E402
from scripts.enrichment.human_behavior import WorkSchedule, delay_between_profiles  # noqa: E402
from scripts.enrichment.state import EnrichmentState  # noqa: E402
from src.core.config import settings  # noqa: E402
from src.core.logging import get_logger  # noqa: E402

logger = get_logger(__name__)

API_BASE = os.environ.get("ENRICHMENT_API_BASE", "https://crm-hth-0f0e9a31256d.herokuapp.com")
API_KEY = os.environ.get("ENRICHMENT_API_KEY", "")


def _flag_for_review(crm: CRMClient, contact: ContactToEnrich, reason: str, dry_run: bool):
    """Mark a contact as needing human research with a reason."""
    logger.info("Flagging %s for human review: %s", contact.name, reason)
    if not dry_run:
        crm.update_contact(
            contact.id,
            enrichment_status="needs_review",
            enrichment_notes=reason,
        )


def enrich_contact(
    contact: ContactToEnrich,
    browser: LinkedInBrowser,
    crm: CRMClient,
    dry_run: bool = False,
) -> bool:
    """
    Enrich a single contact with LinkedIn data.

    Returns True if enrichment succeeded.
    """
    logger.info(
        "Processing: %s (%s) — company: %s",
        contact.name,
        contact.email,
        contact.company_name,
    )

    linkedin_url = contact.linkedin_url
    search_name = contact.name or contact.email.split("@")[0]

    # Step 1: If no LinkedIn URL, search Google for it
    if not linkedin_url:
        # Flag contacts with only a first name (no space = single word) — too ambiguous
        if contact.name and " " not in contact.name.strip():
            _flag_for_review(
                crm,
                contact,
                f"Only first name available ({contact.name}) — too ambiguous for automated search",
                dry_run,
            )
            return False

        linkedin_url = browser.search_google_for_linkedin(search_name, contact.company_name)
        if not linkedin_url:
            _flag_for_review(
                crm,
                contact,
                f"No LinkedIn profile found via Google search for '{search_name}'",
                dry_run,
            )
            return False

    # Step 2: Visit LinkedIn profile and extract data
    profile = browser.extract_profile(linkedin_url)

    # Filter out junk titles
    junk_titles = {"--", "-", ".", "...", "n/a", "na", "none", "linkedin member"}
    if not profile.title or profile.title.strip().lower() in junk_titles:
        _flag_for_review(
            crm,
            contact,
            f"No usable job title on LinkedIn profile ({linkedin_url})",
            dry_run,
        )
        return False

    # Verify company match — name alone isn't enough confidence
    if contact.company_name and profile.company_name:
        crm_norm = _normalize_company_name(contact.company_name)
        li_norm = _normalize_company_name(profile.company_name)
        if crm_norm and li_norm and crm_norm not in li_norm and li_norm not in crm_norm:
            _flag_for_review(
                crm,
                contact,
                f"Company mismatch: CRM has '{contact.company_name}' but LinkedIn shows "
                f"'{profile.company_name}' — may be wrong person ({linkedin_url})",
                dry_run,
            )
            return False

    if dry_run:
        logger.info(
            "[DRY RUN] Would update contact %s: title=%s, linkedin_url=%s",
            contact.id,
            profile.title,
            linkedin_url,
        )
        if profile.company_name:
            logger.info(
                "[DRY RUN] Would update company: name=%s, linkedin_url=%s",
                profile.company_name,
                profile.company_linkedin_url,
            )
        return True

    # Step 3: Update contact via API
    update_fields: dict = {
        "title": profile.title,
        "enrichment_status": "enriched",
        "enrichment_notes": None,
    }
    if linkedin_url and not contact.linkedin_url:
        update_fields["linkedin_url"] = linkedin_url
    crm.update_contact(contact.id, **update_fields)
    logger.info("Updated contact %s: title=%s", contact.name, profile.title)

    # Step 4: Update company name and LinkedIn URL if found
    if contact.company_name and (profile.company_name or profile.company_linkedin_url):
        _update_company(crm, contact.company_name, profile)

    return True


def _normalize_company_name(name: str) -> str:
    """Normalize company name for comparison (strip suffixes, lowercase)."""
    if not name:
        return ""
    name = name.lower().strip()
    # Strip common corporate suffixes
    for suffix in (
        ", inc.",
        ", inc",
        " inc.",
        " inc",
        ", llc",
        " llc",
        ", ltd.",
        ", ltd",
        " ltd.",
        " ltd",
        ", corp.",
        ", corp",
        " corp.",
        " corp",
        " corporation",
        " incorporated",
        " company",
        ", l.p.",
        " l.p.",
    ):
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def recheck_contact(
    contact: ContactToEnrich,
    browser: LinkedInBrowser,
    crm: CRMClient,
    dry_run: bool = False,
) -> str:
    """
    Re-check a contact's LinkedIn profile for job change.

    Returns: "match", "changed", or "error".
    """
    logger.info(
        "Re-checking: %s (%s) — CRM company: %s",
        contact.name,
        contact.email,
        contact.company_name,
    )

    if not contact.linkedin_url:
        return "error"

    profile = browser.extract_profile(contact.linkedin_url)
    now = datetime.now(UTC).isoformat()

    # Compare company names
    crm_company = _normalize_company_name(contact.company_name or "")
    linkedin_company = _normalize_company_name(profile.company_name or "")

    if not linkedin_company:
        # LinkedIn shows no company — may have retired or profile incomplete
        logger.info("No company on LinkedIn for %s — flagging as changed", contact.name)
        if not dry_run:
            crm.update_contact(
                contact.id,
                is_active=False,
                linkedin_company_raw=profile.company_name or "(no company listed)",
                job_change_detected_at=now,
                last_linkedin_check_at=now,
            )
        return "changed"

    if crm_company and crm_company in linkedin_company or linkedin_company in crm_company:
        # Names overlap — still at same company
        logger.info("Company match for %s: '%s'", contact.name, profile.company_name)
        if not dry_run:
            crm.update_contact(contact.id, last_linkedin_check_at=now)
        return "match"

    # Mismatch — job change detected
    logger.info(
        "JOB CHANGE: %s moved from '%s' to '%s'",
        contact.name,
        contact.company_name,
        profile.company_name,
    )
    if not dry_run:
        crm.update_contact(
            contact.id,
            is_active=False,
            linkedin_company_raw=profile.company_name,
            job_change_detected_at=now,
            last_linkedin_check_at=now,
        )
    return "changed"


def _update_company(crm: CRMClient, search_name: str, profile) -> None:
    """Search for the company in CRM and update its LinkedIn URL and track name mismatches."""
    try:
        companies = crm.search_companies(search_name)
        if not companies:
            return

        company = companies[0]
        update_fields: dict = {}

        # Track LinkedIn name when it differs from CRM name (for manual review)
        if profile.company_name and profile.company_name != company.get("name"):
            update_fields["linkedin_name"] = profile.company_name

        # Update company LinkedIn URL if not already set
        if profile.company_linkedin_url and not company.get("linkedin_url"):
            update_fields["linkedin_url"] = profile.company_linkedin_url

        if update_fields:
            crm.update_company(company["id"], **update_fields)
            logger.info("Updated company %s: %s", company["name"], update_fields)
    except Exception as e:
        logger.error("Failed to update company for %s: %s", search_name, e)


def main():
    parser = argparse.ArgumentParser(description="LinkedIn contact enrichment")
    parser.add_argument("--setup", action="store_true", help="Interactive LinkedIn login")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API updates")
    parser.add_argument("--limit", type=int, default=0, help="Max contacts to process (0=all)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--no-schedule", action="store_true", help="Skip work schedule (run now)")
    parser.add_argument(
        "--start-now", action="store_true", help="Skip initial work hours wait but keep pacing"
    )
    args = parser.parse_args()

    browser = LinkedInBrowser(headless=args.headless)

    # --setup mode: interactive login
    if args.setup:
        browser.setup_auth()
        return

    logger.info(
        "LinkedIn Enricher starting (dry_run=%s, limit=%s)",
        args.dry_run,
        args.limit,
    )

    # Load state and reset if new day
    state = EnrichmentState.load()
    state.reset_if_new_day()

    # Initialize work schedule
    # --start-now: skip work hours checks but keep human-like pacing/breaks
    # --no-schedule: skip everything (work hours, pacing, breaks)
    schedule = WorkSchedule()
    check_hours = not args.no_schedule and not args.start_now
    use_pacing = not args.no_schedule
    if check_hours:
        if not schedule.wait_for_work_hours():
            logger.info("Past work hours for today — exiting")
            return

    # Graceful shutdown on SIGINT/SIGTERM
    shutdown_requested = False

    def _signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown requested (signal %d) — finishing current contact", signum)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Initialize API client (prefer ENRICHMENT_API_KEY env var, fall back to settings)
    api_key = API_KEY or settings.secret_key
    if not api_key:
        logger.error("No API key — set ENRICHMENT_API_KEY env var or SECRET_KEY in .env")
        return
    crm = CRMClient(base_url=API_BASE, api_key=api_key)

    try:
        browser.start()

        processed_count = 0

        # Phase 1: Re-check enriched contacts for job changes (HIGHEST PRIORITY)
        # Detect people who left their company — most actionable intelligence
        queue_recheck = crm.get_needs_recheck()
        recheck_contacts = [c for c in queue_recheck if not state.is_processed(c.id)]
        logger.info(
            "Re-check queue: %d contacts due (%d already done today)",
            len(recheck_contacts),
            len(queue_recheck) - len(recheck_contacts),
        )

        if args.limit:
            recheck_contacts = recheck_contacts[: args.limit]

        recheck_matches = 0
        recheck_changes = 0

        for contact in recheck_contacts:
            if check_hours and not schedule.wait_for_work_hours():
                logger.info("Work day ended — stopping")
                break

            if shutdown_requested:
                logger.info("Shutdown requested — stopping gracefully")
                break

            if use_pacing and schedule.should_take_break():
                schedule.take_break()

            try:
                result = recheck_contact(contact, browser, crm, dry_run=args.dry_run)
                state.mark_processed(contact.id)
                if result == "match":
                    recheck_matches += 1
                elif result == "changed":
                    recheck_changes += 1
            except Exception as e:
                logger.error("Error re-checking %s: %s", contact.id, e)
                state.mark_skipped(contact.id)

            state.save()
            processed_count += 1

            if use_pacing:
                delay_between_profiles()

        logger.info(
            "Re-check complete: %d still at company, %d job changes detected",
            recheck_matches,
            recheck_changes,
        )

        # Phase 2: Enrich contacts that need LinkedIn data
        if not shutdown_requested:
            queue_browser = crm.get_needs_browser_enrich()
            queue_url = crm.get_needs_linkedin_url()
            logger.info(
                "Enrichment queues: %d need browser enrich, %d need LinkedIn URL",
                len(queue_browser),
                len(queue_url),
            )

            # Prioritize contacts that already have LinkedIn URLs (faster)
            all_contacts = queue_browser + queue_url

            # Filter out already-processed contacts
            contacts = [c for c in all_contacts if not state.is_processed(c.id)]
            logger.info(
                "%d contacts to enrich (%d already done today)",
                len(contacts),
                len(all_contacts) - len(contacts),
            )

            if args.limit:
                remaining = max(0, args.limit - processed_count)
                contacts = contacts[:remaining]

            for contact in contacts:
                if check_hours and not schedule.wait_for_work_hours():
                    logger.info("Work day ended — stopping")
                    break

                if shutdown_requested:
                    logger.info("Shutdown requested — stopping gracefully")
                    break

                if use_pacing and schedule.should_take_break():
                    schedule.take_break()

                try:
                    success = enrich_contact(contact, browser, crm, dry_run=args.dry_run)
                    if success:
                        state.mark_processed(contact.id)
                    else:
                        state.mark_skipped(contact.id)
                except Exception as e:
                    logger.error("Error processing contact %s: %s", contact.id, e)
                    state.mark_error()
                    state.mark_skipped(contact.id)

                state.save()

                if use_pacing:
                    delay_between_profiles()

            logger.info(
                "Enrichment complete: %d enriched, %d skipped, %d errors",
                state.total_enriched,
                state.total_skipped,
                state.total_errors,
            )

    finally:
        browser.stop()
        crm.close()
        state.save()


if __name__ == "__main__":
    main()
