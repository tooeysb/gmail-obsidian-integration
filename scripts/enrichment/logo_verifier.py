#!/usr/bin/env python3
"""
Logo verification: compare company website logos with LinkedIn profile logos.

For each company with both a linkedin_url and domain, extracts logos from both
sites and compares them using perceptual hashing (pHash). Companies with
matching logos get marked as logo_verified=True.

Usage:
    python -m scripts.enrichment.logo_verifier              # Full run
    python -m scripts.enrichment.logo_verifier --dry-run    # Preview only
    python -m scripts.enrichment.logo_verifier --limit 5    # Process N companies
    python -m scripts.enrichment.logo_verifier --headless   # Run headless
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from scripts.enrichment.browser import LinkedInBrowser  # noqa: E402
from scripts.enrichment.crm_client import CRMClient  # noqa: E402
from scripts.enrichment.human_behavior import (  # noqa: E402
    WorkSchedule,
    delay_between_profiles,
)
from scripts.enrichment.logo_utils import (  # noqa: E402
    MATCH_THRESHOLD,
    SOFT_MATCH_THRESHOLD,
    best_hash_distance,
    domains_match,
    extract_linkedin_domain,
    extract_linkedin_logo,
    extract_linkedin_name,
    extract_website_logo,
    extract_website_title,
    hash_distance,
    names_match,
)
from src.core.config import settings  # noqa: E402
from src.core.logging import get_logger  # noqa: E402

logger = get_logger(__name__)

API_BASE = os.environ.get("ENRICHMENT_API_BASE", "https://crm-hth-0f0e9a31256d.herokuapp.com")
API_KEY = os.environ.get("ENRICHMENT_API_KEY", "")


def verify_company_logo(
    company: dict,
    browser: LinkedInBrowser,
    crm: CRMClient,
    *,
    dry_run: bool = False,
    match_threshold: int = MATCH_THRESHOLD,
) -> str:
    """Verify a company's LinkedIn page matches their website.

    Check order (fast → slow):
    1. Domain match — LinkedIn About page lists the same domain → verified
    2. Name match + logo pHash — name matches AND logo within soft threshold → verified
    3. Logo pHash alone — distance <= strict threshold → verified

    Returns: "domain_match", "match", "no_match", "website_failed", "linkedin_failed"
    """
    name = company["name"]
    domain = company["domain"]
    linkedin_url = company["linkedin_url"]
    cid = company["id"]

    logger.info("Verifying: %s (domain=%s)", name, domain)

    page = browser._page

    # Step 1: Check LinkedIn domain (fastest — navigates to About page)
    linkedin_domain = extract_linkedin_domain(page, linkedin_url)
    # Extract name from the same page (already on LinkedIn About)
    linkedin_name = extract_linkedin_name(page)
    if linkedin_name:
        logger.info("LinkedIn name: %s", linkedin_name)
    if domains_match(domain, linkedin_domain):
        logger.info("DOMAIN MATCH: %s — LinkedIn lists %s, verified", name, linkedin_domain)
        if not dry_run:
            crm.update_company(
                cid,
                logo_verified=True,
                logo_verified_at=datetime.now(UTC).isoformat(),
            )
        return "domain_match"

    if linkedin_domain:
        logger.info("Domain mismatch: company=%s, LinkedIn=%s", domain, linkedin_domain)

    # Step 2: Extract website logo + title
    website_result = extract_website_logo(page, domain)
    website_title = extract_website_title(page)
    if website_title:
        logger.info("Website title: %s", website_title)

    if not website_result.phash:
        logger.warning("No website logo for %s: %s", name, website_result.error or "unknown")
        if not dry_run:
            crm.update_company(cid, logo_verified_at=datetime.now(UTC).isoformat())
        return "website_failed"

    logger.info("Website pHash: %s (from %s)", website_result.phash, website_result.source_url)

    # Step 3: Extract LinkedIn logo
    linkedin_result = extract_linkedin_logo(page, linkedin_url)
    if not linkedin_result.phash:
        logger.warning("No LinkedIn logo for %s: %s", name, linkedin_result.error or "unknown")
        if not dry_run:
            crm.update_company(
                cid,
                logo_verified_at=datetime.now(UTC).isoformat(),
                logo_hash_website=website_result.phash,
            )
        return "linkedin_failed"

    logger.info("LinkedIn pHash: %s (from %s)", linkedin_result.phash, linkedin_result.source_url)

    # Step 4: Compare logos
    full_distance = hash_distance(website_result.phash, linkedin_result.phash)
    website_hashes = website_result.phash_crops or [website_result.phash]
    linkedin_hashes = linkedin_result.phash_crops or [linkedin_result.phash]
    distance = best_hash_distance(website_hashes, linkedin_hashes)

    # Logo alone within strict threshold
    is_match = distance <= match_threshold

    # Name match + logo within soft threshold
    if not is_match and distance <= SOFT_MATCH_THRESHOLD:
        name_confirmed = names_match(name, website_title, linkedin_name)
        if name_confirmed:
            logger.info(
                "Logo distance %d + company name confirmed — verified",
                distance,
            )
            is_match = True

    logger.info(
        "Logo comparison for %s: full=%d, best=%d (crops), threshold=%d, match=%s",
        name,
        full_distance,
        distance,
        match_threshold,
        is_match,
    )

    # Step 5: Update
    if not dry_run:
        crm.update_company(
            cid,
            logo_verified=is_match,
            logo_verified_at=datetime.now(UTC).isoformat(),
            logo_hash_website=website_result.phash,
            logo_hash_linkedin=linkedin_result.phash,
            logo_hash_distance=distance,
        )

    status = "match" if is_match else "no_match"
    logger.info("%s: %s (distance=%d)", status.upper(), name, distance)
    return status


def main():
    parser = argparse.ArgumentParser(description="Logo verification for LinkedIn company pages")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API updates")
    parser.add_argument("--limit", type=int, default=0, help="Max companies to process (0=all)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--proxy", action="store_true", help="Use rotating proxy")
    parser.add_argument("--no-schedule", action="store_true", help="Skip work schedule pacing")
    parser.add_argument("--setup", action="store_true", help="Interactive LinkedIn login")
    parser.add_argument(
        "--match-threshold",
        type=int,
        default=MATCH_THRESHOLD,
        help=f"pHash distance threshold for match (default: {MATCH_THRESHOLD})",
    )
    args = parser.parse_args()

    if args.setup:
        browser = LinkedInBrowser(headless=False)
        browser.setup_auth()
        return

    logger.info(
        "Logo Verifier starting (dry_run=%s, limit=%s, threshold=%d)",
        args.dry_run,
        args.limit,
        args.match_threshold,
    )

    # Work schedule
    schedule = WorkSchedule()
    check_hours = not args.no_schedule
    use_pacing = not args.no_schedule

    if check_hours and not schedule.wait_for_work_hours():
        logger.info("Past work hours — exiting")
        return

    # Graceful shutdown
    shutdown_flag = [False]

    def _signal_handler(signum, frame):
        logger.info("Shutdown requested (signal %d)", signum)
        shutdown_flag[0] = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    api_key = API_KEY or settings.secret_key
    if not api_key:
        logger.error("No API key — set ENRICHMENT_API_KEY or SECRET_KEY")
        return

    crm = CRMClient(base_url=API_BASE, api_key=api_key)
    browser = LinkedInBrowser(headless=args.headless, proxy=args.proxy)

    try:
        browser.start()

        companies = crm.get_needs_logo_verification()
        logger.info("Companies needing logo verification: %d", len(companies))

        if args.limit:
            companies = companies[: args.limit]

        stats = {
            "domain_match": 0,
            "match": 0,
            "no_match": 0,
            "website_failed": 0,
            "linkedin_failed": 0,
            "errors": 0,
        }

        for i, company in enumerate(companies):
            if shutdown_flag[0]:
                logger.info("Shutdown requested — stopping")
                break

            if check_hours and not schedule.wait_for_work_hours():
                logger.info("Work day ended — stopping")
                break

            if use_pacing and schedule.should_take_break():
                schedule.take_break()

            logger.info("[%d/%d] %s", i + 1, len(companies), company["name"])

            try:
                status = verify_company_logo(
                    company,
                    browser,
                    crm,
                    dry_run=args.dry_run,
                    match_threshold=args.match_threshold,
                )
                stats[status] = stats.get(status, 0) + 1
            except Exception as e:
                logger.error("Error verifying %s: %s", company["name"], e)
                stats["errors"] += 1

            if use_pacing and i < len(companies) - 1:
                delay_between_profiles()

        logger.info(
            "Logo verification complete: %d domain match, %d logo match, %d no match, "
            "%d website failed, %d linkedin failed, %d errors",
            stats["domain_match"],
            stats["match"],
            stats["no_match"],
            stats["website_failed"],
            stats["linkedin_failed"],
            stats["errors"],
        )

    finally:
        browser.stop()
        crm.close()


if __name__ == "__main__":
    main()
