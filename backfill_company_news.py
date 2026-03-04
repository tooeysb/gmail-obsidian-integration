#!/usr/bin/env python3
"""
Backfill news articles for all CRM companies via Google News RSS.

For each company, searches Google News with the company's full name,
verifies matches to avoid ambiguity (e.g. "Turner Construction" vs
"Whiting-Turner"), and stores articles in the database.

Usage:
    python backfill_company_news.py                    # All companies (14-day default)
    python backfill_company_news.py --limit 10         # First 10
    python backfill_company_news.py --company "Turner"  # Search by name
    python backfill_company_news.py --max-age-days 7   # Last 7 days only
    python backfill_company_news.py --dry-run          # Preview only
"""

import argparse
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import feedparser
from dateutil import parser as dateutil_parser
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.database import SyncSessionLocal
from src.core.logging import get_logger
from src.models.company import Company
from src.models.company_news import CompanyNewsItem
from src.services.news.company_names import SKIP_NAMES, clean_company_name

logger = get_logger(__name__)


def _build_search_url(company_name: str, max_age_days: int | None = None) -> str:
    """Build a Google News RSS search URL for a company."""
    # Use quoted name for exact match + "construction" for industry context
    query = f'"{company_name}" construction'
    if max_age_days:
        query += f" when:{max_age_days}d"
    return (
        f"https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )


def _name_variants(name: str) -> list[str]:
    """Generate plausible name variants for matching (e.g. with/without 'The')."""
    seen = set()
    variants = []

    def _add(v):
        v = v.strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            variants.append(v)

    _add(name)
    # Strip "The " prefix
    lower = name.lower()
    no_the = name[4:].strip() if lower.startswith("the ") else name
    _add(no_the)
    # Strip trailing industry suffixes
    for base in [name, no_the]:
        for suffix in [" contracting", " construction", " builders", " building", " services"]:
            if base.lower().endswith(suffix):
                _add(base[: -len(suffix)])
    return variants


def _verify_company_match(
    title: str,
    summary: str,
    target_name: str,
    all_companies: list[str],
) -> bool:
    """
    Verify that an article genuinely mentions the target company,
    not a similarly-named one.

    Returns True only if:
    1. The target company name appears in the text (word boundary match)
    2. No longer company name also matches (ambiguity check)
    """
    text = f"{title} {summary}".lower()

    # Check any variant of the target name appears with word boundaries
    variants = _name_variants(target_name)
    matched_variant = None
    for variant in variants:
        pattern = r"\b" + re.escape(variant.lower()) + r"\b"
        if re.search(pattern, text):
            matched_variant = variant
            break

    if not matched_variant:
        return False

    target_lower = matched_variant.lower()

    # Ambiguity check: does another company also match in the text?
    # Only flag if the other company's name is an exact word-boundary match
    # AND the other name is more specific (longer) than ours.
    for other_name in all_companies:
        other_lower = other_name.lower()
        if other_lower == target_lower:
            continue
        # Check all variants of the other company
        for other_variant in _name_variants(other_name):
            ov_lower = other_variant.lower()
            if ov_lower == target_lower:
                continue
            # Only check if the other name appears in text
            other_pattern = r"\b" + re.escape(ov_lower) + r"\b"
            if not re.search(other_pattern, text):
                continue
            # Both companies match this article. Decide who gets it:
            # If target is a substring of the other match (e.g. "Turner" inside
            # "Whiting-Turner"), skip for target — the other is more specific.
            # But "Turner Construction" vs "Whiting-Turner" are independent
            # names, so only conflict if one is truly a sub-phrase of the other.
            if target_lower in ov_lower and target_lower != ov_lower:
                # Our name is fully contained in the other — other is more specific
                logger.debug(
                    "Ambiguous: '%s' matches both '%s' and '%s' — skipping",
                    title[:60],
                    target_name,
                    other_variant,
                )
                return False

    return True


def backfill_company(
    db: Session,
    company: Company,
    all_company_names: list[str],
    user_id: str,
    dry_run: bool = False,
    max_age_days: int | None = None,
) -> dict:
    """Search Google News for a single company and store articles."""
    # Use manual override if set, otherwise auto-detect
    has_override = bool(company.news_search_override)
    if has_override:
        search_name = company.news_search_override.strip()
        clean_name = search_name  # Use override for matching too
        logger.info("Using search override: %s (for %s)", search_name, company.name)
    else:
        clean_name = clean_company_name(company.name)
        # Use the best search name — shortest meaningful variant
        search_variants = _name_variants(clean_name)
        # Pick the shortest variant that's still >5 chars (most recognizable)
        search_name = min(
            (v for v in search_variants if len(v) > 5),
            key=len,
            default=search_variants[0],
        )

        # Skip overly generic names
        if clean_name.lower() in SKIP_NAMES or search_name.lower() in SKIP_NAMES:
            logger.info("Skipping generic name: %s", company.name)
            return {"searched": False, "reason": "generic_name"}

        if len(search_name) < 4:
            logger.info("Skipping short name: %s", company.name)
            return {"searched": False, "reason": "too_short"}

    url = _build_search_url(search_name, max_age_days=max_age_days)
    logger.info("Searching Google News for: %s (max %s days)", search_name, max_age_days or "all")

    try:
        feed = feedparser.parse(url)
    except Exception:
        logger.exception("Failed to parse feed for %s", company.name)
        return {"searched": True, "articles": 0, "stored": 0, "errors": 1}

    # Compute date cutoff for filtering
    date_cutoff = None
    if max_age_days:
        date_cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    stats = {
        "searched": True,
        "articles": len(feed.entries),
        "stored": 0,
        "skipped_ambiguous": 0,
        "skipped_too_old": 0,
    }

    for entry in feed.entries[:30]:  # Cap per company
        title = entry.get("title", "")
        link = entry.get("link", "")
        summary = entry.get("summary", "")
        published = entry.get("published", "")

        if not title or not link:
            continue

        # Strip HTML from summary
        summary_clean = re.sub(r"<[^>]*>", "", summary).strip()

        # Verify this article is actually about our target company
        # Skip ambiguity check when user provided an explicit override
        if has_override:
            # Just check the override name appears in the article
            text = f"{title} {summary_clean}".lower()
            pattern = r"\b" + re.escape(clean_name.lower()) + r"\b"
            if not re.search(pattern, text):
                stats["skipped_ambiguous"] += 1
                continue
        elif not _verify_company_match(title, summary_clean, clean_name, all_company_names):
            stats["skipped_ambiguous"] += 1
            continue

        # Parse published date
        published_at = None
        if published:
            try:
                published_at = dateutil_parser.parse(published)
            except (ValueError, OverflowError):
                pass

        # Enforce date cutoff (skip articles older than max_age_days)
        if date_cutoff and published_at:
            pub_utc = published_at.astimezone(timezone.utc) if published_at.tzinfo else published_at
            if pub_utc < date_cutoff:
                stats["skipped_too_old"] += 1
                continue

        if dry_run:
            logger.info(
                "  [DRY RUN] %s | %s | %s",
                company.name,
                title[:80],
                published_at.strftime("%Y-%m-%d") if published_at else "no date",
            )
            stats["stored"] += 1
            continue

        stmt = (
            pg_insert(CompanyNewsItem)
            .values(
                id=uuid.uuid4(),
                user_id=user_id,
                company_id=company.id,
                source_url=link[:2048],
                source_type="google_news_backfill",
                title=title[:500],
                summary=summary_clean[:2000] or None,
                published_at=published_at,
                status="new",
            )
            .on_conflict_do_nothing(constraint="uq_company_news_source")
        )

        result = db.execute(stmt)
        if result.rowcount > 0:
            stats["stored"] += 1

    if not dry_run:
        db.commit()

    logger.info(
        "  %s: %d articles found, %d stored, %d ambiguous, %d too old",
        company.name,
        stats["articles"],
        stats["stored"],
        stats["skipped_ambiguous"],
        stats["skipped_too_old"],
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill company news from Google News RSS")
    parser.add_argument("--limit", type=int, help="Max companies to process")
    parser.add_argument("--company", type=str, help="Filter by company name (partial match)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without storing")
    parser.add_argument(
        "--user-id",
        default="d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31",
        help="User ID",
    )
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between companies")
    parser.add_argument(
        "--max-age-days", type=int, default=14, help="Only store articles this many days old (0=all)"
    )
    args = parser.parse_args()

    max_age = args.max_age_days if args.max_age_days > 0 else None

    db = SyncSessionLocal()
    try:
        # Load all companies
        query = db.query(Company).filter(Company.user_id == args.user_id).order_by(Company.name)

        if args.company:
            query = query.filter(Company.name.ilike(f"%{args.company}%"))

        if args.limit:
            query = query.limit(args.limit)

        companies = query.all()
        logger.info("Found %d companies to process", len(companies))

        # Build full list of all company names for ambiguity checking
        all_names_query = db.query(Company.name).filter(Company.user_id == args.user_id).all()
        all_company_names = [clean_company_name(row[0]) for row in all_names_query]

        totals = {
            "processed": 0,
            "skipped": 0,
            "total_articles": 0,
            "total_stored": 0,
            "total_ambiguous": 0,
            "total_too_old": 0,
        }
        skipped_companies = []  # Track companies that couldn't be searched

        for i, company in enumerate(companies):
            logger.info("[%d/%d] %s", i + 1, len(companies), company.name)

            result = backfill_company(
                db,
                company,
                all_company_names,
                args.user_id,
                dry_run=args.dry_run,
                max_age_days=max_age,
            )

            if not result.get("searched"):
                totals["skipped"] += 1
                skipped_companies.append(
                    f"{company.name} — {result.get('reason', 'unknown')}"
                )
            else:
                totals["processed"] += 1
                totals["total_articles"] += result.get("articles", 0)
                totals["total_stored"] += result.get("stored", 0)
                totals["total_ambiguous"] += result.get("skipped_ambiguous", 0)
                totals["total_too_old"] += result.get("skipped_too_old", 0)

            # Rate limit between companies
            if i < len(companies) - 1:
                time.sleep(args.delay)

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("  Companies processed: %d", totals["processed"])
        logger.info("  Companies skipped:   %d", totals["skipped"])
        logger.info("  Total articles found: %d", totals["total_articles"])
        logger.info("  Total articles stored: %d", totals["total_stored"])
        logger.info("  Ambiguous skipped:   %d", totals["total_ambiguous"])
        logger.info("  Too old skipped:     %d", totals["total_too_old"])
        if max_age:
            logger.info("  Date filter:         last %d days", max_age)
        if args.dry_run:
            logger.info("  (DRY RUN — nothing was stored)")

        # Write skipped companies list for user review
        if skipped_companies:
            skip_file = "skipped_companies.txt"
            with open(skip_file, "w") as f:
                f.write(f"Skipped {len(skipped_companies)} companies:\n\n")
                for name in sorted(skipped_companies):
                    f.write(f"  {name}\n")
            logger.info("  Skipped companies written to: %s", skip_file)

    finally:
        db.close()


if __name__ == "__main__":
    main()
