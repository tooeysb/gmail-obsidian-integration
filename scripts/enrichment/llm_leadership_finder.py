#!/usr/bin/env python3
"""
LLM-based leadership discovery — ask Claude for top executives at each company.

Replaces brittle HTML scraping with a single LLM call per company. Claude draws
from LinkedIn, press releases, news articles, and public records to return names,
titles, and LinkedIn URLs for C-suite executives.

Usage:
    python -m scripts.enrichment.llm_leadership_finder --dry-run --limit 3
    python -m scripts.enrichment.llm_leadership_finder --retry-failed --dry-run
    python -m scripts.enrichment.llm_leadership_finder --retry-failed
    python -m scripts.enrichment.llm_leadership_finder --retry-failed --resume
    python -m scripts.enrichment.llm_leadership_finder --model claude-sonnet-4-5-20250929
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure project root is on PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from anthropic import Anthropic  # noqa: E402

from scripts.enrichment.crm_client import CRMClient  # noqa: E402
from src.core.config import settings  # noqa: E402
from src.core.logging import get_logger  # noqa: E402
from src.core.utils import strip_markdown_codeblocks  # noqa: E402

logger = get_logger(__name__)

API_BASE = os.environ.get("ENRICHMENT_API_BASE", "https://crm-hth-0f0e9a31256d.herokuapp.com")
API_KEY = os.environ.get("ENRICHMENT_API_KEY", "")
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
MAX_SEARCH_USES = 5

STATE_FILE = PROJECT_ROOT / ".llm_leadership_state.json"

SYSTEM_PROMPT = """You are a business research assistant. Search the web to find the top 5-10 \
senior executives at the company specified.

Search for their leadership team page and LinkedIn profiles.

IMPORTANT: Your final message must contain ONLY a JSON array. No text before or after.

Rules:
- Include CEO, President, COO, CFO, CTO, CIO, EVPs, SVPs, and other C-suite officers
- Return 5-10 executives maximum, prioritizing highest-ranking first
- Only include people currently at this company based on search results
- If you cannot find reliable information, return an empty array: []
- For linkedin_url, provide the full URL if found via search, otherwise null

Response format (JSON array only):
[{"name": "Full Name", "title": "Exact Title", "linkedin_url": "https://linkedin.com/in/..." or null}]"""


# ---------------------------------------------------------------------------
# Email guess generation (reused from leadership_discoverer.py)
# ---------------------------------------------------------------------------


def _generate_email_guesses(name: str, domain: str) -> list[str]:
    """Generate common email patterns from name + domain."""
    # Strip suffixes like Jr., Sr., III, IV, etc.
    cleaned = re.sub(r",?\s+(jr\.?|sr\.?|ii+|iv|v|ph\.?d\.?|esq\.?|md)$", "", name, flags=re.I)
    parts = cleaned.lower().split()
    if len(parts) < 2:
        return []
    first = re.sub(r"[^a-z]", "", parts[0])
    last = re.sub(r"[^a-z]", "", parts[-1])
    if not first or not last:
        return []
    return [
        f"{first}.{last}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first}{last[0]}@{domain}",
        f"{first}@{domain}",
        f"{first}{last}@{domain}",
    ]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, TypeError):
            logger.warning("Corrupt state file — starting fresh")
    return {"processed_ids": [], "total_contacts_added": 0, "total_errors": 0}


def _save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------


def find_executives(
    client: Anthropic,
    model: str,
    company_name: str,
    domain: str,
    existing_names: list[str],
) -> list[dict]:
    """Ask Claude for top executives at a company. Returns list of dicts."""
    skip_list = ""
    if existing_names:
        skip_list = f"\n\nSkip these people (already in our system): {', '.join(existing_names)}"

    user_prompt = (
        f"Find the top 5-10 senior executives at {company_name} "
        f"(website: {domain}).{skip_list}"
    )

    try:
        message = client.messages.create(
            model=model,
            max_tokens=4096,
            tools=[
                {
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": MAX_SEARCH_USES,
                }
            ],
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        logger.error("Claude API error for %s: %s", company_name, e)
        return []

    # Extract text (final block after web search tool use)
    text = ""
    for block in message.content:
        if hasattr(block, "type") and block.type == "text":
            text = block.text

    if not text.strip():
        logger.warning("Empty response for %s", company_name)
        return []

    # Parse JSON — handle cases where Claude wraps JSON in explanatory text
    cleaned = strip_markdown_codeblocks(text)
    try:
        executives = json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to extract JSON array from mixed text
        match = re.search(r"\[[\s\S]*\]", cleaned)
        if match:
            try:
                executives = json.loads(match.group())
            except json.JSONDecodeError:
                logger.error("Invalid JSON from Claude for %s: %s", company_name, cleaned[:200])
                return []
        else:
            logger.error("No JSON found in Claude response for %s: %s", company_name, cleaned[:200])
            return []

    if not isinstance(executives, list):
        logger.error("Expected list from Claude for %s, got %s", company_name, type(executives))
        return []

    # Validate each entry
    valid = []
    for exec_data in executives:
        if not isinstance(exec_data, dict):
            continue
        name = exec_data.get("name", "").strip()
        title = exec_data.get("title", "").strip()
        if not name or not title:
            continue
        valid.append({
            "name": name,
            "title": title[:255],
            "linkedin_url": exec_data.get("linkedin_url") or None,
        })

    logger.info(
        "Claude found %d executives at %s (input=%d, output=%d tokens)",
        len(valid),
        company_name,
        message.usage.input_tokens,
        message.usage.output_tokens,
    )
    return valid


# ---------------------------------------------------------------------------
# Process one company
# ---------------------------------------------------------------------------


def process_company(
    company: dict,
    claude: Anthropic,
    model: str,
    crm: CRMClient,
    dry_run: bool = False,
) -> int:
    """Find and add executives for one company. Returns count added."""
    company_name = company["name"]
    domain = company.get("domain", "")
    company_id = company["id"]

    if not domain:
        logger.warning("No domain for %s — skipping", company_name)
        return 0

    # Clean domain (remove protocol, trailing slash)
    domain = re.sub(r"^https?://", "", domain).rstrip("/")
    # Parse all domains (comma-separated) for email generation
    all_domains = [re.sub(r"^www\.", "", d.strip()).rstrip("/") for d in domain.split(",")]
    email_domains = [re.sub(r"^https?://", "", d) for d in all_domains if d]
    # Use first domain for display/search
    domain = all_domains[0] if all_domains else domain

    logger.info("Processing: %s (%s)", company_name, domain)

    # Get existing contacts for dedup
    existing_names: list[str] = []
    try:
        detail = crm.get_company_detail(company_id)
        for c in detail.get("contacts", []):
            if c.get("name"):
                existing_names.append(c["name"])
    except Exception as e:
        logger.warning("Could not fetch existing contacts for %s: %s", company_name, e)

    # Ask Claude
    executives = find_executives(claude, model, company_name, domain, existing_names)

    if not executives:
        logger.info("No executives found for %s", company_name)
        if not dry_run:
            crm.update_company(company_id, leadership_scraped_at=_now_iso())
        return 0

    # Add each executive as contact
    added = 0
    for exec_data in executives:
        name = exec_data["name"]
        title = exec_data["title"]
        linkedin_url = exec_data.get("linkedin_url")

        # Generate email guesses across all company domains
        email_guesses = []
        for ed in email_domains:
            email_guesses.extend(_generate_email_guesses(name, ed))
        if not email_guesses:
            logger.warning("Could not generate email for %s", name)
            continue
        email = email_guesses[0]

        if dry_run:
            logger.info(
                "[DRY RUN] Would add: %s | %s | %s | linkedin=%s",
                name,
                title,
                email,
                linkedin_url or "none",
            )
            added += 1
            continue

        try:
            result = crm.add_contact_to_company(
                company_id=company_id,
                email=email,
                name=name,
                title=title,
                contact_source="ai_research",
            )
            if result.get("created"):
                contact_id = result["contact"]["id"]
                logger.info("Added: %s (%s) at %s", name, title, company_name)
                added += 1

                # Update with LinkedIn URL and metadata
                update_fields: dict = {
                    "source_data": {
                        "discovered_by": "llm_leadership_finder",
                        "discovered_at": _now_iso(),
                        "all_email_guesses": email_guesses,
                        "ai_model": model,
                    },
                }
                if linkedin_url:
                    update_fields["linkedin_url"] = linkedin_url
                try:
                    crm.update_contact(contact_id, **update_fields)
                except Exception as e:
                    logger.warning("Could not update metadata for %s: %s", name, e)
            else:
                logger.info("Already exists: %s (%s)", name, email)
        except Exception as e:
            logger.error("Failed to add %s: %s", name, e)

    # Mark company as scraped
    if not dry_run:
        crm.update_company(company_id, leadership_scraped_at=_now_iso())

    return added


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="LLM-based leadership discovery")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API updates")
    parser.add_argument("--limit", type=int, default=0, help="Max companies to process (0=all)")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry GC/SC companies that were scraped but no leadership page found",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process ALL GC/SC companies (including those already scraped)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Claude model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from saved state")
    args = parser.parse_args()

    logger.info(
        "LLM Leadership Finder starting (dry_run=%s, limit=%s, retry_failed=%s, model=%s)",
        args.dry_run,
        args.limit,
        args.retry_failed,
        args.model,
    )

    # Graceful shutdown
    shutdown_requested = False

    def _signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown requested (signal %d)", signum)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Initialize clients
    crm_key = API_KEY or settings.secret_key
    if not crm_key:
        logger.error("No CRM API key — set ENRICHMENT_API_KEY")
        return

    crm = CRMClient(base_url=API_BASE, api_key=crm_key)
    claude = Anthropic(api_key=settings.anthropic_api_key)

    # Load state
    state = _load_state() if args.resume else {
        "processed_ids": [],
        "total_contacts_added": 0,
        "total_errors": 0,
    }
    processed_ids = set(state.get("processed_ids", []))

    try:
        # Fetch companies
        companies = []
        if args.all:
            all_gc_sc = crm.get_all_gc_sc()
            logger.info("All GC/SC companies: %d", len(all_gc_sc))
            companies.extend(all_gc_sc)
        else:
            if args.retry_failed:
                retry = crm.get_needs_leadership_retry()
                logger.info("Retry-failed: %d GC/SC companies", len(retry))
                companies.extend(retry)

            new = crm.get_needs_leadership()
            logger.info("New (never scraped): %d companies", len(new))
            companies.extend(new)

        # Deduplicate by ID
        seen = set()
        unique = []
        for c in companies:
            if c["id"] not in seen:
                seen.add(c["id"])
                unique.append(c)
        companies = unique

        if not companies:
            logger.info("No companies to process")
            return

        if args.limit:
            companies = companies[: args.limit]

        logger.info("Processing %d companies total", len(companies))

        total_added = 0
        total_processed = 0

        for company in companies:
            if shutdown_requested:
                logger.info("Shutdown requested — stopping")
                break

            if args.resume and company["id"] in processed_ids:
                continue

            try:
                added = process_company(
                    company, claude, args.model, crm, dry_run=args.dry_run
                )
                total_added += added
                total_processed += 1

                # Update state
                processed_ids.add(company["id"])
                state["processed_ids"] = list(processed_ids)
                state["total_contacts_added"] = (
                    state.get("total_contacts_added", 0) + added
                )
                _save_state(state)

            except Exception as e:
                logger.error("Error processing %s: %s", company["name"], e)
                state["total_errors"] = state.get("total_errors", 0) + 1
                _save_state(state)

        logger.info(
            "LLM Leadership Finder complete: %d companies processed, %d contacts added",
            total_processed,
            total_added,
        )

    finally:
        crm.close()


if __name__ == "__main__":
    main()
