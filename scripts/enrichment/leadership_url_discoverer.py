#!/usr/bin/env python3
"""
Fast async HTTP-based leadership page URL discovery.

For each company with a domain but no leadership page:
1. Probe common leadership URL patterns in parallel (22 requests per company)
2. Fallback: scan the company homepage for leadership-related links
3. PATCH the company via CRM API with the discovered URL

No browser automation. No leader scraping. Just URL discovery.

Usage:
    python -m scripts.enrichment.leadership_url_discoverer                    # Full run
    python -m scripts.enrichment.leadership_url_discoverer --dry-run          # Preview only
    python -m scripts.enrichment.leadership_url_discoverer --limit 10         # Process N companies
    python -m scripts.enrichment.leadership_url_discoverer --concurrency 100  # Max parallel
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure project root is on PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env before reading env vars
from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

import httpx  # noqa: E402

from src.core.config import settings  # noqa: E402
from src.core.logging import get_logger  # noqa: E402

logger = get_logger(__name__)

API_BASE = os.environ.get("ENRICHMENT_API_BASE", "https://crm-hth-0f0e9a31256d.herokuapp.com")
API_KEY = os.environ.get("ENRICHMENT_API_KEY", "")

# Common leadership page URL patterns (same as leadership_discoverer.py)
LEADERSHIP_URL_PATTERNS = [
    "/about/leadership",
    "/about-us/leadership",
    "/leadership",
    "/our-team",
    "/about/team",
    "/about-us/our-team",
    "/about/our-leadership",
    "/about/executives",
    "/about/management",
    "/people",
    "/team",
    "/about",
    "/about-us",
]

# Keywords that indicate a page contains leadership content
TITLE_KEYWORDS = re.compile(
    r"(president|chief|ceo|cfo|coo|cto|cio|cmo|cpo|evp|svp|"
    r"vice president|vp|director|head of|managing|partner|founder|"
    r"general manager|executive|officer|principal|senior)",
    re.IGNORECASE,
)

# Keywords in links that suggest a leadership page
LINK_KEYWORDS = re.compile(
    r"(leader|team|people|executive|management|staff|about.*/team|our.team)",
    re.IGNORECASE,
)

# Timeout per HTTP request (seconds)
REQUEST_TIMEOUT = 10.0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


class AsyncCRMClient:
    """Async HTTP client for CRM API (leadership discovery only)."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"X-API-Key": api_key},
            timeout=timeout,
        )

    async def get_needs_leadership(self) -> list[dict]:
        resp = await self._client.get("/crm/api/reports/needs-leadership-discovery")
        resp.raise_for_status()
        return resp.json()["items"]

    async def update_company(self, company_id: str, **fields) -> dict:
        resp = await self._client.patch(f"/crm/api/companies/{company_id}", json=fields)
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()


async def probe_url(client: httpx.AsyncClient, url: str) -> str | None:
    """Fetch a URL and validate it contains leadership content. Returns URL if valid."""
    try:
        resp = await client.get(url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type:
            return None

        if TITLE_KEYWORDS.search(resp.text):
            return str(resp.url)  # Use final URL after redirects

        return None
    except (httpx.RequestError, httpx.HTTPStatusError, Exception):
        return None


async def discover_via_patterns(client: httpx.AsyncClient, domain: str) -> str | None:
    """Probe all known URL patterns in parallel. Returns first valid URL."""
    urls = []
    for pattern in LEADERSHIP_URL_PATTERNS:
        urls.append(f"https://www.{domain}{pattern}")
        urls.append(f"https://{domain}{pattern}")

    results = await asyncio.gather(
        *(probe_url(client, url) for url in urls), return_exceptions=True
    )

    for result in results:
        if isinstance(result, str):
            return result

    return None


async def discover_via_homepage(client: httpx.AsyncClient, domain: str) -> str | None:
    """Fetch homepage and scan for links to leadership pages. Fallback strategy."""
    link_pattern = re.compile(
        r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    for homepage_url in [f"https://www.{domain}/", f"https://{domain}/"]:
        try:
            resp = await client.get(homepage_url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                continue

            candidate_urls = []
            for match in link_pattern.finditer(resp.text):
                href, link_text = match.group(1), match.group(2)

                if not (LINK_KEYWORDS.search(href) or LINK_KEYWORDS.search(link_text)):
                    continue

                if href.startswith("/"):
                    full_url = f"https://{resp.url.host}{href}"
                elif href.startswith("http") and domain in href:
                    full_url = href
                else:
                    continue

                candidate_urls.append(full_url)

            if not candidate_urls:
                continue

            # Probe discovered candidates in parallel (cap at 10)
            results = await asyncio.gather(
                *(probe_url(client, url) for url in candidate_urls[:10]),
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, str):
                    return result

        except (httpx.RequestError, Exception):
            continue

    return None


async def process_company(
    company: dict,
    http_client: httpx.AsyncClient,
    crm: AsyncCRMClient,
    semaphore: asyncio.Semaphore,
    dry_run: bool = False,
) -> tuple[str, str | None]:
    """Discover leadership page URL for a single company."""
    async with semaphore:
        company_id = company["id"]
        name = company["name"]
        domain = company["domain"]

        logger.info("Probing %s (%s)", name, domain)

        # Phase A: Try known URL patterns
        url = await discover_via_patterns(http_client, domain)

        # Phase B: Fallback to homepage link scan
        if not url:
            url = await discover_via_homepage(http_client, domain)

        # Phase C: Save result
        now = datetime.now(UTC).isoformat()

        if url:
            logger.info("FOUND: %s -> %s", name, url)
            if not dry_run:
                await crm.update_company(
                    company_id, leadership_page_url=url, leadership_scraped_at=now
                )
        else:
            logger.info("NOT FOUND: %s (%s)", name, domain)
            if not dry_run:
                await crm.update_company(company_id, leadership_scraped_at=now)

        return (name, url)


async def async_main(args: argparse.Namespace):
    """Async entry point."""
    api_key = API_KEY or settings.secret_key
    if not api_key:
        logger.error("No API key — set ENRICHMENT_API_KEY or SECRET_KEY in .env")
        return

    crm = AsyncCRMClient(base_url=API_BASE, api_key=api_key)

    try:
        companies = await crm.get_needs_leadership()
        logger.info("Companies needing leadership discovery: %d", len(companies))

        if args.limit:
            companies = companies[: args.limit]

        if not companies:
            logger.info("No companies to process")
            return

        semaphore = asyncio.Semaphore(args.concurrency)

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
            limits=httpx.Limits(
                max_connections=args.concurrency * 2,
                max_keepalive_connections=args.concurrency,
            ),
            headers={"User-Agent": USER_AGENT},
        ) as http_client:
            tasks = [
                process_company(company, http_client, crm, semaphore, args.dry_run)
                for company in companies
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        found = sum(1 for r in results if isinstance(r, tuple) and r[1] is not None)
        not_found = sum(1 for r in results if isinstance(r, tuple) and r[1] is None)
        errors = sum(1 for r in results if isinstance(r, BaseException))

        logger.info(
            "Complete: %d found, %d not found, %d errors (of %d total)",
            found,
            not_found,
            errors,
            len(companies),
        )

        for r in results:
            if isinstance(r, BaseException):
                logger.error("Task error: %s", r)

    finally:
        await crm.close()


def main():
    parser = argparse.ArgumentParser(description="Fast leadership page URL discovery")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API updates")
    parser.add_argument("--limit", type=int, default=0, help="Max companies to process (0=all)")
    parser.add_argument(
        "--concurrency", type=int, default=50, help="Max concurrent companies (default 50)"
    )
    parser.add_argument("--verbose", action="store_true", help="Debug-level logging")
    args = parser.parse_args()

    if args.verbose:
        import logging

        logging.getLogger().setLevel(logging.DEBUG)

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
