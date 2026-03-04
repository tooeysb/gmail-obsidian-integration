"""
News page discovery service.

Discovers the news/press/insights page URL for each company by trying
common URL patterns against their domain.
"""

import time
from datetime import datetime, timezone

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.company import Company

logger = get_logger(__name__)

# Common news page paths, ordered by likelihood for construction companies
COMMON_PATHS = [
    "/news",
    "/newsroom",
    "/press",
    "/press-releases",
    "/insights",
    "/media",
    "/blog",
    "/updates",
    "/about/news",
    "/about/press",
    "/about/newsroom",
    "/company/news",
    "/media-center",
    "/about-us/news",
    "/resources/news",
]

# Indicators that a page is a news listing (case-insensitive)
NEWS_INDICATORS = [
    "<article",
    "<time",
    'class="news',
    'class="post',
    'class="press',
    'class="article',
    'class="blog',
    "news-item",
    "press-release",
    "news-card",
    "article-card",
]


class NewsPageDiscoveryService:
    """Discovers news pages on company websites."""

    def __init__(self, db: Session):
        self.db = db
        self.client = httpx.Client(
            timeout=8.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-NewsBot/1.0)"},
        )

    def close(self):
        self.client.close()

    def _is_news_page(self, html: str) -> bool:
        """Check if the HTML content looks like a news listing page."""
        html_lower = html.lower()
        matches = sum(1 for indicator in NEWS_INDICATORS if indicator in html_lower)
        # Require at least 2 indicators to avoid false positives
        return matches >= 2

    def _check_domain_reachable(self, domain: str) -> bool:
        """Quick HEAD check to see if domain resolves and responds."""
        try:
            resp = self.client.head(f"https://{domain}", timeout=5.0)
            return resp.status_code < 500
        except httpx.HTTPError:
            return False

    def discover_for_company(self, company: Company, dry_run: bool = False) -> str | None:
        """
        Try common URL paths for a company domain. Return first valid news page URL.
        """
        if not company.domain:
            return None

        # Quick reachability check — skip all paths if domain is down
        if not self._check_domain_reachable(company.domain):
            logger.debug("Domain unreachable: %s", company.domain)
            return None

        base_url = f"https://{company.domain}"

        for path in COMMON_PATHS:
            url = base_url + path
            try:
                resp = self.client.get(url)
                if resp.status_code == 200 and self._is_news_page(resp.text):
                    logger.info("Discovered news page for %s: %s", company.name, url)
                    if not dry_run:
                        company.news_page_url = url
                        company.news_page_discovered_at = datetime.now(timezone.utc)
                    return url
            except httpx.HTTPError:
                continue

        # No news page found
        logger.debug("No news page found for %s (%s)", company.name, company.domain)
        return None

    def discover_all(
        self, user_id: str, limit: int | None = None, dry_run: bool = False
    ) -> dict:
        """
        Run discovery for all companies without a news_page_url.

        Fetches company IDs first, then processes each with a fresh DB query
        to avoid Supabase connection timeouts on long-running operations.

        Returns stats dict: {total, discovered, failed, skipped}
        """
        # Fetch only IDs to avoid holding ORM objects across long HTTP operations
        query = (
            self.db.query(Company.id)
            .filter(
                Company.user_id == user_id,
                Company.domain.isnot(None),
                Company.domain != "",
                or_(Company.news_page_url.is_(None), Company.news_page_url == ""),
                Company.news_scrape_enabled.is_(True),
            )
            .order_by(Company.name)
        )

        if limit:
            query = query.limit(limit)

        company_ids = [row[0] for row in query.all()]
        stats = {"total": len(company_ids), "discovered": 0, "failed": 0, "skipped": 0}

        for i, company_id in enumerate(company_ids):
            # Fresh query per company to avoid stale connections
            company = self.db.get(Company, company_id)
            if not company:
                stats["skipped"] += 1
                continue

            logger.info(
                "[%d/%d] Discovering news page for %s (%s)",
                i + 1,
                stats["total"],
                company.name,
                company.domain,
            )

            result = self.discover_for_company(company, dry_run=dry_run)
            if result:
                stats["discovered"] += 1
            else:
                stats["failed"] += 1
                if not dry_run:
                    company.news_scrape_enabled = False

            # Commit per-company to avoid Supabase connection timeout
            if not dry_run:
                try:
                    self.db.commit()
                except Exception:
                    logger.warning("DB commit failed for %s, reconnecting", company.name)
                    self.db.rollback()
                    # Force session to reconnect on next use
                    self.db.close()

            # Brief pause between companies
            time.sleep(0.5)

        logger.info("Discovery complete: %s", stats)
        return stats
