"""
News scraping service.

Fetches news pages from company websites and RSS feeds,
parses article metadata, and stores new items in the database.
"""

import hashlib
import time
import uuid
from datetime import UTC

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.company import Company
from src.models.company_news import CompanyNewsItem
from src.services.news.feeds import RSS_FEEDS, WEB_FEEDS
from src.services.news.parser import NewsPageParser

logger = get_logger(__name__)


class NewsScraperService:
    """Scrapes company news pages and RSS feeds for new articles."""

    def __init__(self, db: Session):
        self.db = db
        self.parser = NewsPageParser()
        self.client = httpx.Client(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-NewsBot/1.0)"},
        )

    def close(self):
        self.client.close()

    def scrape_company(self, company: Company, user_id: str) -> list[dict]:
        """
        Fetch a company's news page and extract article metadata.
        Returns list of article dicts.
        """
        if not company.news_page_url:
            return []

        try:
            resp = self.client.get(company.news_page_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch %s: %s", company.news_page_url, e)
            return []

        # Check if content changed since last scrape
        html_hash = hashlib.sha256(resp.text.encode()).hexdigest()

        # Parse articles
        articles = self.parser.parse(resp.text, company.news_page_url)
        logger.debug("Parsed %d articles from %s", len(articles), company.name)

        # Store each article (dedup by unique constraint)
        new_count = 0
        for article in articles:
            if not article.get("url") or not article.get("title"):
                continue

            stmt = (
                pg_insert(CompanyNewsItem)
                .values(
                    id=uuid.uuid4(),
                    user_id=user_id,
                    company_id=company.id,
                    source_url=article["url"],
                    source_type="company_website",
                    title=article["title"][:500],
                    summary=article.get("snippet", "")[:2000] or None,
                    published_at=article.get("published_at"),
                    raw_html_hash=html_hash,
                    status="new",
                )
                .on_conflict_do_nothing(constraint="uq_company_news_source")
            )

            result = self.db.execute(stmt)
            if result.rowcount > 0:
                new_count += 1

        self.db.commit()
        return articles

    def scrape_all_companies(self, user_id: str) -> dict:
        """
        Scrape all enabled company news pages.
        Returns stats: {companies_scraped, new_items, errors}
        """
        companies = (
            self.db.query(Company)
            .filter(
                Company.user_id == user_id,
                Company.news_scrape_enabled.is_(True),
                Company.news_page_url.isnot(None),
            )
            .all()
        )

        stats = {"companies_scraped": 0, "new_items": 0, "errors": 0}

        for company in companies:
            try:
                articles = self.scrape_company(company, user_id)
                stats["companies_scraped"] += 1
                stats["new_items"] += len(articles)
            except Exception:
                logger.exception("Error scraping %s", company.name)
                stats["errors"] += 1

            time.sleep(1.0)  # Rate limit between companies

        logger.info("Company scraping complete: %s", stats)
        return stats

    def _build_company_lookup(self, user_id: str) -> dict[str, Company]:
        """
        Build a lookup dict mapping name variants to Company objects.

        Includes full names, domains, aliases, and shortened names
        (strips common suffixes like Corp., Inc., LLC, etc.).
        """
        companies = self.db.query(Company).filter(Company.user_id == user_id).all()

        lookup: dict[str, Company] = {}
        # Common English words that are also company names — skip these to avoid
        # false positives in article text matching
        _stopwords = {
            "target",
            "columbia",
            "summit",
            "frontier",
            "core",
            "compass",
            "legacy",
            "pinnacle",
            "premier",
            "sterling",
            "venture",
            "delta",
            "granite",
            "united",
            "national",
            "american",
            "pacific",
            "western",
            "southern",
            "central",
            "modern",
            "royal",
            "global",
            "metro",
            "universal",
            "general",
            "continental",
            "standard",
            "classic",
            "executive",
            "commercial",
            # Multi-word names that match common construction phrases
            "terminal construction",
            "buffalo construction",
            "construction partners",
            "performance contracting",
        }
        # Common suffixes to strip for fuzzy name matching
        _suffixes = [
            " - hq",
            " - headquarters",
            " inc.",
            " inc",
            " corp.",
            " corp",
            " llc",
            " llp",
            " ltd",
            " co.",
            " co",
            " group",
            " construction",
            " builders",
            " building",
            " services",
            " management",
            " contracting",
            " company",
            " corporation",
        ]

        for c in companies:
            name_lower = c.name.lower()
            # Skip full names that are just a stopword (e.g. "Target")
            if name_lower not in _stopwords:
                lookup[name_lower] = c

            # Generate shortened name by stripping suffixes
            short = name_lower
            for suffix in _suffixes:
                if short.endswith(suffix):
                    short = short[: -len(suffix)].strip()
            # Only add short name if it's meaningfully different, long enough,
            # and not a common English word that would cause false positives
            if short != name_lower and len(short) > 6 and short not in _stopwords:
                lookup[short] = c

            if c.domain:
                lookup[c.domain.lower()] = c
            if c.aliases:
                for alias in c.aliases:
                    lookup[alias.lower()] = c

        return lookup

    def _match_article_to_company(
        self, title: str, snippet: str, lookup: dict[str, Company]
    ) -> Company | None:
        """Match article text against company lookup, longest match first.

        Uses word-boundary matching to avoid false positives where short
        company names (e.g. "power") match common English words.
        """
        import re

        text = f"{title} {snippet}".lower()
        # Sort by key length descending to prefer longer (more specific) matches
        for name in sorted(lookup, key=len, reverse=True):
            if len(name) <= 3:
                continue
            # Skip domain-style keys for text matching
            if "." in name and "/" not in name:
                continue
            # Word boundary match to prevent "build" matching "builders"
            pattern = r"\b" + re.escape(name) + r"\b"
            if re.search(pattern, text):
                return lookup[name]
        return None

    def scrape_google_news_per_company(self, user_id: str) -> dict:
        """
        For each company, fetch a targeted Google News RSS feed using the company name.
        This catches news for companies where we couldn't find a direct news page.
        """
        try:
            import feedparser
        except ImportError:
            logger.warning("feedparser not installed, skipping Google News per-company")
            return {"companies_checked": 0, "new_items": 0}

        from urllib.parse import quote

        # Get all companies (not just ones with news pages)
        companies = self.db.query(Company.id, Company.name).filter(Company.user_id == user_id).all()

        stats = {"companies_checked": 0, "new_items": 0}

        for company_id, company_name in companies:
            # Build company-specific Google News RSS URL
            # Use exact phrase match for multi-word names
            search_name = company_name.split(" - ")[0].strip()  # Remove " - HQ" suffixes
            # Remove common suffixes that would narrow results too much
            for suffix in [" Inc.", " Inc", " LLC", " Ltd", " Corp.", " Corp", " Co."]:
                if search_name.endswith(suffix):
                    search_name = search_name[: -len(suffix)].strip()

            if len(search_name) < 4:
                continue

            encoded = quote(f'"{search_name}"')
            feed_url = (
                f"https://news.google.com/rss/search?"
                f"q={encoded}+construction&hl=en-US&gl=US&ceid=US:en&when=3d"
            )

            try:
                feed = feedparser.parse(feed_url)
            except Exception:
                continue

            stats["companies_checked"] += 1

            for entry in feed.entries[:10]:  # Max 10 articles per company
                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                published = entry.get("published", "")

                if not title or not link:
                    continue

                published_at = None
                if published:
                    try:
                        from dateutil import parser as dateutil_parser

                        published_at = dateutil_parser.parse(published)
                    except (ValueError, OverflowError):
                        pass

                # Skip articles older than 30 days — Google News RSS
                # sometimes returns stale results despite the when=3d filter
                if published_at:
                    from datetime import datetime

                    age_days = (datetime.now(UTC) - published_at).days
                    if age_days > 30:
                        continue

                stmt = (
                    pg_insert(CompanyNewsItem)
                    .values(
                        id=uuid.uuid4(),
                        user_id=user_id,
                        company_id=company_id,
                        source_url=link[:2048],
                        source_type="google_news",
                        title=title[:500],
                        summary=summary[:2000] or None,
                        published_at=published_at,
                        status="new",
                    )
                    .on_conflict_do_nothing(constraint="uq_company_news_source")
                )

                result = self.db.execute(stmt)
                if result.rowcount > 0:
                    stats["new_items"] += 1

            try:
                self.db.commit()
            except Exception:
                logger.warning("Commit failed for %s, rolling back", company_name)
                self.db.rollback()
                self.db.close()

            time.sleep(0.5)  # Rate limit Google News

        logger.info("Google News per-company scraping complete: %s", stats)
        return stats

    def scrape_rss_feeds(self, user_id: str) -> dict:
        """
        Scrape supplementary RSS feeds and match articles to companies.
        Returns stats: {feeds_scraped, new_items, matched}
        """
        try:
            import feedparser
        except ImportError:
            logger.warning("feedparser not installed, skipping RSS feeds")
            return {"feeds_scraped": 0, "new_items": 0, "matched": 0}

        company_lookup = self._build_company_lookup(user_id)

        stats = {"feeds_scraped": 0, "new_items": 0, "matched": 0}

        for feed_config in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_config["url"])
                stats["feeds_scraped"] += 1
            except Exception:
                logger.exception("Failed to parse RSS feed: %s", feed_config["name"])
                continue

            for entry in feed.entries[:50]:  # Limit per feed
                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                published = entry.get("published", "")

                if not title or not link:
                    continue

                matched_company = self._match_article_to_company(title, summary, company_lookup)
                if not matched_company:
                    continue

                stats["matched"] += 1

                # Parse published date
                published_at = None
                if published:
                    try:
                        from dateutil import parser as dateutil_parser

                        published_at = dateutil_parser.parse(published)
                    except (ValueError, OverflowError):
                        pass

                # Skip articles older than 30 days
                if published_at:
                    from datetime import datetime

                    age_days = (datetime.now(UTC) - published_at).days
                    if age_days > 30:
                        continue

                stmt = (
                    pg_insert(CompanyNewsItem)
                    .values(
                        id=uuid.uuid4(),
                        user_id=user_id,
                        company_id=matched_company.id,
                        source_url=link[:2048],
                        source_type=feed_config["source_type"],
                        title=title[:500],
                        summary=summary[:2000] or None,
                        published_at=published_at,
                        status="new",
                    )
                    .on_conflict_do_nothing(constraint="uq_company_news_source")
                )

                result = self.db.execute(stmt)
                if result.rowcount > 0:
                    stats["new_items"] += 1

            self.db.commit()

        logger.info("RSS scraping complete: %s", stats)
        return stats

    def scrape_web_feeds(self, user_id: str) -> dict:
        """
        Scrape industry news websites (HTML, not RSS) and match articles to companies.
        Returns stats: {sites_scraped, articles_found, matched, new_items}
        """
        company_lookup = self._build_company_lookup(user_id)
        stats = {"sites_scraped": 0, "articles_found": 0, "matched": 0, "new_items": 0}

        for feed_config in WEB_FEEDS:
            try:
                resp = self.client.get(feed_config["url"])
                resp.raise_for_status()
                stats["sites_scraped"] += 1
            except httpx.HTTPError:
                logger.exception("Failed to fetch web feed: %s", feed_config["name"])
                continue

            articles = self.parser.parse(resp.text, feed_config["url"])
            stats["articles_found"] += len(articles)

            for article in articles:
                title = article.get("title", "")
                snippet = article.get("snippet", "")
                url = article.get("url", "")

                if not title or not url:
                    continue

                matched_company = self._match_article_to_company(title, snippet, company_lookup)
                if not matched_company:
                    continue

                stats["matched"] += 1

                stmt = (
                    pg_insert(CompanyNewsItem)
                    .values(
                        id=uuid.uuid4(),
                        user_id=user_id,
                        company_id=matched_company.id,
                        source_url=url[:2048],
                        source_type=feed_config["source_type"],
                        title=title[:500],
                        summary=snippet[:2000] or None,
                        published_at=article.get("published_at"),
                        status="new",
                    )
                    .on_conflict_do_nothing(constraint="uq_company_news_source")
                )

                result = self.db.execute(stmt)
                if result.rowcount > 0:
                    stats["new_items"] += 1

            self.db.commit()

        logger.info("Web feed scraping complete: %s", stats)
        return stats
