"""
News page HTML parser.

Extracts article links, titles, dates, and snippets from news listing pages.
Handles various common HTML patterns used by construction company websites.
"""

from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.core.logging import get_logger

logger = get_logger(__name__)


def _try_parse_date(text: str) -> datetime | None:
    """Attempt to parse a date string using dateutil."""
    if not text or len(text.strip()) < 4:
        return None
    try:
        from dateutil import parser as dateutil_parser

        return dateutil_parser.parse(text.strip(), fuzzy=True)
    except (ValueError, OverflowError):
        return None


def _extract_text(element: Tag, max_len: int = 300) -> str:
    """Extract cleaned text from an element, truncated to max_len."""
    text = element.get_text(separator=" ", strip=True)
    if len(text) > max_len:
        text = text[:max_len].rsplit(" ", 1)[0] + "..."
    return text


class NewsPageParser:
    """Parses news listing pages and extracts article metadata."""

    def parse(self, html: str, base_url: str) -> list[dict]:
        """
        Parse HTML and extract news article items.

        Returns list of dicts with keys: title, url, published_at, snippet
        """
        soup = BeautifulSoup(html, "lxml")
        articles = []

        # Strategy 1: <article> elements
        articles.extend(self._parse_article_elements(soup, base_url))

        # Strategy 2: Common news container classes/patterns
        if not articles:
            articles.extend(self._parse_news_containers(soup, base_url))

        # Strategy 3: Card-based layouts (div.card with links)
        if not articles:
            articles.extend(self._parse_card_links(soup, base_url))

        # Strategy 4: Links within date-annotated sections
        if not articles:
            articles.extend(self._parse_dated_links(soup, base_url))

        # Deduplicate by URL
        seen_urls = set()
        unique = []
        for article in articles:
            if article["url"] not in seen_urls and article["title"]:
                seen_urls.add(article["url"])
                unique.append(article)

        return unique

    def _parse_article_elements(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Extract articles from <article> HTML elements."""
        results = []
        for article_el in soup.find_all("article", limit=30):
            link = article_el.find("a", href=True)
            if not link:
                continue

            title_el = article_el.find(["h2", "h3", "h4"]) or link
            title = _extract_text(title_el, max_len=200)
            url = urljoin(base_url, link["href"])

            time_el = article_el.find("time")
            published_at = None
            if time_el:
                published_at = _try_parse_date(time_el.get("datetime", "") or time_el.get_text())

            snippet_el = article_el.find(
                ["p", "div"], class_=lambda c: c and "excerpt" in str(c).lower()
            ) or article_el.find("p")
            snippet = _extract_text(snippet_el, max_len=300) if snippet_el else ""

            results.append(
                {
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "snippet": snippet,
                }
            )

        return results

    def _parse_news_containers(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Extract articles from common CSS class patterns."""
        results = []
        news_classes = [
            "news-item",
            "news-card",
            "press-release",
            "post-item",
            "blog-post",
            "article-card",
            "news-entry",
            "media-item",
        ]

        for cls in news_classes:
            target_cls = cls
            items = soup.find_all(
                class_=lambda c, t=target_cls: c and t in str(c).lower(), limit=30
            )
            if not items:
                continue

            for item in items:
                link = item.find("a", href=True)
                if not link:
                    continue

                title_el = item.find(["h2", "h3", "h4"]) or link
                title = _extract_text(title_el, max_len=200)
                url = urljoin(base_url, link["href"])

                # Try to find date
                date_el = item.find("time") or item.find(
                    class_=lambda c: c and "date" in str(c).lower()
                )
                published_at = _try_parse_date(date_el.get_text() if date_el else "")

                snippet_el = item.find("p")
                snippet = _extract_text(snippet_el, max_len=300) if snippet_el else ""

                results.append(
                    {
                        "title": title,
                        "url": url,
                        "published_at": published_at,
                        "snippet": snippet,
                    }
                )

            if results:
                break  # Found articles with this class pattern

        return results

    def _parse_card_links(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Extract articles from card-based layouts (e.g. BldUp, Bootstrap cards)."""
        results = []
        cards = soup.find_all(
            "div",
            class_=lambda c: c and "card" in (c if isinstance(c, list) else [c]),
            limit=50,
        )
        for card in cards:
            link = card.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            # Skip navigation/utility links
            if href in ("#", "/") or len(href) < 5:
                continue

            title_el = card.find(["h2", "h3", "h4", "h5"]) or card.find(
                class_=lambda c: c and "title" in str(c).lower()
            )
            if not title_el:
                title_el = link
            title = _extract_text(title_el, max_len=200)
            if not title or len(title) < 10:
                continue

            url = urljoin(base_url, href)

            date_el = card.find("time") or card.find(
                class_=lambda c: c and "date" in str(c).lower()
            )
            published_at = _try_parse_date(date_el.get_text() if date_el else "")

            snippet_el = card.find("p")
            snippet = _extract_text(snippet_el, max_len=300) if snippet_el else ""

            results.append(
                {
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "snippet": snippet,
                }
            )

        return results

    def _parse_dated_links(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Fallback: find links near date-like text."""
        results = []

        # Look for elements containing date text near links
        for container in soup.find_all(["li", "div", "tr"], limit=50):
            link = container.find("a", href=True)
            if not link:
                continue

            text = container.get_text()
            date = _try_parse_date(text)
            if not date:
                continue

            title = _extract_text(link, max_len=200)
            url = urljoin(base_url, link["href"])

            if title and len(title) > 10:
                results.append(
                    {
                        "title": title,
                        "url": url,
                        "published_at": date,
                        "snippet": "",
                    }
                )

        return results
