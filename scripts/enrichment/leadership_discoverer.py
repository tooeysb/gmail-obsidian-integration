#!/usr/bin/env python3
"""
Automated company leadership discovery via hybrid HTTP + browser scraping.

For each company in the CRM that has a domain but hasn't been scraped yet:
1. Check if company already has a leadership_page_url (from URL discoverer)
2. If not, try common URL patterns via HTTP
3. Scrape the leadership page: HTTP + BeautifulSoup first, Playwright fallback
4. Extract leaders: name, title, photo, bio, credentials
5. Generate email guesses and add as contacts (contact_source="website")
6. Update company with leadership_page_url + leadership_scraped_at

Usage:
    python -m scripts.enrichment.leadership_discoverer              # Full run
    python -m scripts.enrichment.leadership_discoverer --dry-run    # Preview only
    python -m scripts.enrichment.leadership_discoverer --limit 5    # Process N companies
    python -m scripts.enrichment.leadership_discoverer --http-only  # Skip Playwright fallback
    python -m scripts.enrichment.leadership_discoverer --browser-only  # Playwright only
    python -m scripts.enrichment.leadership_discoverer --rescrape   # Re-process scraped companies
    python -m scripts.enrichment.leadership_discoverer --resume     # Resume from saved state
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

# Ensure project root is on PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env before reading env vars
from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

import httpx  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

from scripts.enrichment.crm_client import CRMClient  # noqa: E402
from scripts.enrichment.human_behavior import (  # noqa: E402
    WorkSchedule,
    delay_between_profiles,
    delay_page_load,
)
from src.core.config import settings  # noqa: E402
from src.core.logging import get_logger  # noqa: E402

logger = get_logger(__name__)

API_BASE = os.environ.get("ENRICHMENT_API_BASE", "https://crm-hth-0f0e9a31256d.herokuapp.com")
API_KEY = os.environ.get("ENRICHMENT_API_KEY", "")

STATE_FILE = PROJECT_ROOT / ".leadership_state.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Common leadership page URL patterns
LEADERSHIP_URL_PATTERNS = [
    "/about/leadership",
    "/about-us/leadership",
    "/leadership",
    "/senior-leadership",
    "/about/senior-leadership",
    "/about/executive-leadership",
    "/our-leadership",
    "/our-team",
    "/about/team",
    "/about-us/our-team",
    "/about/our-leadership",
    "/about/our-team",
    "/about/executives",
    "/about/management",
    "/about/management-team",
    "/people",
    "/our-people",
    "/about/our-people",
    "/team",
    "/who-we-are",
    "/who-we-are/leadership",
    "/company/leadership",
    "/about-us/our-leadership-team",
    "/about/leadership-team",
    "/why-us/leadership",
]

# Name patterns to exclude (not real people)
EXCLUDE_PATTERNS = re.compile(
    r"(cookie|privacy|contact us|learn more|read more|view all|see all|"
    r"©|copyright|\d{4}|terms|careers|join|subscribe|sign up|log in|"
    r"menu|search|home|about|news|back to|share|print|download)",
    re.IGNORECASE,
)

# Common executive title keywords
TITLE_KEYWORDS = re.compile(
    r"(president|chief|ceo|cfo|coo|cto|cio|cmo|cpo|evp|svp|"
    r"vice president|vp|director|head of|managing|partner|founder|"
    r"general manager|executive|officer|principal|senior|superintendent|"
    r"project executive|regional|division|operations manager|estimat)",
    re.IGNORECASE,
)

# CSS class patterns for leadership cards
CARD_CLASS_PATTERNS = [
    "team-member",
    "leadership-card",
    "executive-card",
    "person-card",
    "staff-member",
    "bio-card",
    "leader",
    "team-member",
    "executive",
    "person",
    "staff",
    "profile",
    "member",
]

# Professional credentials regex
CREDENTIAL_PATTERNS = re.compile(
    r"\b(PE|AIA|FAIA|LEED\s*AP|PMP|CPA|DBIA|CCM|"
    r"OSHA|CSP|ASP|CIH|MBA|PhD|JD|P\.?Eng?\.?|RA|SE|"
    r"CPSM|AICP|CFM|CFCI|NICET|CPC)\b",
    re.IGNORECASE,
)

# University/school patterns
EDUCATION_PATTERNS = re.compile(
    r"(?:University|College|Institute|School)\s+of\s+[\w\s]+|"
    r"(?:MIT|Stanford|Harvard|Georgia Tech|Purdue|Cornell|UCLA|USC|"
    r"UT Austin|Texas A&M|Virginia Tech|Cal Poly|Penn State|"
    r"Notre Dame|Duke|Carnegie Mellon|UC Berkeley|Michigan State|"
    r"Clemson|Auburn|Florida State|Ohio State|Illinois|Wisconsin|"
    r"Iowa State|Kansas State|NC State|Oregon State)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class LeaderPerson:
    """Extracted leader from a company website."""

    name: str
    title: str | None = None
    photo_url: str | None = None
    bio_text: str | None = None
    detail_page_url: str | None = None
    email_guesses: list[str] = field(default_factory=list)
    credentials: list[str] = field(default_factory=list)
    education: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _looks_like_person_name(text: str) -> bool:
    """Heuristic: check if text looks like a person's name."""
    text = text.strip()
    # Strip common markdown/formatting
    text = re.sub(r"[*_`]", "", text).strip()
    if not text or len(text) < 3 or len(text) > 60:
        return False
    if EXCLUDE_PATTERNS.search(text):
        return False
    # Must have at least first + last name
    parts = text.split()
    if len(parts) < 2 or len(parts) > 5:
        return False
    # Each part should start with uppercase
    if not all(p[0].isupper() for p in parts if p):
        return False
    # No digits in names
    if any(c.isdigit() for c in text):
        return False
    return True


def _generate_email_guesses(name: str, domain: str) -> list[str]:
    """Generate common email patterns from name + domain."""
    parts = name.lower().split()
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


def _extract_metadata_from_bio(bio_text: str) -> dict:
    """Extract credentials and education from bio text."""
    credentials = list(set(CREDENTIAL_PATTERNS.findall(bio_text)))
    education = list(set(EDUCATION_PATTERNS.findall(bio_text)))
    return {
        "credentials": credentials or [],
        "education": education or [],
    }


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# State management (resume support)
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
# HTTP scraping (BeautifulSoup)
# ---------------------------------------------------------------------------


def _is_js_heavy(soup: BeautifulSoup) -> bool:
    """Detect if page likely needs JavaScript rendering."""
    # Check for common SPA indicators
    noscript = soup.find("noscript")
    if noscript:
        noscript_text = noscript.get_text().lower()
        if "javascript" in noscript_text or "enable" in noscript_text:
            return True

    # Very little visible text vs script content
    scripts = soup.find_all("script")
    script_size = sum(len(s.get_text()) for s in scripts)
    body = soup.find("body")
    body_text_size = len(body.get_text(strip=True)) if body else 0
    if body_text_size < 500 and script_size > 2000:
        return True

    # React/Vue/Angular root with minimal children
    root_ids = ["root", "app", "__next", "__nuxt"]
    for rid in root_ids:
        el = soup.find(id=rid)
        if el and len(list(el.children)) <= 2:
            return True

    return False


def _extract_photo_from_card(card, base_url: str) -> str | None:
    """Extract person photo URL from a leadership card."""
    img = card.find("img")
    if not img:
        return None

    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
    if not src:
        return None

    # Filter out icons, logos, tiny placeholders
    width = img.get("width", "")
    height = img.get("height", "")
    if width and str(width).isdigit() and int(width) < 50:
        return None
    if height and str(height).isdigit() and int(height) < 50:
        return None

    # Skip SVGs and data URIs
    if src.startswith("data:") or src.endswith(".svg"):
        return None

    return urljoin(base_url, src)


def _extract_detail_url_from_card(card, base_url: str) -> str | None:
    """Extract a link to an individual bio page from the card."""
    link = card.find("a", href=True)
    if link:
        href = link["href"]
        if href and not href.startswith("#") and not href.startswith("mailto:"):
            return urljoin(base_url, href)
    return None


def _extract_bio_from_card(card) -> str | None:
    """Extract biographical text from a card (beyond just name/title)."""
    paragraphs = card.find_all("p")
    for p in paragraphs:
        text = p.get_text(strip=True)
        if len(text) > 50 and not EXCLUDE_PATTERNS.search(text):
            return text[:500]
    return None


def _parse_card_bs4(card, base_url: str) -> LeaderPerson | None:
    """Parse a leadership card using BeautifulSoup."""
    text_parts = card.get_text(separator="\n").strip().split("\n")
    text_parts = [t.strip() for t in text_parts if t.strip()]

    name = None
    title = None

    for part in text_parts:
        # Strip markdown formatting
        part = re.sub(r"[*_`]", "", part).strip()
        if not name and _looks_like_person_name(part):
            name = part
        elif name and not title and TITLE_KEYWORDS.search(part):
            title = part[:255]
            break

    if not name:
        return None
    if not title:
        return None

    photo_url = _extract_photo_from_card(card, base_url)
    detail_url = _extract_detail_url_from_card(card, base_url)
    bio_text = _extract_bio_from_card(card)

    person = LeaderPerson(
        name=name,
        title=title,
        photo_url=photo_url,
        detail_page_url=detail_url,
        bio_text=bio_text,
    )

    if bio_text:
        meta = _extract_metadata_from_bio(bio_text)
        person.credentials = meta["credentials"]
        person.education = meta["education"]

    return person


def scrape_http(url: str, http_client: httpx.Client) -> tuple[list[LeaderPerson], bool]:
    """
    Scrape a leadership page via HTTP + BeautifulSoup.

    Returns (leaders, is_js_heavy). If is_js_heavy is True and leaders are empty,
    the caller should try Playwright.
    """
    try:
        resp = http_client.get(url)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("HTTP fetch failed for %s: %s", url, e)
        return [], True  # Assume JS-heavy if we can't fetch

    content_type = resp.headers.get("content-type", "")
    if "text/html" not in content_type:
        return [], False

    soup = BeautifulSoup(resp.text, "lxml")
    js_heavy = _is_js_heavy(soup)
    base_url = str(resp.url)

    # Strategy 1: CSS class-based cards
    for pattern in CARD_CLASS_PATTERNS:
        cards = soup.find_all(class_=re.compile(pattern, re.I))
        if len(cards) >= 2:
            results = []
            for card in cards:
                person = _parse_card_bs4(card, base_url)
                if person:
                    results.append(person)
            if results:
                logger.info(
                    "HTTP extracted %d leaders from cards (class~%s) on %s",
                    len(results),
                    pattern,
                    url,
                )
                return results, js_heavy

    # Strategy 2: Headings with person names + sibling titles
    results = _extract_from_headings_bs4(soup, base_url)
    if results:
        logger.info("HTTP extracted %d leaders from headings on %s", len(results), url)
        return results, js_heavy

    # Strategy 3: List/grid items
    results = _extract_from_list_items_bs4(soup, base_url)
    if results:
        logger.info("HTTP extracted %d leaders from list items on %s", len(results), url)
        return results, js_heavy

    return [], js_heavy


def _extract_from_headings_bs4(soup: BeautifulSoup, base_url: str) -> list[LeaderPerson]:
    """Extract name-title pairs from headings and siblings."""
    results = []
    headings = soup.find_all(["h2", "h3", "h4", "h5"])

    for heading in headings:
        name_text = heading.get_text(strip=True)
        # Strip formatting
        name_text = re.sub(r"[*_`]", "", name_text).strip()
        if not _looks_like_person_name(name_text):
            continue

        # Look at sibling elements for title
        title = None
        for sibling in heading.find_next_siblings():
            if sibling.name in ("h2", "h3", "h4", "h5"):
                break  # Hit next heading, stop
            sib_text = sibling.get_text(strip=True)
            if sib_text and TITLE_KEYWORDS.search(sib_text):
                title = sib_text[:255]
                break

        if title:
            # Try to get photo from parent container
            parent = heading.parent
            photo_url = _extract_photo_from_card(parent, base_url) if parent else None
            detail_url = _extract_detail_url_from_card(parent, base_url) if parent else None
            bio_text = _extract_bio_from_card(parent) if parent else None

            person = LeaderPerson(
                name=name_text,
                title=title,
                photo_url=photo_url,
                detail_page_url=detail_url,
                bio_text=bio_text,
            )
            if bio_text:
                meta = _extract_metadata_from_bio(bio_text)
                person.credentials = meta["credentials"]
                person.education = meta["education"]
            results.append(person)

    return results


def _extract_from_list_items_bs4(soup: BeautifulSoup, base_url: str) -> list[LeaderPerson]:
    """Extract from list items containing name + title patterns."""
    results = []
    items = soup.select("li, .grid > div, .row > div, .columns > div")

    for item in items:
        text = item.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if len(lines) < 2:
            continue

        name = None
        title = None
        for line in lines[:3]:
            line = re.sub(r"[*_`]", "", line).strip()
            if not name and _looks_like_person_name(line):
                name = line
            elif name and not title and TITLE_KEYWORDS.search(line):
                title = line[:255]
                break

        if name and title:
            photo_url = _extract_photo_from_card(item, base_url)
            detail_url = _extract_detail_url_from_card(item, base_url)
            bio_text = _extract_bio_from_card(item)

            person = LeaderPerson(
                name=name,
                title=title,
                photo_url=photo_url,
                detail_page_url=detail_url,
                bio_text=bio_text,
            )
            if bio_text:
                meta = _extract_metadata_from_bio(bio_text)
                person.credentials = meta["credentials"]
                person.education = meta["education"]
            results.append(person)

    return results


# ---------------------------------------------------------------------------
# Playwright scraping (fallback for JS-heavy pages)
# ---------------------------------------------------------------------------


class LeadershipScraper:
    """Browser-based leadership page scraper (Playwright fallback)."""

    def __init__(self, headless: bool = False):
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._headless = headless
        self._started = False

    def start(self):
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self._headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=USER_AGENT,
        )
        self._page = self._context.new_page()
        self._started = True
        logger.info("Leadership scraper browser started")

    def stop(self):
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        self._started = False
        logger.info("Leadership scraper browser stopped")

    @property
    def is_started(self) -> bool:
        return self._started

    def find_leadership_page(self, company_name: str, domain: str) -> str | None:
        """Search Google for a company's leadership/team page."""
        page = self._page
        query = f'site:{domain} "{company_name}" leadership OR team OR executives OR "our people"'
        page.goto(
            f"https://www.google.com/search?q={query}",
            wait_until="domcontentloaded",
        )
        delay_page_load()

        links = page.query_selector_all("a[href]")
        for link in links:
            href = link.get_attribute("href") or ""
            if domain in href and any(
                kw in href.lower()
                for kw in ["leader", "team", "people", "executive", "management", "about"]
            ):
                logger.info("Found leadership page via Google: %s", href)
                return href

        # Fallback: try common URL patterns directly
        for pattern in LEADERSHIP_URL_PATTERNS:
            url = f"https://www.{domain}{pattern}"
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=10000)
                if resp and resp.status == 200:
                    text = page.inner_text("body")
                    if TITLE_KEYWORDS.search(text):
                        logger.info("Found leadership page at known pattern: %s", url)
                        return url
            except Exception:
                continue

        logger.warning("No leadership page found for %s (%s)", company_name, domain)
        return None

    def scrape_leadership_page(self, url: str) -> list[LeaderPerson]:
        """Visit a leadership page with Playwright and extract leaders."""
        page = self._page
        results = []

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15000)
            delay_page_load()
        except Exception as e:
            logger.error("Failed to load leadership page %s: %s", url, e)
            return results

        base_url = page.url

        # Strategy 1: CSS class cards
        card_selectors = [
            ".team-member",
            ".leadership-card",
            ".executive-card",
            ".person-card",
            ".staff-member",
            ".bio-card",
            "[class*='leader']",
            "[class*='team-member']",
            "[class*='executive']",
            "[class*='person']",
            "[class*='staff']",
        ]

        for selector in card_selectors:
            cards = page.query_selector_all(selector)
            if len(cards) >= 2:
                for card in cards:
                    person = self._extract_person_from_card(card, base_url)
                    if person:
                        results.append(person)
                if results:
                    logger.info(
                        "Browser extracted %d leaders from cards (%s) on %s",
                        len(results),
                        selector,
                        url,
                    )
                    return results

        # Strategy 2: Headings + siblings
        results = self._extract_from_headings(page, base_url)
        if results:
            logger.info("Browser extracted %d leaders from headings on %s", len(results), url)
            return results

        # Strategy 3: List items
        results = self._extract_from_list_items(page, base_url)
        if results:
            logger.info("Browser extracted %d leaders from list items on %s", len(results), url)

        return results

    def _extract_person_from_card(self, card, base_url: str) -> LeaderPerson | None:
        """Extract leader data from a Playwright card element."""
        text_parts = card.inner_text().strip().split("\n")
        text_parts = [t.strip() for t in text_parts if t.strip()]

        name = None
        title = None

        for part in text_parts:
            part = re.sub(r"[*_`]", "", part).strip()
            if not name and _looks_like_person_name(part):
                name = part
            elif name and not title and TITLE_KEYWORDS.search(part):
                title = part[:255]
                break

        if not name or not title:
            return None

        # Extract photo
        photo_url = None
        img = card.query_selector("img")
        if img:
            src = img.get_attribute("src") or img.get_attribute("data-src") or ""
            if src and not src.startswith("data:") and not src.endswith(".svg"):
                photo_url = urljoin(base_url, src)

        # Extract detail URL
        detail_url = None
        link = card.query_selector("a[href]")
        if link:
            href = link.get_attribute("href") or ""
            if href and not href.startswith("#") and not href.startswith("mailto:"):
                detail_url = urljoin(base_url, href)

        # Extract bio
        bio_text = None
        p_tag = card.query_selector("p")
        if p_tag:
            p_text = p_tag.inner_text().strip()
            if len(p_text) > 50 and not EXCLUDE_PATTERNS.search(p_text):
                bio_text = p_text[:500]

        person = LeaderPerson(
            name=name,
            title=title,
            photo_url=photo_url,
            detail_page_url=detail_url,
            bio_text=bio_text,
        )

        if bio_text:
            meta = _extract_metadata_from_bio(bio_text)
            person.credentials = meta["credentials"]
            person.education = meta["education"]

        return person

    def _extract_from_headings(self, page, base_url: str) -> list[LeaderPerson]:
        """Extract name-title pairs from headings and their siblings."""
        results = []
        headings = page.query_selector_all("h2, h3, h4, h5")

        for heading in headings:
            name_text = heading.inner_text().strip()
            name_text = re.sub(r"[*_`]", "", name_text).strip()
            if not _looks_like_person_name(name_text):
                continue

            sibling = heading.evaluate(
                """el => {
                    let next = el.nextElementSibling;
                    if (next) return next.innerText;
                    let parent = el.parentElement;
                    if (parent) {
                        let texts = Array.from(parent.querySelectorAll('p, span, div'))
                            .map(e => e.innerText.trim())
                            .filter(t => t && t !== el.innerText.trim());
                        return texts.join('\\n');
                    }
                    return '';
                }"""
            )

            if sibling:
                for line in sibling.split("\n"):
                    line = line.strip()
                    if line and TITLE_KEYWORDS.search(line):
                        results.append(
                            LeaderPerson(name=name_text, title=line[:255])
                        )
                        break

        return results

    def _extract_from_list_items(self, page, base_url: str) -> list[LeaderPerson]:
        """Extract from list items that contain name + title."""
        results = []
        items = page.query_selector_all("li, .grid > div, .row > div")

        for item in items:
            text = item.inner_text().strip()
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            if len(lines) < 2:
                continue

            name = None
            title = None
            for line in lines[:3]:
                line = re.sub(r"[*_`]", "", line).strip()
                if not name and _looks_like_person_name(line):
                    name = line
                elif name and not title and TITLE_KEYWORDS.search(line):
                    title = line[:255]
                    break

            if name and title:
                results.append(LeaderPerson(name=name, title=title))

        return results


# ---------------------------------------------------------------------------
# URL discovery via HTTP (fast, no browser needed)
# ---------------------------------------------------------------------------


def discover_url_http(domain: str, http_client: httpx.Client) -> str | None:
    """Try common leadership URL patterns via HTTP HEAD/GET."""
    for pattern in LEADERSHIP_URL_PATTERNS:
        url = f"https://www.{domain}{pattern}"
        try:
            resp = http_client.head(url, follow_redirects=True)
            if resp.status_code == 200:
                # Verify it has leadership content
                full_resp = http_client.get(url)
                if full_resp.status_code == 200 and TITLE_KEYWORDS.search(full_resp.text):
                    logger.info("Found leadership page at %s", url)
                    return url
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------


def process_company(
    company: dict,
    http_client: httpx.Client,
    crm: CRMClient,
    scraper: LeadershipScraper | None = None,
    dry_run: bool = False,
    http_only: bool = False,
    browser_only: bool = False,
) -> int:
    """
    Discover leaders for a single company using hybrid approach.

    Returns number of contacts added.
    """
    company_name = company["name"]
    domain = company["domain"]
    company_id = company["id"]

    logger.info("Processing company: %s (%s)", company_name, domain)

    # Step 1: Get leadership page URL
    leadership_url = company.get("leadership_page_url")

    if not leadership_url:
        # Try HTTP URL patterns first (fast)
        if not browser_only:
            leadership_url = discover_url_http(domain, http_client)

        # Fallback to browser Google search
        if not leadership_url and scraper and scraper.is_started and not http_only:
            leadership_url = scraper.find_leadership_page(company_name, domain)

    if not leadership_url:
        logger.info("No leadership page found for %s", company_name)
        if not dry_run:
            crm.update_company(
                company_id,
                leadership_scraped_at=_now_iso(),
            )
        return 0

    # Step 2: Scrape the leadership page
    leaders: list[LeaderPerson] = []

    if not browser_only:
        # Try HTTP + BeautifulSoup first
        leaders, js_heavy = scrape_http(leadership_url, http_client)

        if not leaders and js_heavy and scraper and scraper.is_started and not http_only:
            # Fallback to Playwright for JS-heavy pages
            logger.info("JS-heavy page detected, falling back to Playwright: %s", leadership_url)
            leaders = scraper.scrape_leadership_page(leadership_url)
    elif scraper and scraper.is_started:
        # Browser-only mode
        leaders = scraper.scrape_leadership_page(leadership_url)

    if not leaders:
        logger.info("No leaders extracted from %s", leadership_url)
        if not dry_run:
            crm.update_company(
                company_id,
                leadership_page_url=leadership_url,
                leadership_scraped_at=_now_iso(),
            )
        return 0

    logger.info("Found %d leaders on %s", len(leaders), leadership_url)

    # Step 3: Get existing contacts for name dedup
    existing_names: set[str] = set()
    if not dry_run:
        try:
            detail = crm.get_company_detail(company_id)
            for c in detail.get("contacts", []):
                if c.get("name"):
                    existing_names.add(c["name"].lower().strip())
        except Exception as e:
            logger.warning("Could not fetch existing contacts for dedup: %s", e)

    # Step 4: Generate emails and add as contacts
    added = 0
    for leader in leaders:
        # Name dedup
        if leader.name.lower().strip() in existing_names:
            logger.info("Skipping duplicate: %s (already at company)", leader.name)
            continue

        # Generate email guesses
        email_guesses = _generate_email_guesses(leader.name, domain)
        if not email_guesses:
            logger.warning("Could not generate email for %s", leader.name)
            continue
        leader.email_guesses = email_guesses

        email = email_guesses[0]

        if dry_run:
            logger.info(
                "[DRY RUN] Would add: %s (%s) — %s | photo=%s",
                leader.name,
                leader.title,
                email,
                "yes" if leader.photo_url else "no",
            )
            added += 1
            continue

        try:
            result = crm.add_contact_to_company(
                company_id=company_id,
                email=email,
                name=leader.name,
                title=leader.title,
                contact_source="website",
            )
            if result.get("created"):
                logger.info(
                    "Added contact: %s (%s) at %s",
                    leader.name,
                    leader.title,
                    company_name,
                )
                added += 1

                # Update source_data with scraped metadata
                source_data = {
                    "scraped_from": leadership_url,
                    "scraped_at": _now_iso(),
                    "all_email_guesses": email_guesses,
                }
                if leader.photo_url:
                    source_data["photo_url"] = leader.photo_url
                if leader.bio_text:
                    source_data["bio_text"] = leader.bio_text
                if leader.detail_page_url:
                    source_data["detail_page_url"] = leader.detail_page_url
                if leader.credentials:
                    source_data["credentials"] = leader.credentials
                if leader.education:
                    source_data["education"] = leader.education

                contact_id = result["contact"]["id"]
                try:
                    crm.update_contact(contact_id, source_data=source_data)
                except Exception as e:
                    logger.warning("Could not update source_data for %s: %s", leader.name, e)
            else:
                logger.info("Contact already exists: %s (%s)", leader.name, email)
        except Exception as e:
            logger.error("Failed to add contact %s: %s", leader.name, e)

    # Step 5: Update company
    if not dry_run:
        crm.update_company(
            company_id,
            leadership_page_url=leadership_url,
            leadership_scraped_at=_now_iso(),
        )

    return added


def main():
    parser = argparse.ArgumentParser(description="Company leadership discovery (hybrid)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API updates")
    parser.add_argument("--limit", type=int, default=0, help="Max companies to process (0=all)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--no-schedule", action="store_true", help="Skip work schedule")
    parser.add_argument("--http-only", action="store_true", help="HTTP+BS4 only, no Playwright")
    parser.add_argument(
        "--browser-only", action="store_true", help="Playwright only, no HTTP (original behavior)"
    )
    parser.add_argument(
        "--rescrape", action="store_true", help="Re-process companies already scraped"
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry GC/SC companies that were scraped but no leadership page found",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from saved state")
    args = parser.parse_args()

    logger.info(
        "Leadership Discoverer starting (dry_run=%s, limit=%s, http_only=%s, browser_only=%s)",
        args.dry_run,
        args.limit,
        args.http_only,
        args.browser_only,
    )

    # Initialize work schedule
    schedule = WorkSchedule()
    if not args.no_schedule:
        if not schedule.wait_for_work_hours():
            logger.info("Past work hours for today — exiting")
            return

    # Graceful shutdown
    shutdown_requested = False

    def _signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown requested (signal %d)", signum)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Initialize API client
    api_key = API_KEY or settings.secret_key
    if not api_key:
        logger.error("No API key — set ENRICHMENT_API_KEY env var or SECRET_KEY in .env")
        return

    crm = CRMClient(base_url=API_BASE, api_key=api_key)
    http_client = httpx.Client(
        timeout=15.0,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )

    # Only initialize Playwright if needed
    scraper: LeadershipScraper | None = None
    if not args.http_only:
        scraper = LeadershipScraper(headless=args.headless)

    # Load state for resume
    state = _load_state() if args.resume else {
        "processed_ids": [],
        "total_contacts_added": 0,
        "total_errors": 0,
    }
    processed_ids = set(state.get("processed_ids", []))

    try:
        if scraper:
            scraper.start()

        # Fetch companies needing leadership discovery
        if args.retry_failed:
            companies = crm.get_needs_leadership_retry()
            logger.info(
                "Retry-failed mode: %d GC/SC companies to retry with expanded URL patterns",
                len(companies),
            )
            # Clear leadership_scraped_at so process_company tries URL discovery again
            for c in companies:
                c["leadership_scraped_at"] = None
        else:
            companies = crm.get_needs_leadership()
            logger.info("Companies needing leadership discovery: %d", len(companies))

        if args.limit:
            companies = companies[: args.limit]

        total_added = 0
        total_processed = 0

        for company in companies:
            if shutdown_requested:
                logger.info("Shutdown requested — stopping")
                break

            # Skip if already processed in this state
            if args.resume and company["id"] in processed_ids:
                continue

            if not args.no_schedule and not schedule.wait_for_work_hours():
                logger.info("Work day ended — stopping")
                break

            if not args.no_schedule and schedule.should_take_break():
                schedule.take_break()

            try:
                added = process_company(
                    company,
                    http_client,
                    crm,
                    scraper=scraper,
                    dry_run=args.dry_run,
                    http_only=args.http_only,
                    browser_only=args.browser_only,
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

            if not args.no_schedule:
                delay_between_profiles()

        logger.info(
            "Leadership discovery complete: %d companies processed, %d contacts added",
            total_processed,
            total_added,
        )

    finally:
        if scraper:
            scraper.stop()
        http_client.close()
        crm.close()


if __name__ == "__main__":
    main()
