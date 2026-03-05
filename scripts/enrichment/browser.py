"""
Playwright-based browser automation for LinkedIn profile extraction.

Uses saved auth state (cookies) to access LinkedIn without locking the user's Chrome profile.
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

from scripts.enrichment.human_behavior import delay_between_clicks, delay_page_load
from src.core.logging import get_logger

logger = get_logger(__name__)

AUTH_STATE_FILE = Path(__file__).parent / ".auth_state.json"


@dataclass
class LinkedInProfile:
    """Extracted LinkedIn profile data."""

    title: str | None = None
    linkedin_url: str | None = None
    company_name: str | None = None
    company_linkedin_url: str | None = None


class LinkedInBrowser:
    """Manages a Playwright browser session for LinkedIn browsing."""

    def __init__(self, headless: bool = False):
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._headless = headless

    # ------------------------------------------------------------------
    # Setup: interactive login to save cookies
    # ------------------------------------------------------------------

    def setup_auth(self):
        """Launch browser for manual LinkedIn login, then save auth state.

        Auto-detects successful login by watching for URL change from /login
        to the LinkedIn feed. Waits up to 5 minutes.
        """
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.goto("https://www.linkedin.com/login")

        logger.info("Please log into LinkedIn in the browser window.")
        logger.info("Waiting for login to complete (up to 5 minutes)...")

        # Wait until the URL is no longer the login page
        page.wait_for_url(
            lambda url: "/login" not in url and "/checkpoint" not in url,
            timeout=300000,
        )
        logger.info("Login detected — saving session cookies...")

        context.storage_state(path=str(AUTH_STATE_FILE))
        logger.info("Auth state saved to %s", AUTH_STATE_FILE)

        context.close()
        browser.close()
        pw.stop()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Launch Chromium with saved LinkedIn session."""
        if not AUTH_STATE_FILE.exists():
            raise FileNotFoundError(
                f"No auth state found at {AUTH_STATE_FILE}. "
                "Run with --setup first to log into LinkedIn."
            )

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        self._context = self._browser.new_context(
            storage_state=str(AUTH_STATE_FILE),
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        # Remove webdriver property that LinkedIn checks
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self._page = self._context.new_page()
        logger.info("Browser started with saved LinkedIn session")

    def stop(self):
        """Close browser and cleanup."""
        if self._context:
            # Save refreshed cookies for next run
            try:
                self._context.storage_state(path=str(AUTH_STATE_FILE))
            except Exception:
                pass
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.info("Browser stopped")

    # ------------------------------------------------------------------
    # Page interaction helpers
    # ------------------------------------------------------------------

    def _scroll_page(self):
        """Scroll down the page like a human would."""
        page = self._page
        scroll_amount = random.randint(300, 700)
        page.mouse.wheel(0, scroll_amount)
        delay_between_clicks()
        # Sometimes scroll a bit more
        if random.random() < 0.3:
            page.mouse.wheel(0, random.randint(200, 500))
            delay_between_clicks()

    def _is_login_page(self) -> bool:
        """Check if LinkedIn redirected to the login page (cookies expired)."""
        url = self._page.url
        return "/login" in url or "/authwall" in url or "/checkpoint" in url

    # ------------------------------------------------------------------
    # Google search
    # ------------------------------------------------------------------

    def search_google_for_linkedin(self, name: str, company: str | None) -> str | None:
        """
        Search Google for a person's LinkedIn profile.

        Returns the first linkedin.com/in/ URL found, or None.
        """
        parts = [name]
        if company:
            parts.append(company)
        parts.append("LinkedIn")
        query = " ".join(parts)

        page = self._page
        page.goto(f"https://www.google.com/search?q={query}", wait_until="domcontentloaded")
        delay_page_load()
        self._scroll_page()

        # Extract LinkedIn profile URLs from search results
        content = page.content()
        # Match linkedin.com/in/ URLs in href attributes or text
        matches = re.findall(r"https?://(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)", content)
        if matches:
            # Deduplicate, take first unique slug
            seen = []
            for slug in matches:
                if slug not in seen:
                    seen.append(slug)
            linkedin_url = f"https://www.linkedin.com/in/{seen[0]}"
            logger.info("Found LinkedIn URL via Google: %s", linkedin_url)
            return linkedin_url

        logger.warning("No LinkedIn profile found for: %s", query)
        return None

    # ------------------------------------------------------------------
    # Profile extraction
    # ------------------------------------------------------------------

    def extract_profile(self, linkedin_url: str) -> LinkedInProfile:
        """Navigate to a LinkedIn profile and extract title + company info."""
        result = LinkedInProfile(linkedin_url=linkedin_url)
        page = self._page

        try:
            page.goto(linkedin_url, wait_until="domcontentloaded", timeout=15000)
            delay_page_load()

            if self._is_login_page():
                logger.error("LinkedIn session expired — re-run with --setup")
                return result

            self._scroll_page()

            # Extract headline/title from the profile
            # LinkedIn puts the headline in a div.text-body-medium below the name
            headline_el = page.query_selector("div.text-body-medium.break-words")
            if headline_el:
                headline = headline_el.inner_text().strip()
                result.title = self._parse_title_from_headline(headline)
                logger.info("Extracted title: %s", result.title)
            else:
                # Fallback: try the page title tag ("Name - Title - Company | LinkedIn")
                title_tag = page.title()
                if title_tag:
                    parsed = self._parse_title_from_page_title(title_tag)
                    if parsed:
                        result.title = parsed
                        logger.info("Extracted title from page title: %s", result.title)

            # Find company LinkedIn URL from the profile
            company_links = page.query_selector_all("a[href*='/company/']")
            for link in company_links:
                href = link.get_attribute("href") or ""
                match = re.search(r"(https?://www\.linkedin\.com/company/[^/?#]+)", href)
                if match:
                    result.company_linkedin_url = match.group(1).rstrip("/") + "/"
                    # Get company name text from the link
                    text = link.inner_text().strip()
                    if text and len(text) < 100:
                        result.company_name = text
                    break

            delay_between_clicks()

            # Visit company page to get canonical name
            if result.company_linkedin_url:
                canonical_name = self._extract_company_name(result.company_linkedin_url)
                if canonical_name:
                    result.company_name = canonical_name

        except Exception as e:
            logger.error("Error extracting profile from %s: %s", linkedin_url, e)

        return result

    def _extract_company_name(self, company_url: str) -> str | None:
        """Navigate to company LinkedIn page and extract the canonical name."""
        page = self._page
        try:
            page.goto(company_url, wait_until="domcontentloaded", timeout=15000)
            delay_page_load()
            self._scroll_page()

            # Company name is in the h1 element
            name_el = page.query_selector("h1")
            if name_el:
                name = name_el.inner_text().strip()
                if name and len(name) < 200:
                    logger.info("Company name from LinkedIn: %s", name)
                    return name
        except Exception as e:
            logger.error("Error extracting company from %s: %s", company_url, e)

        return None

    # ------------------------------------------------------------------
    # Title parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_title_from_headline(headline: str) -> str:
        """
        Parse a job title from a LinkedIn headline.

        Headlines vary: "VP of Engineering at Procore Technologies",
        "CEO & Co-Founder, Acme Corp", "Senior Software Engineer | ML"
        """
        # Split on common delimiters — take the part before company
        for sep in [" at ", " @ "]:
            if sep in headline:
                return headline.split(sep)[0].strip()
        # If pipe or dash, might be "Title | Company" or "Title - Company"
        for sep in [" | ", " - "]:
            if sep in headline:
                candidate = headline.split(sep)[0].strip()
                if candidate:
                    return candidate
        return headline.strip()

    @staticmethod
    def _parse_title_from_page_title(page_title: str) -> str | None:
        """
        Parse title from page <title> tag.

        Format: "Name - Title - Company | LinkedIn" (3+ parts)
        or: "Name - Title | LinkedIn" (2 parts)
        """
        raw = page_title.split(" | ")[0].strip()
        parts = [p.strip() for p in raw.split(" - ")]
        if len(parts) >= 3:
            title = " - ".join(parts[1:-1])
            if title.lower() not in ("linkedin", ""):
                return title
        elif len(parts) == 2:
            candidate = parts[1]
            if candidate.lower() not in ("linkedin", ""):
                return candidate
        return None
