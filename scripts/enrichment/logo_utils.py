"""
Logo extraction and perceptual hash comparison utilities.

Extracts logos from company websites and LinkedIn pages using Playwright,
then compares them using perceptual hashing (pHash) to verify identity.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import imagehash
from PIL import Image
from playwright.sync_api import Page

from scripts.enrichment.human_behavior import delay_page_load
from src.core.logging import get_logger

logger = get_logger(__name__)

# pHash distance thresholds
# 0-10: identical or near-identical logos
# 10-20: same logo with color/background differences (e.g., dark vs light mode)
# 20-30: uncertain zone — same logo with text/layout differences, needs secondary check
# 30+: likely different logos
MATCH_THRESHOLD = 20  # distance <= 20 = confident match
SOFT_MATCH_THRESHOLD = 30  # distance 21-30 = match IF secondary signals confirm
NO_MATCH_THRESHOLD = 30  # distance > 30 = definite no-match

# Minimum image dimensions to accept (avoid tiny favicons / tracking pixels)
MIN_LOGO_SIZE = (32, 32)


@dataclass
class LogoResult:
    """Result of logo extraction from a page."""

    image_bytes: bytes | None = None
    phash: str | None = None
    phash_crops: list[str] | None = None  # additional hashes from cropped variants
    source_url: str | None = None
    error: str | None = None


def compute_phash(image_bytes: bytes) -> str | None:
    """Compute perceptual hash of an image from raw bytes."""
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        if img.size[0] < MIN_LOGO_SIZE[0] or img.size[1] < MIN_LOGO_SIZE[1]:
            logger.warning("Image too small (%dx%d), skipping", img.size[0], img.size[1])
            return None
        h = imagehash.phash(img)
        return str(h)
    except Exception as e:
        logger.error("Failed to compute pHash: %s", e)
        return None


def compute_phash_crops(image_bytes: bytes) -> list[str]:
    """Compute pHash for full image and cropped variants (top 60%, 75%).

    LinkedIn logos often include company name text below the graphic mark.
    Cropping the top portion isolates just the logo for better comparison.
    """
    hashes = []
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        if img.size[0] < MIN_LOGO_SIZE[0] or img.size[1] < MIN_LOGO_SIZE[1]:
            return hashes

        # Full image hash
        hashes.append(str(imagehash.phash(img)))

        # Cropped variants — top 60% and top 75%
        w, h = img.size
        for ratio in (0.60, 0.75):
            crop_h = int(h * ratio)
            if crop_h >= MIN_LOGO_SIZE[1]:
                cropped = img.crop((0, 0, w, crop_h))
                hashes.append(str(imagehash.phash(cropped)))
    except Exception as e:
        logger.error("Failed to compute pHash crops: %s", e)
    return hashes


def hash_distance(hash1: str, hash2: str) -> int:
    """Compute hamming distance between two hex pHash strings."""
    h1 = imagehash.hex_to_hash(hash1)
    h2 = imagehash.hex_to_hash(hash2)
    return int(h1 - h2)


def best_hash_distance(hashes1: list[str], hashes2: list[str]) -> int:
    """Find minimum hamming distance across all pairs of candidate hashes.

    Both sides can provide multiple crops (full, top 60%, top 75%) to handle
    cases where one version has extra text, whitespace, or trademark symbols.
    """
    if not hashes1 or not hashes2:
        return 64  # max possible distance
    return min(hash_distance(h1, h2) for h1 in hashes1 for h2 in hashes2)


def extract_website_logo(page: Page, domain: str) -> LogoResult:
    """Visit a company website and extract the primary logo image.

    Strategy (in priority order):
    1. <img> in header/nav with "logo" in class, id, alt, or src
    2. og:image meta tag
    3. Favicon (<link rel="icon"> or /favicon.ico)
    """
    result = LogoResult()

    url = f"https://www.{domain}" if not domain.startswith("http") else domain

    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=15000)
        if not resp or resp.status >= 400:
            result.error = f"HTTP {resp.status if resp else 'no response'}"
            return result
        delay_page_load()
    except Exception as e:
        result.error = f"Navigation failed: {e}"
        return result

    # Strategy 1: Logo image in header/nav
    logo_selectors = [
        "header img[class*='logo' i]",
        "nav img[class*='logo' i]",
        "img[id*='logo' i]",
        "img[alt*='logo' i]",
        "img[src*='logo' i]",
        ".logo img",
        "#logo img",
        "[class*='logo'] img",
        "header a[href='/'] img",
        "header a img:first-of-type",
        "nav a[href='/'] img",
    ]

    for selector in logo_selectors:
        try:
            el = page.query_selector(selector)
            if el:
                img_bytes = _screenshot_element(el)
                if img_bytes:
                    phash = compute_phash(img_bytes)
                    if phash:
                        result.image_bytes = img_bytes
                        result.phash = phash
                        result.phash_crops = compute_phash_crops(img_bytes)
                        result.source_url = el.get_attribute("src") or selector
                        logger.info("Website logo found via selector: %s", selector)
                        return result
        except Exception:
            continue

    # Strategy 2: og:image meta tag
    try:
        og_el = page.query_selector("meta[property='og:image']")
        if og_el:
            og_url = og_el.get_attribute("content")
            if og_url:
                img_bytes = _download_image_via_page(page, og_url)
                if img_bytes:
                    phash = compute_phash(img_bytes)
                    if phash:
                        result.image_bytes = img_bytes
                        result.phash = phash
                        result.source_url = og_url
                        logger.info("Website logo found via og:image")
                        return result
    except Exception:
        pass

    # Strategy 3: Favicon
    try:
        favicon_selectors = [
            "link[rel='icon']",
            "link[rel='shortcut icon']",
            "link[rel='apple-touch-icon']",
        ]
        for sel in favicon_selectors:
            el = page.query_selector(sel)
            if el:
                href = el.get_attribute("href")
                if href:
                    img_bytes = _download_image_via_page(page, href)
                    if img_bytes:
                        phash = compute_phash(img_bytes)
                        if phash:
                            result.image_bytes = img_bytes
                            result.phash = phash
                            result.source_url = href
                            logger.info("Website logo found via favicon: %s", sel)
                            return result

        # Last resort: /favicon.ico
        favicon_url = f"{url.rstrip('/')}/favicon.ico"
        img_bytes = _download_image_via_page(page, favicon_url)
        if img_bytes:
            phash = compute_phash(img_bytes)
            if phash:
                result.image_bytes = img_bytes
                result.phash = phash
                result.source_url = favicon_url
                logger.info("Website logo found via /favicon.ico")
                return result
    except Exception:
        pass

    result.error = "No logo found on website"
    return result


def extract_linkedin_logo(page: Page, linkedin_url: str) -> LogoResult:
    """Extract company logo from a LinkedIn company page.

    Uses element screenshot to avoid LinkedIn CDN auth issues.
    """
    result = LogoResult()

    try:
        about_url = linkedin_url.rstrip("/") + "/"
        page.goto(about_url, wait_until="domcontentloaded", timeout=15000)
        delay_page_load()
    except Exception as e:
        result.error = f"Navigation failed: {e}"
        return result

    # Check for login wall
    current_url = page.url
    if "/login" in current_url or "/authwall" in current_url:
        result.error = "LinkedIn session expired"
        return result

    logo_selectors = [
        ".org-top-card-primary-content__logo-container img",
        ".top-card-layout__entity-image",
        "img[data-ghost-url]",
        ".org-top-card__primary-content img",
        "img.org-top-card-primary-content__logo",
        ".top-card-layout img",
        "img[alt*='logo' i]",
    ]

    for selector in logo_selectors:
        try:
            el = page.query_selector(selector)
            if el:
                img_bytes = _screenshot_element(el)
                if img_bytes:
                    phash = compute_phash(img_bytes)
                    if phash:
                        result.image_bytes = img_bytes
                        result.phash = phash
                        result.phash_crops = compute_phash_crops(img_bytes)
                        result.source_url = el.get_attribute("src") or selector
                        logger.info("LinkedIn logo found via selector: %s", selector)
                        return result
        except Exception:
            continue

    result.error = "No logo found on LinkedIn page"
    return result


def extract_website_title(page: Page) -> str | None:
    """Extract company name from current page's <title> or og:site_name."""
    try:
        # Try og:site_name first (usually cleaner)
        og = page.query_selector("meta[property='og:site_name']")
        if og:
            name = og.get_attribute("content")
            if name and len(name.strip()) > 1:
                return name.strip()

        # Fall back to <title> tag, strip common suffixes
        title_el = page.query_selector("title")
        if title_el:
            title = title_el.inner_text().strip()
            # Remove common suffixes like " | Home", " - Welcome", etc.
            title = re.split(r"\s*[|–—-]\s*(?:Home|Welcome|Official).*$", title, flags=re.IGNORECASE)[0]
            if title and len(title.strip()) > 1:
                return title.strip()
    except Exception:
        pass
    return None


def extract_linkedin_name(page: Page) -> str | None:
    """Extract company name from the LinkedIn company page heading."""
    selectors = [
        "h1.org-top-card-summary__title",
        "h1.top-card-layout__title",
        "h1 span[dir='ltr']",
        ".org-top-card-summary__title",
        "h1",
    ]
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                if text and len(text) > 1:
                    return text
        except Exception:
            continue
    return None


def extract_linkedin_domain(page: Page, linkedin_url: str) -> str | None:
    """Extract the company website domain from LinkedIn's About section.

    Navigates to the /about/ tab and looks for the website link,
    which LinkedIn displays as a domain (e.g., "abc.org").
    """
    try:
        about_url = linkedin_url.rstrip("/") + "/about/"
        page.goto(about_url, wait_until="domcontentloaded", timeout=15000)
        delay_page_load()
    except Exception:
        return None

    # LinkedIn shows website in a <dd> element after a <dt>Website</dt> label,
    # and also as <a class="link-without-visited-state"> with the URL as text
    try:
        # Best approach: find <a> links whose text looks like a URL (not linkedin.com)
        links = page.query_selector_all("a.link-without-visited-state")
        for link in links:
            href = link.get_attribute("href") or ""
            text = link.inner_text().strip()
            # Skip LinkedIn-internal links
            if "linkedin.com" in href.lower():
                continue
            domain = _extract_domain(text) or _extract_domain(href)
            if domain:
                logger.info("LinkedIn About domain: %s", domain)
                return domain
    except Exception:
        pass

    # Fallback: check <dd> elements for URL-like text
    try:
        dds = page.query_selector_all("dd")
        for dd in dds:
            text = dd.inner_text().strip()
            domain = _extract_domain(text)
            if domain:
                logger.info("LinkedIn About domain (dd fallback): %s", domain)
                return domain
    except Exception:
        pass

    return None


def _extract_domain(url_or_text: str) -> str | None:
    """Extract bare domain from a URL or text like 'www.abc.org'."""
    if not url_or_text:
        return None
    text = url_or_text.strip().lower()
    # If it looks like a URL, parse it
    if "://" in text:
        parsed = urlparse(text)
        host = parsed.hostname or ""
        return host.removeprefix("www.") if host else None
    # If it looks like a domain
    text = text.removeprefix("www.")
    if re.match(r"^[\w.-]+\.\w{2,}$", text):
        return text
    return None


def domains_match(domain: str, linkedin_domain: str | None) -> bool:
    """Check if the company domain matches the domain listed on LinkedIn."""
    if not linkedin_domain:
        return False
    norm1 = domain.lower().removeprefix("www.")
    norm2 = linkedin_domain.lower().removeprefix("www.")
    return norm1 == norm2


def names_match(company_name: str, website_title: str | None, linkedin_name: str | None) -> bool:
    """Check if company name appears in website title or LinkedIn name.

    Uses case-insensitive substring matching with normalization.
    Returns True if the company name is found in either source.
    """
    if not website_title and not linkedin_name:
        return False

    def normalize(s: str) -> str:
        # Lowercase + strip punctuation + collapse whitespace
        s = re.sub(r"[^\w\s]", " ", s.lower())
        return " ".join(s.split())

    norm_company = normalize(company_name)
    if not norm_company:
        return False

    # Check each word of the company name (handles abbreviations like "ABC")
    company_words = norm_company.split()

    for source in [website_title, linkedin_name]:
        if not source:
            continue
        norm_source = normalize(source)
        # Full name contained in source
        if norm_company in norm_source or norm_source in norm_company:
            return True
        # For short names (<=3 words), check if all words appear
        if len(company_words) <= 3 and all(w in norm_source for w in company_words):
            return True

    return False


def _screenshot_element(element) -> bytes | None:
    """Take a screenshot of a Playwright element, returning PNG bytes."""
    try:
        return element.screenshot(type="png")
    except Exception as e:
        logger.debug("Element screenshot failed: %s", e)
        return None


def _download_image_via_page(page: Page, url: str) -> bytes | None:
    """Download an image using the page's browser context."""
    try:
        if not url.startswith("http"):
            base = page.url
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                parsed = urlparse(base)
                url = f"{parsed.scheme}://{parsed.netloc}{url}"

        response = page.request.get(url)
        if response.ok:
            return response.body()
    except Exception as e:
        logger.debug("Image download failed for %s: %s", url, e)
    return None
