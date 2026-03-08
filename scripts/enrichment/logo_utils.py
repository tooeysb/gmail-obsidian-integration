"""
Logo extraction and perceptual hash comparison utilities.

Extracts logos from company websites and LinkedIn pages using Playwright,
then compares them using perceptual hashing (pHash) to verify identity.
"""

from __future__ import annotations

import io
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
# 20+: likely different logos
MATCH_THRESHOLD = 20  # distance <= 20 = match
NO_MATCH_THRESHOLD = 30  # distance >= 30 = definite no-match

# Minimum image dimensions to accept (avoid tiny favicons / tracking pixels)
MIN_LOGO_SIZE = (32, 32)


@dataclass
class LogoResult:
    """Result of logo extraction from a page."""

    image_bytes: bytes | None = None
    phash: str | None = None
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


def hash_distance(hash1: str, hash2: str) -> int:
    """Compute hamming distance between two hex pHash strings."""
    h1 = imagehash.hex_to_hash(hash1)
    h2 = imagehash.hex_to_hash(hash2)
    return int(h1 - h2)


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
                        result.source_url = el.get_attribute("src") or selector
                        logger.info("LinkedIn logo found via selector: %s", selector)
                        return result
        except Exception:
            continue

    result.error = "No logo found on LinkedIn page"
    return result


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
