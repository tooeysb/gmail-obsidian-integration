"""
HTTP client for CRM API endpoints used by LinkedIn enrichment.

Uses httpx (sync) to call the production Heroku API with X-API-Key auth.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

import httpx

from src.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ContactToEnrich:
    """Contact needing LinkedIn enrichment."""

    id: str
    name: str | None
    email: str
    company_name: str | None
    linkedin_url: str | None
    email_count: int
    title: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ContactToEnrich:
        """Create from API response dict, ignoring extra fields."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


class CRMClient:
    """Sync HTTP client for CRM API."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0):
        self._client = httpx.Client(
            base_url=base_url,
            headers={"X-API-Key": api_key},
            timeout=timeout,
        )

    def get_needs_browser_enrich(self) -> list[ContactToEnrich]:
        """Contacts with linkedin_url but no title."""
        resp = self._client.get("/crm/api/reports/needs-browser-enrich")
        resp.raise_for_status()
        return [ContactToEnrich.from_dict(item) for item in resp.json()["items"]]

    def get_needs_linkedin_url(self) -> list[ContactToEnrich]:
        """Contacts without linkedin_url at all."""
        resp = self._client.get("/crm/api/reports/needs-linkedin-url")
        resp.raise_for_status()
        items = []
        for item in resp.json()["items"]:
            item.setdefault("linkedin_url", None)
            items.append(ContactToEnrich.from_dict(item))
        return items

    def get_needs_recheck(self) -> list[ContactToEnrich]:
        """Enriched contacts due for LinkedIn re-check (oldest first)."""
        resp = self._client.get("/crm/api/reports/needs-linkedin-recheck")
        resp.raise_for_status()
        return [ContactToEnrich.from_dict(item) for item in resp.json()["items"]]

    def update_contact(self, contact_id: str, **fields) -> dict:
        """PATCH /crm/api/contacts/{id} with partial update."""
        resp = self._client.patch(f"/crm/api/contacts/{contact_id}", json=fields)
        resp.raise_for_status()
        return resp.json()

    def search_companies(self, query: str) -> list[dict]:
        """GET /crm/api/companies?search=X — returns list of company dicts."""
        resp = self._client.get("/crm/api/companies", params={"search": query, "page_size": 5})
        resp.raise_for_status()
        return resp.json().get("items", [])

    def create_company(self, **fields) -> dict:
        """POST /crm/api/companies — create a new company."""
        resp = self._client.post("/crm/api/companies", json=fields)
        resp.raise_for_status()
        return resp.json()

    def update_company(self, company_id: str, **fields) -> dict:
        """PATCH /crm/api/companies/{id} with partial update."""
        resp = self._client.patch(f"/crm/api/companies/{company_id}", json=fields)
        resp.raise_for_status()
        return resp.json()

    def get_company_detail(self, company_id: str) -> dict:
        """GET /crm/api/companies/{id} — full company detail with contacts."""
        resp = self._client.get(f"/crm/api/companies/{company_id}")
        resp.raise_for_status()
        return resp.json()

    def get_needs_company_linkedin(self) -> list[dict]:
        """Companies without a LinkedIn company page URL."""
        resp = self._client.get("/crm/api/reports/needs-company-linkedin")
        resp.raise_for_status()
        return resp.json()["items"]

    def get_needs_leadership(self) -> list[dict]:
        """Companies with a domain but no leadership page scraped."""
        resp = self._client.get("/crm/api/reports/needs-leadership-discovery")
        resp.raise_for_status()
        return resp.json()["items"]

    def get_needs_leadership_retry(self) -> list[dict]:
        """GC/SC companies scraped but no leadership page found (for retry)."""
        resp = self._client.get("/crm/api/reports/needs-leadership-retry")
        resp.raise_for_status()
        return resp.json()["items"]

    def get_needs_logo_verification(self) -> list[dict]:
        """Companies with linkedin_url and domain but no logo verification."""
        resp = self._client.get("/crm/api/reports/needs-logo-verification")
        resp.raise_for_status()
        return resp.json()["items"]

    def add_contact_to_company(
        self,
        company_id: str,
        email: str,
        name: str | None = None,
        title: str | None = None,
        contact_source: str | None = None,
    ) -> dict:
        """POST /crm/api/companies/{id}/contacts — add a new contact."""
        body: dict = {"email": email}
        if name:
            body["name"] = name
        if title:
            body["title"] = title
        if contact_source:
            body["contact_source"] = contact_source
        resp = self._client.post(f"/crm/api/companies/{company_id}/contacts", json=body)
        resp.raise_for_status()
        return resp.json()

    # LinkedIn monitoring endpoints

    def get_needs_post_check(self, tier: str | None = None) -> list[dict]:
        """Contacts due for LinkedIn activity scraping based on tier schedule."""
        params = {}
        if tier:
            params["tier"] = tier
        resp = self._client.get("/crm/api/reports/needs-post-check", params=params)
        resp.raise_for_status()
        return resp.json()["items"]

    def get_needs_profile_check(self, tier: str | None = None) -> list[dict]:
        """Contacts due for LinkedIn profile check (job/title changes)."""
        params = {}
        if tier:
            params["tier"] = tier
        resp = self._client.get("/crm/api/reports/needs-profile-check", params=params)
        resp.raise_for_status()
        return resp.json()["items"]

    def create_linkedin_posts(self, contact_id: str, posts: list[dict]) -> dict:
        """POST /crm/api/contacts/{id}/linkedin-posts — batch create posts."""
        resp = self._client.post(
            f"/crm/api/contacts/{contact_id}/linkedin-posts",
            json={"posts": posts},
        )
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._client.close()
