"""
HTTP client for CRM API endpoints used by LinkedIn enrichment.

Uses httpx (sync) to call the production Heroku API with X-API-Key auth.
"""

from __future__ import annotations

from dataclasses import dataclass

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
        return [ContactToEnrich(**item) for item in resp.json()["items"]]

    def get_needs_linkedin_url(self) -> list[ContactToEnrich]:
        """Contacts without linkedin_url at all."""
        resp = self._client.get("/crm/api/reports/needs-linkedin-url")
        resp.raise_for_status()
        items = []
        for item in resp.json()["items"]:
            item.setdefault("linkedin_url", None)
            items.append(ContactToEnrich(**item))
        return items

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

    def update_company(self, company_id: str, **fields) -> dict:
        """PATCH /crm/api/companies/{id} with partial update."""
        resp = self._client.patch(f"/crm/api/companies/{company_id}", json=fields)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._client.close()
