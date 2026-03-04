"""
Enrichment merger for CRM data.
Merges multi-tab enrichment data with tab priority and updates relationship profiles.
"""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.company import Company
from src.models.contact import Contact
from src.models.contact_enrichment import ContactEnrichment
from src.models.relationship_profile import RelationshipProfile

logger = get_logger(__name__)

# Tab priority order (highest first) for resolving field conflicts
_TAB_PRIORITY: list[str] = [
    "Customer",
    "Feb 7 Transition",
    "2025 CAB",
    "CAB-rolled in",
    "Cab List",
    "Margaux - Responses",
    "Internal",
    "Family_Friends_Mentors_Others",
]

_TAB_PRIORITY_MAP: dict[str, int] = {name: idx for idx, name in enumerate(_TAB_PRIORITY)}


@dataclass
class MergeStats:
    """Statistics from an enrichment merge run."""

    contacts_merged: int = 0
    profiles_updated: int = 0
    conflicts_resolved: int = 0


class EnrichmentMerger:
    """Merges multi-tab enrichment data into Contact and RelationshipProfile records."""

    def __init__(self, user_id: UUID, db: Session):
        self.user_id = user_id
        self.db = db

    def merge_all(self) -> MergeStats:
        """
        Group ContactEnrichment rows by email and apply tab priority rules.

        For fields like title and company_id, the highest-priority tab wins.
        Tags are merged (union) from all tabs.

        Returns:
            MergeStats with counts of merged contacts and updated profiles.
        """
        stats = MergeStats()

        # Load all enrichments grouped by email
        enrichment_groups = self._load_enrichment_groups()
        logger.info("Found %d unique emails with enrichment data", len(enrichment_groups))

        # Bulk-load all companies referenced by enrichments to avoid N+1 queries
        all_company_ids = {
            e.company_id for group in enrichment_groups.values() for e in group if e.company_id
        }
        companies_by_id: dict = {}
        if all_company_ids:
            companies = (
                self.db.execute(select(Company).where(Company.id.in_(all_company_ids)))
                .scalars()
                .all()
            )
            companies_by_id = {c.id: c for c in companies}

        for email, enrichments in enrichment_groups.items():
            contact = self._get_contact(email)
            if contact is None:
                continue

            self._merge_contact_data(contact, enrichments)
            stats.contacts_merged += 1

            if self._update_relationship_profile(contact, enrichments, companies_by_id):
                stats.profiles_updated += 1

            if len(enrichments) > 1:
                stats.conflicts_resolved += 1

        self.db.commit()
        logger.info(
            "Merge complete: %d contacts merged, %d profiles updated, %d conflicts resolved",
            stats.contacts_merged,
            stats.profiles_updated,
            stats.conflicts_resolved,
        )
        return stats

    def _load_enrichment_groups(self) -> dict[str, list[ContactEnrichment]]:
        """Load all enrichments for this user, grouped by match_email."""
        stmt = (
            select(ContactEnrichment)
            .where(ContactEnrichment.user_id == self.user_id)
            .order_by(ContactEnrichment.match_email)
        )
        enrichments = self.db.execute(stmt).scalars().all()

        groups: dict[str, list[ContactEnrichment]] = {}
        for e in enrichments:
            groups.setdefault(e.match_email, []).append(e)

        # Sort each group by tab priority (highest priority first)
        for email in groups:
            groups[email].sort(key=lambda e: _TAB_PRIORITY_MAP.get(e.source_tab, 999))

        return groups

    def _get_contact(self, email: str) -> Contact | None:
        """Look up a contact by email."""
        stmt = select(Contact).where(Contact.user_id == self.user_id, Contact.email == email)
        return self.db.execute(stmt).scalar_one_or_none()

    def _merge_contact_data(self, contact: Contact, enrichments: list[ContactEnrichment]) -> None:
        """
        Apply enrichment data to a contact using tab priority rules.

        Enrichments are already sorted by priority (highest first).
        For scalar fields (title, company_id), take the first non-null value.
        For tags, merge (union) from all sources.
        """
        all_tags: set[str] = set(getattr(contact, "tags", None) or [])

        for enrichment in enrichments:
            raw = enrichment.raw_data or {}

            # Title: take highest priority non-null
            title = raw.get("title")
            if title and not getattr(contact, "title", None):
                contact.title = title

            # Company: take highest priority
            if enrichment.company_id and not getattr(contact, "company_id", None):
                contact.company_id = enrichment.company_id

            # Contact type from tab mapping
            from src.services.enrichment.contact_matcher import _TAB_CONTACT_TYPES

            ct = _TAB_CONTACT_TYPES.get(enrichment.source_tab)
            if ct and not getattr(contact, "contact_type", None):
                contact.contact_type = ct

            # Phone
            phone = raw.get("phone") or raw.get("cell")
            if phone and not contact.phone:
                contact.phone = phone

            # Personal email
            personal_email = raw.get("personal_email")
            if personal_email and not getattr(contact, "personal_email", None):
                contact.personal_email = personal_email

            # Salesforce ID
            sfid = raw.get("salesforce_id")
            if sfid and not getattr(contact, "salesforce_id", None):
                contact.salesforce_id = sfid

            # Collect tags from this tab
            from src.services.enrichment.contact_matcher import _TAB_TAGS

            tab_tags = _TAB_TAGS.get(enrichment.source_tab, [])
            all_tags.update(tab_tags)

        # Apply merged tags
        if all_tags:
            contact.tags = sorted(all_tags)

    def _update_relationship_profile(
        self,
        contact: Contact,
        enrichments: list[ContactEnrichment],
        companies_by_id: dict | None = None,
    ) -> bool:
        """
        Populate customer_data JSON on the contact's RelationshipProfile.

        Returns:
            True if a profile was updated, False otherwise.
        """
        stmt = select(RelationshipProfile).where(
            RelationshipProfile.user_id == self.user_id,
            RelationshipProfile.contact_email == contact.email,
        )
        profile = self.db.execute(stmt).scalar_one_or_none()
        if profile is None:
            return False

        companies_by_id = companies_by_id or {}

        # Build customer_data from enrichment sources
        customer_data: dict = {}

        # Company info (from highest-priority enrichment with company_id)
        for e in enrichments:
            if e.company_id:
                company = companies_by_id.get(e.company_id)
                if company:
                    customer_data["company_name"] = company.name
                    customer_data["company_domain"] = company.domain
                    if company.arr is not None:
                        customer_data["arr"] = str(company.arr)
                    if company.revenue_segment:
                        customer_data["revenue_segment"] = company.revenue_segment
                    if company.company_type:
                        customer_data["company_type"] = company.company_type
                    if company.account_tier:
                        customer_data["account_tier"] = company.account_tier
                    if company.account_owner:
                        customer_data["account_owner"] = company.account_owner
                    if company.csm:
                        customer_data["csm"] = company.csm
                    if company.renewal_date:
                        customer_data["renewal_date"] = company.renewal_date.isoformat()
                break

        # Contact enrichment summary
        customer_data["source_tabs"] = [e.source_tab for e in enrichments]
        customer_data["tags"] = sorted(set(getattr(contact, "tags", None) or []))

        if getattr(contact, "title", None):
            customer_data["title"] = contact.title
        if getattr(contact, "contact_type", None):
            customer_data["contact_type"] = contact.contact_type

        profile.customer_data = customer_data
        return True
