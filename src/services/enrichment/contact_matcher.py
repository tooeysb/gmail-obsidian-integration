"""
Contact matcher for CRM enrichment.
Matches spreadsheet rows to existing database contacts and creates new ones.
"""

from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.contact import Contact
from src.models.contact_enrichment import ContactEnrichment

logger = get_logger(__name__)

# Tag assignments by source tab
_TAB_TAGS: dict[str, list[str]] = {
    "Customer": ["critical", "customer"],
    "Feb 7 Transition": ["critical", "customer"],
    "2025 CAB": ["critical", "cab_member"],
    "CAB-rolled in": ["critical", "cab_member"],
    "Cab List": ["critical", "cab_member"],
    "Margaux - Responses": ["critical", "customer"],
    "Internal": ["critical", "internal"],
    "Family_Friends_Mentors_Others": ["critical", "family_friend"],
}

# Contact type by source tab
_TAB_CONTACT_TYPES: dict[str, str] = {
    "Customer": "customer",
    "Feb 7 Transition": "customer",
    "2025 CAB": "cab_member",
    "CAB-rolled in": "cab_member",
    "Cab List": "cab_member",
    "Margaux - Responses": "customer",
    "Internal": "internal",
    "Family_Friends_Mentors_Others": "family_friend",
}


@dataclass
class ImportStats:
    """Statistics from an enrichment import run."""

    matched: int = 0
    created: int = 0
    skipped: int = 0
    unmatched: int = 0
    per_tab: dict[str, dict[str, int]] = field(default_factory=dict)


class ContactMatcher:
    """Matches spreadsheet contacts against the database and imports new ones."""

    def __init__(self, user_id: UUID, db: Session, source_file: str | Path = ""):
        self.user_id = user_id
        self.db = db
        self.source_file = str(source_file)

    def match_and_import(
        self, tabs_data: dict[str, list[dict]], company_map: dict[str, UUID]
    ) -> ImportStats:
        """
        Process all tabs, matching rows to existing contacts or creating new ones.

        Args:
            tabs_data: Parsed tab data from ExcelImporter.
            company_map: Normalized company name -> company_id from CompanyResolver.

        Returns:
            ImportStats with counts of matched, created, skipped, unmatched contacts.
        """
        stats = ImportStats()
        existing_contacts = self._load_existing_contacts()
        logger.info("Loaded %d existing contacts for matching", len(existing_contacts))

        # Process tabs in priority order
        tab_order = [
            "Customer",
            "Feb 7 Transition",
            "2025 CAB",
            "CAB-rolled in",
            "Cab List",
            "Margaux - Responses",
            "Internal",
            "Family_Friends_Mentors_Others",
        ]

        for tab_name in tab_order:
            rows = tabs_data.get(tab_name, [])
            if not rows:
                continue

            tab_stats = self._process_tab(tab_name, rows, company_map, existing_contacts)
            stats.matched += tab_stats["matched"]
            stats.created += tab_stats["created"]
            stats.skipped += tab_stats["skipped"]
            stats.per_tab[tab_name] = tab_stats

            logger.info(
                "Tab '%s': %d matched, %d created, %d skipped (of %d rows)",
                tab_name,
                tab_stats["matched"],
                tab_stats["created"],
                tab_stats["skipped"],
                len(rows),
            )

        logger.info(
            "Import complete: %d matched, %d created, %d skipped",
            stats.matched,
            stats.created,
            stats.skipped,
        )
        return stats

    def _load_existing_contacts(self) -> dict[str, Contact]:
        """Bulk-load all contacts for this user into {lowercase_email: Contact}."""
        stmt = select(Contact).where(Contact.user_id == self.user_id)
        contacts = self.db.execute(stmt).scalars().all()
        return {c.email.lower(): c for c in contacts if c.email}

    def _process_tab(
        self,
        tab_name: str,
        rows: list[dict],
        company_map: dict[str, UUID],
        existing_contacts: dict[str, Contact],
    ) -> dict[str, int]:
        """Process a single tab's rows, creating/updating contacts and audit rows."""
        tab_stats = {"matched": 0, "created": 0, "skipped": 0}
        tags_for_tab = _TAB_TAGS.get(tab_name, [])
        contact_type = _TAB_CONTACT_TYPES.get(tab_name)
        enrichment_batch: list[ContactEnrichment] = []

        for row in rows:
            email = row.get("email")
            if not email:
                tab_stats["skipped"] += 1
                continue

            email_normalized = email.lower().strip()
            if not email_normalized or "@" not in email_normalized:
                tab_stats["skipped"] += 1
                continue

            # Build display name (prefer full "name" if available, else first+last)
            name = row.get("name")
            if not name:
                first = row.get("first_name", "") or ""
                last = row.get("last_name", "") or ""
                name = f"{first} {last}".strip() or None

            # Resolve company_id
            company_name = row.get("company", "")
            company_id = None
            if company_name:
                from src.services.enrichment.company_resolver import CompanyResolver

                normalized_company = CompanyResolver._normalize_name(company_name)
                company_id = company_map.get(normalized_company)

            # Match or create contact
            contact = existing_contacts.get(email_normalized)
            if contact:
                # Update existing contact with enrichment data
                self._update_contact(contact, name, company_id, contact_type, tags_for_tab, row)
                match_status = "matched"
                tab_stats["matched"] += 1
            else:
                # Create new contact
                contact = self._create_contact(
                    email_normalized,
                    name,
                    company_id,
                    contact_type,
                    tags_for_tab,
                    row,
                )
                self.db.add(contact)
                self.db.flush()
                existing_contacts[email_normalized] = contact
                match_status = "created"
                tab_stats["created"] += 1

            # Create audit row
            source_row = row.get("_source_row")
            enrichment = ContactEnrichment(
                user_id=self.user_id,
                match_email=email_normalized,
                contact_id=contact.id,
                company_id=company_id,
                source_file=self.source_file,
                source_tab=tab_name,
                source_row=int(source_row) if source_row else None,
                raw_data=row,
                match_status=match_status,
            )
            enrichment_batch.append(enrichment)

        # Bulk add enrichment records and commit per tab
        if enrichment_batch:
            self.db.add_all(enrichment_batch)
        self.db.commit()

        return tab_stats

    def _update_contact(
        self,
        contact: Contact,
        name: str | None,
        company_id: UUID | None,
        contact_type: str | None,
        tags: list[str],
        row: dict,
    ) -> None:
        """Update an existing contact with enrichment data (non-destructive)."""
        # Only update name if contact has no name
        if name and not contact.name:
            contact.name = name

        # Set company_id if not already set
        if company_id and not getattr(contact, "company_id", None):
            contact.company_id = company_id

        # Set title if available and not already set
        title = row.get("title")
        if title and not getattr(contact, "title", None):
            contact.title = title

        # Set contact_type if not already set
        if contact_type and not getattr(contact, "contact_type", None):
            contact.contact_type = contact_type

        # Set phone if available and not already set
        phone = row.get("phone") or row.get("cell")
        if phone and not contact.phone:
            contact.phone = phone

        # Set personal_email if available
        personal_email = row.get("personal_email")
        if personal_email and not getattr(contact, "personal_email", None):
            contact.personal_email = personal_email

        # Merge tags (union, never overwrite)
        self._merge_tags(contact, tags)

    def _create_contact(
        self,
        email: str,
        name: str | None,
        company_id: UUID | None,
        contact_type: str | None,
        tags: list[str],
        row: dict,
    ) -> Contact:
        """Create a new Contact from enrichment data."""
        phone = row.get("phone") or row.get("cell")
        personal_email = row.get("personal_email")
        title = row.get("title")
        salesforce_id = row.get("salesforce_id")

        kwargs: dict = {
            "user_id": self.user_id,
            "email": email,
            "name": name,
            "phone": phone,
            "account_sources": [],
            "email_count": 0,
            "tags": list(tags),
        }

        # Add CRM fields if the Contact model supports them
        if company_id is not None:
            kwargs["company_id"] = company_id
        if contact_type:
            kwargs["contact_type"] = contact_type
        if title:
            kwargs["title"] = title
        if personal_email:
            kwargs["personal_email"] = personal_email
        if salesforce_id:
            kwargs["salesforce_id"] = salesforce_id

        return Contact(**kwargs)

    @staticmethod
    def _merge_tags(contact: Contact, new_tags: list[str]) -> None:
        """Merge new tags into a contact's existing tags (union)."""
        existing = set(getattr(contact, "tags", None) or [])
        merged = existing | set(new_tags)
        if merged != existing:
            contact.tags = sorted(merged)
