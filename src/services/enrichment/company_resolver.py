"""
Company resolver for CRM enrichment.
Resolves and deduplicates company records from multi-tab spreadsheet data.
"""

import re
from decimal import Decimal, InvalidOperation
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.core.utils import GENERIC_EMAIL_DOMAINS
from src.models.company import Company

logger = get_logger(__name__)

# Suffixes to strip during normalization
_STRIP_SUFFIXES = [
    " - hq",
    " llc",
    " inc.",
    " inc",
    " corp.",
    " corp",
    " ltd.",
    " ltd",
    " co.",
    " co",
    " group",
    " holdings",
]


class CompanyResolver:
    """Builds and matches Company records from CRM spreadsheet data."""

    def __init__(self, user_id: UUID, db: Session):
        self.user_id = user_id
        self.db = db

    def resolve_companies(self, tabs_data: dict[str, list[dict]]) -> dict[str, UUID]:
        """
        Create and match Company records from all tabs.

        Processes "Over 1M Customers" first for canonical records with full metadata,
        then matches company names from other tabs via normalized/fuzzy matching.

        Returns:
            Mapping of normalized company name -> company UUID.
        """
        company_map: dict[str, UUID] = {}

        # Load existing companies for this user
        existing = self._load_existing_companies()
        company_map.update(existing)
        logger.info("Loaded %d existing companies", len(existing))

        # Phase 1: Create canonical companies from "Over 1M Customers" tab
        over_1m_rows = tabs_data.get("Over 1M Customers", [])
        if over_1m_rows:
            created = self._create_from_over_1m(over_1m_rows, company_map)
            logger.info("Created %d companies from Over 1M Customers tab", created)

        # Phase 2: Extract domain hints from contact tabs
        domain_hints = self._extract_domains_from_contacts(tabs_data)

        # Phase 3: Match companies from other tabs
        contact_tabs = [
            "Customer",
            "Feb 7 Transition",
            "2025 CAB",
            "CAB-rolled in",
            "Cab List",
            "Margaux - Responses",
        ]

        for tab_name in contact_tabs:
            rows = tabs_data.get(tab_name, [])
            for row in rows:
                company_name = row.get("company")
                if not company_name:
                    continue

                normalized = self._normalize_name(company_name)
                if normalized in company_map:
                    continue

                # Try fuzzy match against existing companies
                matched_company = self._match_company_name(normalized, company_map)
                if matched_company:
                    company_map[normalized] = matched_company
                    # Store the variant as an alias
                    self._add_alias(matched_company, company_name)
                    continue

                # Create new company with limited metadata
                domain = domain_hints.get(normalized)
                company = Company(
                    user_id=self.user_id,
                    name=company_name.strip(),
                    domain=domain,
                    source_data={"source_tab": tab_name},
                )
                self.db.add(company)
                self.db.flush()
                company_map[normalized] = company.id
                logger.debug("Created company '%s' from tab '%s'", company_name, tab_name)

        self.db.commit()
        logger.info("Company resolution complete: %d total companies mapped", len(company_map))
        return company_map

    def _load_existing_companies(self) -> dict[str, UUID]:
        """Load all existing companies for this user into a normalized-name map."""
        stmt = select(Company).where(Company.user_id == self.user_id)
        companies = self.db.execute(stmt).scalars().all()

        mapping: dict[str, UUID] = {}
        for c in companies:
            mapping[self._normalize_name(c.name)] = c.id
            if c.aliases:
                for alias in c.aliases:
                    mapping[self._normalize_name(alias)] = c.id
        return mapping

    def _create_from_over_1m(self, rows: list[dict], company_map: dict[str, UUID]) -> int:
        """Create canonical Company records from the Over 1M Customers tab."""
        created = 0
        for row in rows:
            name = row.get("company_name")
            if not name:
                continue

            normalized = self._normalize_name(name)
            if normalized in company_map:
                continue

            arr_value = self._parse_arr(row.get("arr"))

            company = Company(
                user_id=self.user_id,
                name=name.strip(),
                arr=arr_value,
                revenue_segment=row.get("revenue_segment"),
                company_type=row.get("company_type"),
                billing_state=row.get("billing_state"),
                account_tier=row.get("account_tier"),
                source_data={"source_tab": "Over 1M Customers", "raw": row},
            )
            self.db.add(company)
            self.db.flush()
            company_map[normalized] = company.id
            created += 1

        self.db.commit()
        return created

    def _match_company_name(self, normalized: str, company_map: dict[str, UUID]) -> UUID | None:
        """
        Fuzzy match a normalized company name against existing company names.
        Uses prefix matching and substring containment for common variations.
        """
        if not normalized:
            return None

        # Direct match (already checked by caller, but safety net)
        if normalized in company_map:
            return company_map[normalized]

        # Check if one name contains the other (for "Acme" vs "Acme Technologies")
        for existing_name, company_id in company_map.items():
            if not existing_name:
                continue
            # One must be a meaningful substring of the other (>4 chars)
            if len(normalized) > 4 and len(existing_name) > 4:
                if normalized in existing_name or existing_name in normalized:
                    return company_id

        return None

    def _add_alias(self, company_id: UUID, alias: str) -> None:
        """Add a name variant as an alias on the Company record."""
        stmt = select(Company).where(Company.id == company_id)
        company = self.db.execute(stmt).scalar_one_or_none()
        if company is None:
            return

        current_aliases = company.aliases or []
        if alias.strip() not in current_aliases:
            company.aliases = current_aliases + [alias.strip()]

    def _extract_domains_from_contacts(self, tabs_data: dict[str, list[dict]]) -> dict[str, str]:
        """
        Scan contact tabs to find email domains associated with company names.

        Returns:
            Mapping of normalized company name -> email domain.
        """
        domain_map: dict[str, str] = {}
        for tab_name, rows in tabs_data.items():
            if tab_name == "Over 1M Customers":
                continue
            for row in rows:
                company = row.get("company")
                email = row.get("email")
                if not company or not email:
                    continue

                normalized = self._normalize_name(company)
                if normalized in domain_map:
                    continue

                domain = self._extract_domain(email)
                if domain and not self._is_generic_domain(domain):
                    domain_map[normalized] = domain

        return domain_map

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize a company name for matching."""
        if not name:
            return ""
        result = name.lower().strip()
        for suffix in _STRIP_SUFFIXES:
            if result.endswith(suffix):
                result = result[: -len(suffix)].strip()
        # Collapse multiple spaces
        result = re.sub(r"\s+", " ", result)
        return result

    @staticmethod
    def _extract_domain(email: str) -> str | None:
        """Extract domain from an email address."""
        if "@" not in email:
            return None
        return email.split("@", 1)[1].lower().strip()

    @staticmethod
    def _is_generic_domain(domain: str) -> bool:
        """Check if a domain is a generic email provider (not company-specific)."""
        return domain in GENERIC_EMAIL_DOMAINS

    @staticmethod
    def _parse_arr(value: str | None) -> Decimal | None:
        """Parse an ARR value that may include $ signs, commas, etc."""
        if not value:
            return None
        cleaned = re.sub(r"[^\d.]", "", str(value))
        if not cleaned:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None
