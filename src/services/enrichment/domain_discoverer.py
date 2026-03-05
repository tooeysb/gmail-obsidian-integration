"""
Domain-based contact discovery for CRM.

Scans all emails to find people whose domain matches a CRM company.
Populates the discovered_contacts cache table for instant UI access.
"""

import re
import uuid as uuid_mod
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.account import GmailAccount
from src.models.company import Company
from src.models.contact import Contact
from src.models.discovered_contact import DiscoveredContact
from src.models.email import Email

logger = get_logger(__name__)

# Regex to extract email from "Name <email>" format
_EMAIL_BRACKET_RE = re.compile(r"<([^>]+)>")

# Domains to skip (generic email providers, not company domains)
_GENERIC_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "live.com",
    "msn.com",
    "comcast.net",
    "att.net",
    "verizon.net",
    "sbcglobal.net",
    "ymail.com",
    "rocketmail.com",
    "protonmail.com",
    "zoho.com",
}


class DomainContactDiscoverer:
    """Scans all emails and discovers people matching CRM company domains."""

    # Only scan the primary account (tooey@procore.com)
    PRIMARY_ACCOUNT_LABEL = "procore-main"

    def __init__(self, user_id: UUID, db: Session):
        self.user_id = user_id
        self.db = db
        self.account_id = self._resolve_account_id()

    def discover_all(self, batch_size: int = 5000) -> dict:
        """
        Full rebuild of discovered_contacts table.

        Returns dict with stats: {companies, emails_scanned, people_found, created}.
        """
        # 1. Load company domains
        domain_to_company = self._load_company_domains()
        if not domain_to_company:
            logger.warning("No companies with domains found")
            return {"companies": 0, "emails_scanned": 0, "people_found": 0, "created": 0}

        logger.info("Loaded %d company domains", len(domain_to_company))

        # 2. Load existing contact emails to exclude
        existing_contacts = self._load_existing_contact_emails()
        logger.info("Loaded %d existing contact emails to exclude", len(existing_contacts))

        # 3. Scan all emails and accumulate discovered people
        people = self._scan_emails(domain_to_company, existing_contacts, batch_size)
        logger.info("Found %d discovered people across all domains", len(people))

        # 4. Clear old discovered contacts and insert new ones
        created = self._rebuild_table(people)

        stats = {
            "companies": len(domain_to_company),
            "people_found": len(people),
            "created": created,
        }
        logger.info("Discovery complete: %s", stats)
        return stats

    def _load_company_domains(self) -> dict[str, UUID]:
        """Build {lowercase_domain: company_id} lookup."""
        stmt = select(Company.id, Company.domain).where(
            Company.user_id == self.user_id, Company.domain.isnot(None)
        )
        rows = self.db.execute(stmt).all()
        result = {}
        for cid, domain in rows:
            if domain:
                d = domain.lower().strip()
                if d and d not in _GENERIC_DOMAINS:
                    result[d] = cid
        return result

    def _resolve_account_id(self) -> UUID | None:
        """Resolve the primary Gmail account ID for filtering emails."""
        stmt = select(GmailAccount.id).where(
            GmailAccount.user_id == self.user_id,
            GmailAccount.account_label == self.PRIMARY_ACCOUNT_LABEL,
        )
        row = self.db.execute(stmt).first()
        if row:
            logger.info("Filtering to account '%s' (%s)", self.PRIMARY_ACCOUNT_LABEL, row[0])
            return row[0]
        logger.warning(
            "Primary account '%s' not found, scanning all emails", self.PRIMARY_ACCOUNT_LABEL
        )
        return None

    def _load_existing_contact_emails(self) -> set[str]:
        """Load all existing contact emails (lowercase) to exclude from discovery."""
        stmt = select(Contact.email).where(Contact.user_id == self.user_id)
        rows = self.db.execute(stmt).all()
        return {email.lower() for (email,) in rows if email}

    def _email_filter(self):
        """Base filter for email queries — scoped to user and primary account."""
        conditions = [Email.user_id == self.user_id]
        if self.account_id:
            conditions.append(Email.account_id == self.account_id)
        return conditions

    def _scan_emails(
        self,
        domain_to_company: dict[str, UUID],
        existing_contacts: set[str],
        batch_size: int,
    ) -> dict[str, dict]:
        """
        Stream emails using keyset pagination (WHERE id > last_id) for
        constant-time batch fetches regardless of table position.

        Returns {email: {name, company_id, email_count, first_email_at, last_email_at}}.
        """
        people: dict[str, dict] = {}

        email_filters = self._email_filter()
        total_emails = self.db.execute(
            select(func.count(Email.id)).where(*email_filters)
        ).scalar_one()

        logger.info("Scanning %d emails in batches of %d", total_emails, batch_size)

        last_id = uuid_mod.UUID(int=0)  # Start from the beginning
        processed = 0

        while True:
            stmt = (
                select(
                    Email.id,
                    Email.sender_email,
                    Email.sender_name,
                    Email.recipient_emails,
                    Email.date,
                )
                .where(*email_filters, Email.id > last_id)
                .order_by(Email.id)
                .limit(batch_size)
            )
            rows = self.db.execute(stmt).all()
            if not rows:
                break

            for row_id, sender_email, sender_name, recipient_emails, email_date in rows:
                last_id = row_id

                # Check sender
                if sender_email:
                    self._check_and_add(
                        sender_email.strip(),
                        sender_name,
                        email_date,
                        domain_to_company,
                        existing_contacts,
                        people,
                    )

                # Check recipients
                if recipient_emails:
                    for raw_addr in recipient_emails.split(","):
                        raw_addr = raw_addr.strip()
                        if not raw_addr:
                            continue
                        parsed_email = self._parse_email_address(raw_addr)
                        parsed_name = self._parse_name(raw_addr)
                        if parsed_email:
                            self._check_and_add(
                                parsed_email,
                                parsed_name,
                                email_date,
                                domain_to_company,
                                existing_contacts,
                                people,
                            )

            processed += len(rows)

            if processed % 50_000 == 0 or processed >= total_emails:
                logger.info(
                    "Progress: %d/%d emails scanned, %d people found",
                    processed,
                    total_emails,
                    len(people),
                )

        return people

    def _check_and_add(
        self,
        email: str,
        name: str | None,
        email_date,
        domain_to_company: dict[str, UUID],
        existing_contacts: set[str],
        people: dict[str, dict],
    ):
        """Check if an email matches a company domain and add to people dict."""
        email_lower = email.lower().strip()
        if not email_lower or "@" not in email_lower:
            return

        # Skip existing CRM contacts
        if email_lower in existing_contacts:
            return

        # Extract domain and check against company lookup
        domain = email_lower.split("@")[-1]
        company_id = domain_to_company.get(domain)
        if not company_id:
            return

        # Skip noreply/automated addresses
        local_part = email_lower.split("@")[0]
        if any(
            p in local_part
            for p in ("noreply", "no-reply", "donotreply", "mailer-daemon", "postmaster")
        ):
            return

        # Add or update entry
        if email_lower not in people:
            people[email_lower] = {
                "email": email,
                "name": name,
                "company_id": company_id,
                "email_count": 0,
                "first_email_at": email_date,
                "last_email_at": email_date,
            }

        entry = people[email_lower]
        entry["email_count"] += 1

        # Update name if we got a better one
        if name and not entry["name"]:
            entry["name"] = name

        # Update date range
        if email_date:
            if not entry["first_email_at"] or email_date < entry["first_email_at"]:
                entry["first_email_at"] = email_date
            if not entry["last_email_at"] or email_date > entry["last_email_at"]:
                entry["last_email_at"] = email_date

    def _rebuild_table(self, people: dict[str, dict]) -> int:
        """Delete old discovered contacts and insert new ones."""
        # Delete existing for this user
        self.db.execute(delete(DiscoveredContact).where(DiscoveredContact.user_id == self.user_id))
        self.db.commit()

        if not people:
            return 0

        # Bulk insert in chunks
        chunk_size = 1000
        items = list(people.values())
        total_created = 0

        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            values = [
                {
                    "id": uuid_mod.uuid4(),
                    "user_id": self.user_id,
                    "company_id": p["company_id"],
                    "email": p["email"].lower().strip(),
                    "name": p["name"],
                    "email_count": p["email_count"],
                    "last_email_at": p["last_email_at"],
                    "first_email_at": p["first_email_at"],
                }
                for p in chunk
            ]
            stmt = pg_insert(DiscoveredContact.__table__).values(values)
            stmt = stmt.on_conflict_do_nothing(constraint="uq_user_discovered_email")
            result = self.db.execute(stmt)
            total_created += result.rowcount

        self.db.commit()
        logger.info("Inserted %d discovered contacts", total_created)
        return total_created

    @staticmethod
    def _parse_email_address(raw: str) -> str | None:
        """Extract and normalize an email address from 'Name <email>' or bare format."""
        if not raw:
            return None
        raw = raw.strip()
        match = _EMAIL_BRACKET_RE.search(raw)
        if match:
            email = match.group(1).strip().lower()
        else:
            email = raw.strip().lower()
        if "@" not in email or " " in email:
            return None
        return email

    @staticmethod
    def _parse_name(raw: str) -> str | None:
        """Extract display name from 'Name <email>' format."""
        if not raw or "<" not in raw:
            return None
        name_part = raw.split("<")[0].strip().strip('"').strip(",").strip()
        return name_part if name_part else None
