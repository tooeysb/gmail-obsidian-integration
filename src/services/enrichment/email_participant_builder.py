"""
Email participant builder for CRM enrichment.
Bulk-populates the EmailParticipant junction table from existing emails and contacts.
"""

import re
import uuid as uuid_mod
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.logging import get_logger
from src.models.contact import Contact
from src.models.email import Email
from src.models.email_participant import EmailParticipant

logger = get_logger(__name__)

# Regex to extract email from "Name <email>" format
_EMAIL_BRACKET_RE = re.compile(r"<([^>]+)>")


class EmailParticipantBuilder:
    """Bulk-populates EmailParticipant junction table from emails and contacts."""

    def __init__(self, user_id: UUID, db: Session):
        self.user_id = user_id
        self.db = db

    def build_all(self, batch_size: int = 5000) -> int:
        """
        Process all emails and create EmailParticipant records linking emails to contacts.

        Uses streaming batches to avoid loading all 1.16M emails into memory.
        Uses INSERT ON CONFLICT DO NOTHING for idempotent re-runs.

        Args:
            batch_size: Number of emails to process per batch.

        Returns:
            Total number of EmailParticipant rows created.
        """
        contact_lookup = self._build_contact_lookup()
        logger.info("Built contact lookup with %d entries", len(contact_lookup))

        if not contact_lookup:
            logger.warning("No contacts found. Cannot build email participants.")
            return 0

        total_emails = self.db.execute(
            select(func.count(Email.id)).where(Email.user_id == self.user_id)
        ).scalar_one()
        logger.info("Processing %d emails in batches of %d", total_emails, batch_size)

        total_created = 0
        processed = 0
        offset = 0

        while offset < total_emails:
            # Windowed query to stream emails in batches
            stmt = (
                select(Email.id, Email.sender_email, Email.recipient_emails)
                .where(Email.user_id == self.user_id)
                .order_by(Email.id)
                .offset(offset)
                .limit(batch_size)
            )
            rows = self.db.execute(stmt).all()
            if not rows:
                break

            batch_created = self._process_batch(rows, contact_lookup)
            total_created += batch_created
            processed += len(rows)
            offset += batch_size

            if processed % 10_000 == 0 or processed == total_emails:
                logger.info(
                    "Progress: %d/%d emails processed, %d participants created",
                    processed,
                    total_emails,
                    total_created,
                )

        logger.info(
            "Build complete: %d total EmailParticipant rows created from %d emails",
            total_created,
            processed,
        )
        return total_created

    def build_for_contact(self, contact_id: UUID, contact_email: str, batch_size: int = 5000) -> int:
        """
        Build EmailParticipant records for a single contact.

        Scans all emails matching the contact's email address and creates
        participant records. Used when a new contact is added to the CRM.

        Returns:
            Number of EmailParticipant rows created.
        """
        email_lower = contact_email.lower().strip()
        contact_lookup = {email_lower: contact_id}

        # Find emails where this person is sender or recipient
        stmt = (
            select(Email.id, Email.sender_email, Email.recipient_emails)
            .where(
                Email.user_id == self.user_id,
                or_(
                    func.lower(Email.sender_email) == email_lower,
                    func.lower(Email.recipient_emails).contains(email_lower),
                ),
            )
            .order_by(Email.id)
        )

        total_created = 0
        offset = 0

        while True:
            rows = self.db.execute(stmt.offset(offset).limit(batch_size)).all()
            if not rows:
                break
            batch_created = self._process_batch(rows, contact_lookup)
            total_created += batch_created
            offset += batch_size

        logger.info(
            "Built %d EmailParticipant records for contact %s (%s)",
            total_created,
            contact_id,
            contact_email,
        )
        return total_created

    def _build_contact_lookup(self) -> dict[str, UUID]:
        """Build a {lowercase_email: contact_id} lookup from all contacts."""
        stmt = select(Contact.id, Contact.email).where(Contact.user_id == self.user_id)
        rows = self.db.execute(stmt).all()
        return {email.lower(): cid for cid, email in rows if email}

    def _process_batch(
        self,
        email_rows: list,
        contact_lookup: dict[str, UUID],
    ) -> int:
        """
        Process a batch of emails and bulk-insert EmailParticipant records.

        Args:
            email_rows: List of (email_id, sender_email, recipient_emails) tuples.
            contact_lookup: {lowercase_email: contact_id} mapping.

        Returns:
            Number of new rows inserted.
        """
        values: list[dict] = []

        for email_id, sender_email, recipient_emails in email_rows:
            # Sender -> contact
            if sender_email:
                sender_normalized = self._parse_email_address(sender_email)
                sender_contact_id = contact_lookup.get(sender_normalized)
                if sender_contact_id:
                    values.append(
                        {
                            "id": uuid_mod.uuid4(),
                            "email_id": email_id,
                            "contact_id": sender_contact_id,
                            "role": "sender",
                        }
                    )

            # Recipients -> contacts
            if recipient_emails:
                for raw_recipient in recipient_emails.split(","):
                    raw_recipient = raw_recipient.strip()
                    if not raw_recipient:
                        continue
                    recipient_normalized = self._parse_email_address(raw_recipient)
                    if not recipient_normalized:
                        continue
                    recipient_contact_id = contact_lookup.get(recipient_normalized)
                    if recipient_contact_id:
                        values.append(
                            {
                                "id": uuid_mod.uuid4(),
                                "email_id": email_id,
                                "contact_id": recipient_contact_id,
                                "role": "to",
                            }
                        )

        if not values:
            return 0

        # Bulk insert with ON CONFLICT DO NOTHING for the unique constraint
        stmt = pg_insert(EmailParticipant.__table__).values(values)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_email_contact_role")
        result = self.db.execute(stmt)
        self.db.commit()

        return result.rowcount

    @staticmethod
    def _parse_email_address(raw: str) -> str | None:
        """
        Extract and normalize an email address from various formats.

        Handles:
        - Plain email: "user@example.com"
        - Display name format: "John Doe <user@example.com>"
        """
        if not raw:
            return None

        raw = raw.strip()

        # Try bracket format first: "Name <email>"
        match = _EMAIL_BRACKET_RE.search(raw)
        if match:
            email = match.group(1).strip().lower()
        else:
            email = raw.strip().lower()

        # Basic validation
        if "@" not in email or " " in email:
            return None

        return email
