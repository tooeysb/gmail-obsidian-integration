"""
Relationship Intelligence Vault Generator.

Creates an Obsidian vault with People profiles, Thread notes,
and Dataview-powered index dashboards.
"""

import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from src.models.company import Company
from src.models.contact import Contact
from src.models.email import Email, EmailTag
from src.models.relationship_profile import RelationshipProfile

logger = logging.getLogger(__name__)


class RelationshipVaultGenerator:
    """Generates Obsidian vault files from relationship profiles."""

    def __init__(self, vault_path: str | Path):
        self.vault_path = Path(vault_path)
        self.people_dir = self.vault_path / "People"
        self.threads_dir = self.vault_path / "Threads"
        self.indexes_dir = self.vault_path / "Indexes"

    def generate_vault(self, user_id: UUID, db: Session) -> dict[str, int]:
        """
        Generate the complete relationship vault.

        Args:
            user_id: User UUID.
            db: SQLAlchemy session.

        Returns:
            Stats dict with counts of generated files.
        """
        logger.info(f"Generating relationship vault at {self.vault_path}")

        # Create directories
        self.people_dir.mkdir(parents=True, exist_ok=True)
        self.threads_dir.mkdir(parents=True, exist_ok=True)
        self.indexes_dir.mkdir(parents=True, exist_ok=True)

        # Load all profiled contacts
        profiles = (
            db.query(RelationshipProfile)
            .filter(
                RelationshipProfile.user_id == user_id,
                RelationshipProfile.profiled_at.isnot(None),
            )
            .order_by(RelationshipProfile.total_email_count.desc())
            .all()
        )

        logger.info(f"Generating notes for {len(profiles)} profiled contacts")

        # Bulk-load contacts and companies for CRM enrichment in person notes
        contact_emails = [p.contact_email for p in profiles]
        contacts_by_email: dict[str, Contact] = {}
        companies_by_id: dict[Any, Company] = {}

        if contact_emails:
            contacts = (
                db.query(Contact)
                .filter(
                    Contact.user_id == user_id,
                    Contact.email.in_(contact_emails),
                )
                .all()
            )
            contacts_by_email = {c.email.lower(): c for c in contacts}

            # Load companies for contacts that have company_id
            company_ids = {c.company_id for c in contacts if c.company_id}
            if company_ids:
                companies = db.query(Company).filter(Company.id.in_(company_ids)).all()
                companies_by_id = {c.id: c for c in companies}

        stats = {"people": 0, "threads": 0, "indexes": 0}

        # Generate People notes
        for profile in profiles:
            contact = contacts_by_email.get(profile.contact_email.lower())
            company = companies_by_id.get(contact.company_id) if contact and contact.company_id else None
            self._generate_person_note(profile, contact, company)
            stats["people"] += 1

        logger.info(f"Generated {stats['people']} People notes")

        # Generate Thread notes for profiled contacts
        thread_count = self._generate_thread_notes(user_id, profiles, db)
        stats["threads"] = thread_count

        logger.info(f"Generated {stats['threads']} Thread notes")

        # Generate Index dashboards
        self._generate_indexes(profiles)
        stats["indexes"] = 5

        logger.info(f"Vault generation complete: {stats}")
        return stats

    def _generate_person_note(
        self,
        profile: RelationshipProfile,
        contact: Contact | None = None,
        company: Company | None = None,
    ) -> None:
        """Generate a People/ note for a single contact."""
        display_name = profile.contact_name or profile.contact_email
        filename = self._sanitize_filename(display_name) + ".md"
        filepath = self.people_dir / filename

        pd = profile.profile_data or {}

        lines = []

        # YAML frontmatter
        lines.append("---")
        lines.append("type: person")
        lines.append(f"name: {self._escape_yaml(display_name)}")
        lines.append(f"email: {profile.contact_email}")
        lines.append(f"relationship: {profile.relationship_type}")
        accounts_str = ", ".join(profile.account_sources or [])
        lines.append(f"accounts: [{accounts_str}]")
        if contact and contact.title:
            lines.append(f"title: {self._escape_yaml(contact.title)}")
        if company:
            lines.append(f"company: {self._escape_yaml(company.name)}")
        if contact and contact.contact_type:
            lines.append(f"contact_type: {contact.contact_type}")
        if contact and contact.is_vip:
            lines.append("is_vip: true")
        if contact and contact.tags:
            tags_str = ", ".join(contact.tags)
            lines.append(f"tags: [{tags_str}]")
        if profile.first_exchange_date:
            lines.append(f"first_exchange: {profile.first_exchange_date.strftime('%Y-%m-%d')}")
        if profile.last_exchange_date:
            lines.append(f"last_exchange: {profile.last_exchange_date.strftime('%Y-%m-%d')}")
        lines.append(f"total_emails: {profile.total_email_count}")
        lines.append(f"sent: {profile.sent_count}")
        lines.append(f"received: {profile.received_count}")
        lines.append(f"threads: {profile.thread_count}")
        lines.append(f"sentiment_trend: {pd.get('sentiment_trend', 'stable')}")
        lines.append("---")
        lines.append("")

        # Header
        lines.append(f"# {display_name}")
        lines.append("")

        # Relationship Summary
        if pd.get("relationship_summary"):
            lines.append("## Relationship Summary")
            lines.append("")
            lines.append(pd["relationship_summary"])
            lines.append("")

        # Business Context (from CRM enrichment)
        if contact and (contact.title or company or contact.contact_type):
            lines.append("## Business Context")
            lines.append("")
            if contact.title:
                lines.append(f"- **Title**: {contact.title}")
            if company:
                company_link = f"[[{self._sanitize_filename(company.name)}|{company.name}]]"
                lines.append(f"- **Company**: {company_link}")
                if company.industry:
                    lines.append(f"- **Industry**: {company.industry}")
                if company.arr:
                    lines.append(f"- **Company ARR**: ${company.arr:,.0f}")
                if company.account_tier:
                    lines.append(f"- **Account Tier**: {company.account_tier}")
            if contact.contact_type:
                lines.append(f"- **Contact Type**: {contact.contact_type}")
            if contact.is_vip:
                lines.append("- **VIP**: Yes")
            if contact.tags:
                lines.append(f"- **Tags**: {', '.join(contact.tags)}")

            # Customer data from RelationshipProfile
            cd = profile.customer_data or {}
            if cd.get("cab_status"):
                lines.append(f"- **CAB Status**: {cd['cab_status']}")
            if cd.get("cab_year"):
                lines.append(f"- **CAB Year**: {cd['cab_year']}")
            if cd.get("renewal_date"):
                lines.append(f"- **Renewal Date**: {cd['renewal_date']}")
            if cd.get("account_executive"):
                lines.append(f"- **Account Executive**: {cd['account_executive']}")
            if cd.get("csm"):
                lines.append(f"- **CSM**: {cd['csm']}")
            lines.append("")

        # What They Think of Me
        if pd.get("perceived_opinion"):
            lines.append("## What They Think of Me")
            lines.append("")
            lines.append(pd["perceived_opinion"])
            lines.append("")

        # Primary Topics
        topics = pd.get("primary_topics", [])
        if topics:
            lines.append("## Primary Topics")
            lines.append("")
            for topic in topics:
                lines.append(f"- {topic}")
            lines.append("")

        # Communication Style
        if pd.get("communication_style"):
            lines.append("## Communication Style")
            lines.append("")
            lines.append(pd["communication_style"])
            lines.append("")

        # Notable Events
        events = pd.get("notable_events", [])
        if events:
            lines.append("## Notable Events")
            lines.append("")
            for event in events:
                lines.append(f"- {event}")
            lines.append("")

        # Conflicts
        conflicts = pd.get("conflicts", [])
        if conflicts:
            lines.append("## Conflicts")
            lines.append("")
            for conflict in conflicts:
                if isinstance(conflict, dict):
                    desc = conflict.get("description", "Unknown")
                    date = conflict.get("approximate_date", "Unknown date")
                    resolution = conflict.get("resolution_status", "unknown")
                    lines.append(f"### {desc}")
                    lines.append(f"- **Period**: {date}")
                    lines.append(f"- **Resolution**: {resolution}")
                    lines.append("")
                else:
                    lines.append(f"- {conflict}")
            lines.append("")

        # Key Quotes
        quotes = pd.get("key_quotes", [])
        if quotes:
            lines.append("## Key Quotes")
            lines.append("")
            for quote in quotes:
                lines.append(f"> {quote}")
                lines.append("")

        # Communication Patterns
        lines.append("## Communication Patterns")
        lines.append("")
        lines.append(f"- **Total Emails**: {profile.total_email_count}")
        lines.append(f"- **Sent**: {profile.sent_count}")
        lines.append(f"- **Received**: {profile.received_count}")
        lines.append(f"- **Threads**: {profile.thread_count}")
        if profile.first_exchange_date:
            lines.append(
                f"- **First Exchange**: {profile.first_exchange_date.strftime('%Y-%m-%d')}"
            )
        if profile.last_exchange_date:
            lines.append(
                f"- **Last Exchange**: {profile.last_exchange_date.strftime('%Y-%m-%d')}"
            )
        lines.append(f"- **Accounts**: {accounts_str}")
        lines.append("")

        # Dataview: Recent Threads
        safe_name = self._sanitize_filename(display_name)
        lines.append("## Recent Threads")
        lines.append("")
        lines.append("```dataview")
        lines.append('TABLE date as "Date", subject as "Subject", message_count as "Messages"')
        lines.append('FROM "Threads"')
        lines.append(f'WHERE contains(participants, "[[{safe_name}]]")')
        lines.append("SORT date DESC")
        lines.append("LIMIT 20")
        lines.append("```")

        filepath.write_text("\n".join(lines), encoding="utf-8")

    def _generate_thread_notes(
        self,
        user_id: UUID,
        profiles: list[RelationshipProfile],
        db: Session,
    ) -> int:
        """
        Generate Thread/ notes grouped by gmail_thread_id.

        Only generates threads involving profiled contacts.
        """
        # Collect all contact emails for filtering
        contact_emails = {p.contact_email for p in profiles}
        contact_name_map = {
            p.contact_email: (p.contact_name or p.contact_email) for p in profiles
        }

        # Find all threads involving these contacts
        thread_ids_query = (
            db.query(Email.gmail_thread_id)
            .filter(
                Email.user_id == user_id,
                Email.gmail_thread_id.isnot(None),
                (
                    func.lower(Email.sender_email).in_(contact_emails)
                    | func.lower(Email.recipient_emails).op("~*")(
                        "|".join(re.escape(e) for e in contact_emails)
                    )
                ),
            )
            .distinct()
            .all()
        )

        thread_ids = [row[0] for row in thread_ids_query]
        logger.info(f"Found {len(thread_ids)} threads to generate")

        count = 0
        # Process threads in batches to manage memory
        batch_size = 500
        for batch_start in range(0, len(thread_ids), batch_size):
            batch_thread_ids = thread_ids[batch_start : batch_start + batch_size]

            # Load all emails for this batch of threads
            emails_by_thread: dict[str, list[Email]] = defaultdict(list)
            thread_emails = (
                db.query(Email)
                .options(selectinload(Email.tags))
                .filter(
                    Email.user_id == user_id,
                    Email.gmail_thread_id.in_(batch_thread_ids),
                )
                .order_by(Email.date.asc())
                .all()
            )

            for email in thread_emails:
                emails_by_thread[email.gmail_thread_id].append(email)

            for thread_id, emails in emails_by_thread.items():
                if not emails:
                    continue

                self._write_thread_note(
                    thread_id, emails, contact_emails, contact_name_map
                )
                count += 1

            if batch_start + batch_size < len(thread_ids):
                logger.info(f"Thread generation progress: {count}/{len(thread_ids)}")

        return count

    def _write_thread_note(
        self,
        thread_id: str,
        emails: list[Email],
        contact_emails: set[str],
        contact_name_map: dict[str, str],
    ) -> None:
        """Write a single thread note."""
        first_email = emails[0]
        last_email = emails[-1]
        subject = first_email.subject or "(no subject)"

        # Build participants list with wikilinks
        participants_set: set[str] = set()
        for email in emails:
            sender = email.sender_email.lower().strip()
            if sender in contact_name_map:
                name = contact_name_map[sender]
                participants_set.add(f"[[{self._sanitize_filename(name)}]]")

            # Check recipients too
            if email.recipient_emails:
                for addr in email.recipient_emails.split(","):
                    addr = addr.strip().lower()
                    if "<" in addr and ">" in addr:
                        addr = addr[addr.index("<") + 1 : addr.index(">")].strip()
                    if addr in contact_name_map:
                        name = contact_name_map[addr]
                        participants_set.add(f"[[{self._sanitize_filename(name)}]]")

        participants = sorted(participants_set)

        # Generate filename
        date_str = first_email.date.strftime("%Y-%m-%d")
        subject_slug = self._slugify(subject)
        filename = f"{date_str}_{subject_slug}.md"
        filepath = self.threads_dir / filename

        lines = []

        # YAML frontmatter
        lines.append("---")
        lines.append("type: thread")
        lines.append(f"thread_id: {thread_id}")
        parts_str = ", ".join(f'"{p}"' for p in participants)
        lines.append(f"participants: [{parts_str}]")
        lines.append(f"date: {first_email.date.strftime('%Y-%m-%d')}")
        lines.append(f"last_reply: {last_email.date.strftime('%Y-%m-%d')}")
        lines.append(f"message_count: {len(emails)}")
        lines.append(f"subject: {self._escape_yaml(subject)}")
        lines.append("---")
        lines.append("")

        # Header
        lines.append(f"# {subject}")
        lines.append("")

        # Thread timeline
        lines.append("## Thread Timeline")
        lines.append("")

        for email in emails:
            sender_name = email.sender_name or email.sender_email
            sender_email = email.sender_email.lower().strip()

            # Make sender a wikilink if they're a known contact
            if sender_email in contact_name_map:
                name = contact_name_map[sender_email]
                sender_display = f"[[{self._sanitize_filename(name)}|{sender_name}]]"
            else:
                sender_display = sender_name

            lines.append(f"### {email.date.strftime('%Y-%m-%d %H:%M')} - {sender_display}")
            lines.append("")

            if email.summary:
                lines.append(email.summary)
                lines.append("")

            # Tags
            if email.tags:
                tag_strs = [f"#{t.tag}" for t in email.tags if t.tag_category in ("topic", "sentiment")]
                if tag_strs:
                    lines.append(f"Tags: {' '.join(tag_strs)}")
                    lines.append("")

        filepath.write_text("\n".join(lines), encoding="utf-8")

    def _generate_indexes(self, profiles: list[RelationshipProfile]) -> None:
        """Generate Dataview-powered index dashboards."""
        self._generate_all_contacts_index(profiles)
        self._generate_by_relationship_index(profiles)
        self._generate_recent_activity_index(profiles)
        self._generate_conflict_log_index(profiles)
        self._generate_companies_index()

    def _generate_all_contacts_index(self, profiles: list[RelationshipProfile]) -> None:
        """Generate All_Contacts.md index."""
        lines = [
            "---",
            "type: index",
            "---",
            "",
            "# All Contacts",
            "",
            "```dataview",
            "TABLE",
            '  company as "Company",',
            '  title as "Title",',
            '  relationship as "Type",',
            '  total_emails as "Emails",',
            '  last_exchange as "Last",',
            '  sentiment_trend as "Trend"',
            'FROM "People"',
            "WHERE type = \"person\"",
            "SORT total_emails DESC",
            "```",
        ]
        (self.indexes_dir / "All_Contacts.md").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _generate_by_relationship_index(
        self, profiles: list[RelationshipProfile]
    ) -> None:
        """Generate By_Relationship.md index grouped by type."""
        lines = [
            "---",
            "type: index",
            "---",
            "",
            "# Contacts by Relationship Type",
            "",
        ]

        types = ["coworker", "client", "personal", "vendor", "recruiter", "unknown"]
        for rel_type in types:
            lines.append(f"## {rel_type.title()}")
            lines.append("")
            lines.append("```dataview")
            lines.append("TABLE")
            lines.append('  total_emails as "Emails",')
            lines.append('  last_exchange as "Last Contact",')
            lines.append('  sentiment_trend as "Trend"')
            lines.append('FROM "People"')
            lines.append(f'WHERE relationship = "{rel_type}"')
            lines.append("SORT total_emails DESC")
            lines.append("```")
            lines.append("")

        (self.indexes_dir / "By_Relationship.md").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _generate_recent_activity_index(
        self, profiles: list[RelationshipProfile]
    ) -> None:
        """Generate Recent_Activity.md index sorted by last exchange."""
        lines = [
            "---",
            "type: index",
            "---",
            "",
            "# Recent Activity",
            "",
            "```dataview",
            "TABLE",
            '  relationship as "Type",',
            '  total_emails as "Emails",',
            '  last_exchange as "Last Contact",',
            '  sentiment_trend as "Trend"',
            'FROM "People"',
            "WHERE type = \"person\"",
            "SORT last_exchange DESC",
            "```",
        ]
        (self.indexes_dir / "Recent_Activity.md").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _generate_conflict_log_index(
        self, profiles: list[RelationshipProfile]
    ) -> None:
        """Generate Conflict_Log.md with all detected conflicts."""
        lines = [
            "---",
            "type: index",
            "---",
            "",
            "# Conflict Log",
            "",
            "All detected conflicts and tensions across relationships.",
            "",
        ]

        has_conflicts = False
        for profile in profiles:
            pd = profile.profile_data or {}
            conflicts = pd.get("conflicts", [])
            if not conflicts:
                continue

            has_conflicts = True
            display_name = profile.contact_name or profile.contact_email
            safe_name = self._sanitize_filename(display_name)
            lines.append(f"## [[{safe_name}|{display_name}]]")
            lines.append("")

            for conflict in conflicts:
                if isinstance(conflict, dict):
                    desc = conflict.get("description", "Unknown")
                    date = conflict.get("approximate_date", "Unknown")
                    resolution = conflict.get("resolution_status", "unknown")
                    lines.append(f"- **{desc}** ({date}) - {resolution}")
                else:
                    lines.append(f"- {conflict}")
            lines.append("")

        if not has_conflicts:
            lines.append("*No conflicts detected across any relationships.*")

        (self.indexes_dir / "Conflict_Log.md").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _generate_companies_index(self) -> None:
        """Generate Companies.md index with Dataview dashboard."""
        lines = [
            "---",
            "type: index",
            "---",
            "",
            "# Companies",
            "",
            "## By ARR",
            "",
            "```dataview",
            "TABLE",
            '  arr as "ARR",',
            '  company_type as "Type",',
            '  revenue_segment as "Segment",',
            '  account_tier as "Tier",',
            '  billing_state as "State"',
            'FROM "People"',
            "WHERE company != null",
            "SORT arr DESC",
            "GROUP BY company",
            "```",
            "",
            "## All Companies (People View)",
            "",
            "```dataview",
            "TABLE",
            '  company as "Company",',
            '  title as "Title",',
            '  total_emails as "Emails",',
            '  contact_type as "Type"',
            'FROM "People"',
            "WHERE company != null",
            "SORT company ASC",
            "```",
        ]
        (self.indexes_dir / "Companies.md").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize name for use in filenames."""
        name = re.sub(r'[<>:"/\\|?*]', "", name)
        name = name.replace(" ", "_")
        return name[:100]

    def _slugify(self, text: str) -> str:
        """Convert text to URL-safe slug."""
        text = text.lower()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[-\s]+", "-", text)
        return text[:60].strip("-")

    def _escape_yaml(self, value: str | None) -> str:
        """Escape value for YAML frontmatter."""
        if value is None:
            return ""
        if any(
            c in value
            for c in [":", "#", "[", "]", "{", "}", ",", "&", "*", "!", "|", ">", "@", "`"]
        ):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return value
