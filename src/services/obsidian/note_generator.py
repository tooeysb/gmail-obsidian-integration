"""
Obsidian Note Generator.
Generates Obsidian markdown notes for contacts and emails with YAML frontmatter.
"""

import re
from collections import Counter
from datetime import datetime
from typing import Any

from src.models.contact import Contact
from src.models.email import Email, EmailTag


class NoteGenerator:
    """Generates Obsidian markdown notes for contacts and emails."""

    def generate_contact_note(self, contact: Contact, emails: list[Email]) -> str:
        """
        Generate merged contact note with YAML frontmatter and email history.

        Args:
            contact: Contact model instance
            emails: List of email model instances associated with this contact

        Returns:
            Markdown content for contact note
        """
        # Build YAML frontmatter
        frontmatter = self._build_contact_frontmatter(contact, emails)

        # Build email count per account
        account_counts = Counter(email.account.account_label for email in emails)

        # Build relationship tags
        tags = []
        if contact.relationship_context:
            tags.append(contact.relationship_context)

        # Build markdown content
        lines = [
            "---",
            f"type: contact",
            f"name: {self._escape_yaml(contact.name or contact.email)}",
            f"email: {contact.email}",
            f"accounts: [{', '.join(repr(acc) for acc in sorted(contact.account_sources))}]",
            f"email_count: {contact.email_count}",
        ]

        # Add per-account email counts
        for account_label in sorted(account_counts.keys()):
            lines.append(f"email_count_{self._sanitize_key(account_label)}: {account_counts[account_label]}")

        # Add last contact date
        if contact.last_contact_at:
            lines.append(f"last_contact: {contact.last_contact_at.strftime('%Y-%m-%d')}")

        # Add tags
        if tags:
            tags_str = ", ".join(repr(tag) for tag in tags)
            lines.append(f"tags: [{tags_str}]")

        lines.append("---")
        lines.append("")

        # Add contact header
        lines.append(f"# {contact.name or contact.email}")
        lines.append("")

        # Add email statistics
        lines.append("## Email History")
        lines.append("")
        lines.append(f"- **Total Emails**: {contact.email_count}")
        lines.append(f"- **Accounts**: {', '.join(sorted(contact.account_sources))}")
        if contact.last_contact_at:
            lines.append(f"- **Last Contact**: {contact.last_contact_at.strftime('%Y-%m-%d')}")
        lines.append("")

        # Add account breakdown
        if account_counts:
            lines.append("### By Account")
            lines.append("")
            for account_label in sorted(account_counts.keys()):
                lines.append(f"- **{account_label}**: {account_counts[account_label]} emails")
            lines.append("")

        # Add notes section if present
        if contact.notes:
            lines.append("## Notes")
            lines.append("")
            lines.append(contact.notes)
            lines.append("")

        # Add wikilinks to email notes (most recent first)
        if emails:
            lines.append("## Recent Emails")
            lines.append("")
            sorted_emails = sorted(emails, key=lambda e: e.date, reverse=True)
            for email in sorted_emails[:10]:  # Show most recent 10
                email_link = self._generate_email_wikilink(email)
                date_str = email.date.strftime("%Y-%m-%d")
                subject = email.subject or "(no subject)"
                lines.append(f"- [[{email_link}|{date_str} - {subject}]]")
            lines.append("")

        # Add Dataview query for full email history
        lines.append("## All Emails (Dataview)")
        lines.append("")
        lines.append("```dataview")
        lines.append("TABLE")
        lines.append("  date as Date,")
        lines.append("  subject as Subject,")
        lines.append("  account as Account")
        lines.append("FROM \"Emails\"")
        lines.append(f'WHERE contains(from, "[[{self._sanitize_filename(contact.name or contact.email)}]]")')
        lines.append("SORT date DESC")
        lines.append("```")

        return "\n".join(lines)

    def generate_email_note(self, email: Email, tags: list[EmailTag]) -> str:
        """
        Generate email note with YAML frontmatter.

        Args:
            email: Email model instance
            tags: List of EmailTag model instances

        Returns:
            Markdown content for email note
        """
        # Extract year and month
        year = email.date.year
        month = email.date.month

        # Build YAML frontmatter
        lines = [
            "---",
            f"type: email",
            f"date: {email.date.isoformat()}",
            f"year: {year}",
            f"month: {month}",
            f"from: [[{self._sanitize_filename(email.sender_name or email.sender_email)}]]",
            f"to: {self._escape_yaml(email.recipient_emails)}",
            f"subject: {self._escape_yaml(email.subject or '(no subject)')}",
            f"account: {email.account.account_label}",
            f"message_id: {email.gmail_message_id}",
        ]

        # Add tags grouped by category
        if tags:
            tag_list = [f'"{tag.tag}"' for tag in tags]
            lines.append(f"tags: [{', '.join(tag_list)}]")

        # Add attachment info
        lines.append(f"has_attachments: {str(email.has_attachments).lower()}")

        lines.append("---")
        lines.append("")

        # Add email header
        subject = email.subject or "(no subject)"
        lines.append(f"# {subject}")
        lines.append("")

        # Add metadata section
        lines.append("## Metadata")
        lines.append("")
        lines.append(f"- **From**: [[{self._sanitize_filename(email.sender_name or email.sender_email)}]]")
        lines.append(f"- **To**: {email.recipient_emails}")
        lines.append(f"- **Date**: {email.date.strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"- **Account**: {email.account.account_label}")
        if email.has_attachments:
            lines.append(f"- **Attachments**: {email.attachment_count}")
        lines.append("")

        # Add summary section
        if email.summary:
            lines.append("## Summary")
            lines.append("")
            lines.append(email.summary)
            lines.append("")

        # Add detected themes by category
        if tags:
            lines.append("## Detected Themes")
            lines.append("")

            # Group tags by category
            tags_by_category = {}
            for tag in tags:
                if tag.tag_category not in tags_by_category:
                    tags_by_category[tag.tag_category] = []
                tags_by_category[tag.tag_category].append(tag)

            # Display tags by category
            for category in sorted(tags_by_category.keys()):
                category_tags = tags_by_category[category]
                lines.append(f"### {category.title()}")
                lines.append("")
                for tag in category_tags:
                    confidence_str = f" ({tag.confidence:.2f})" if tag.confidence else ""
                    lines.append(f"- `{tag.tag}`{confidence_str}")
                lines.append("")

        return "\n".join(lines)

    def _build_contact_frontmatter(self, contact: Contact, emails: list[Email]) -> dict[str, Any]:
        """Build YAML frontmatter dictionary for contact."""
        frontmatter: dict[str, Any] = {
            "type": "contact",
            "name": contact.name or contact.email,
            "email": contact.email,
            "accounts": contact.account_sources,
            "email_count": contact.email_count,
        }

        # Add per-account email counts
        account_counts = Counter(email.account.account_label for email in emails)
        for account_label, count in account_counts.items():
            key = f"email_count_{self._sanitize_key(account_label)}"
            frontmatter[key] = count

        # Add last contact date
        if contact.last_contact_at:
            frontmatter["last_contact"] = contact.last_contact_at.strftime("%Y-%m-%d")

        # Add tags
        if contact.relationship_context:
            frontmatter["tags"] = [contact.relationship_context]

        return frontmatter

    def _generate_email_wikilink(self, email: Email) -> str:
        """
        Generate wikilink path for email note.

        Returns:
            Wikilink path (e.g., "Emails/2024/01/email-filename")
        """
        year = email.date.year
        month = email.date.month
        # Generate filename from date and subject
        date_prefix = email.date.strftime("%Y%m%d-%H%M%S")
        subject_slug = self._slugify(email.subject or "no-subject")
        filename = f"{date_prefix}-{subject_slug}"
        return f"Emails/{year}/{month:02d}/{filename}"

    def _sanitize_filename(self, name: str) -> str:
        """
        Sanitize name for use in filename.

        Args:
            name: Name to sanitize

        Returns:
            Sanitized name safe for filenames
        """
        # Remove or replace problematic characters
        name = re.sub(r'[<>:"/\\|?*]', '', name)
        # Replace spaces with underscores
        name = name.replace(' ', '_')
        # Limit length
        return name[:100]

    def _slugify(self, text: str) -> str:
        """
        Convert text to URL-safe slug.

        Args:
            text: Text to slugify

        Returns:
            Slugified text
        """
        # Convert to lowercase
        text = text.lower()
        # Replace spaces and special chars with hyphens
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        # Limit length
        return text[:50].strip('-')

    def _sanitize_key(self, key: str) -> str:
        """
        Sanitize key for use in YAML frontmatter.

        Args:
            key: Key to sanitize

        Returns:
            Sanitized key
        """
        # Replace hyphens and spaces with underscores
        return key.replace('-', '_').replace(' ', '_')

    def _escape_yaml(self, value: str | None) -> str:
        """
        Escape value for YAML frontmatter.

        Args:
            value: Value to escape

        Returns:
            Escaped value safe for YAML
        """
        if value is None:
            return ""

        # If value contains special YAML characters, quote it
        if any(char in value for char in [':', '#', '[', ']', '{', '}', ',', '&', '*', '!', '|', '>', '@', '`']):
            # Escape quotes and wrap in quotes
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'

        return value
