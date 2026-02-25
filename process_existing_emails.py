"""
Process existing emails in database to generate Obsidian vault.
This runs theme detection and vault generation on emails already fetched.
"""

import os
import uuid
from datetime import datetime
from collections import defaultdict

from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.integrations.claude.batch_processor import ThemeBatchProcessor
from src.models import Email, EmailTag, GmailAccount, User
from src.services.obsidian.note_generator import NoteGenerator
from src.services.obsidian.vault_manager import ObsidianVaultManager
from src.services.theme_detection.prompt_template import generate_tags

load_dotenv()

# Database connection
engine = create_engine(os.environ["DATABASE_URL"])
SessionLocal = sessionmaker(bind=engine)


def process_existing_emails():
    """Process emails already in database to generate Obsidian vault."""
    db = SessionLocal()

    try:
        print("\n" + "=" * 80)
        print("🚀 PROCESSING EXISTING EMAILS TO OBSIDIAN VAULT")
        print("=" * 80 + "\n")

        # Get all emails
        emails = db.query(Email).order_by(Email.date.desc()).all()
        print(f"📊 Found {len(emails):,} emails in database\n")

        if not emails:
            print("❌ No emails found in database!")
            return

        # ========================================
        # PHASE 1: Theme Detection (Skip for now - use simple tags)
        # ========================================
        print("🤖 Phase 1: Theme Detection")
        print("-" * 80)
        print("⚠️  Skipping Claude AI for quick demo")
        print("   Using simple rule-based tags instead\n")

        # Simple rule-based tagging for demo
        for email in emails:
            # Delete existing tags
            db.query(EmailTag).filter(EmailTag.email_id == email.id).delete()

            # Get account label
            account = db.query(GmailAccount).filter(
                GmailAccount.id == email.account_id
            ).first()

            # Simple tags based on email content
            tags = []

            # Account tag
            if account:
                tag = EmailTag(
                    id=uuid.uuid4(),
                    email_id=email.id,
                    tag=f"account/{account.account_label}",
                    tag_category="account",
                )
                db.add(tag)
                tags.append(f"account/{account.account_label}")

            # Domain tag
            if email.sender_email:
                domain = email.sender_email.split("@")[-1]
                tag = EmailTag(
                    id=uuid.uuid4(),
                    email_id=email.id,
                    tag=f"domain/{domain}",
                    tag_category="domain",
                )
                db.add(tag)
                tags.append(f"domain/{domain}")

            # Subject-based tags
            if email.subject:
                subject_lower = email.subject.lower()
                # Check for common keywords
                if any(word in subject_lower for word in ["meeting", "calendar", "invite"]):
                    tag = EmailTag(
                        id=uuid.uuid4(),
                        email_id=email.id,
                        tag="topic/meeting",
                        tag_category="topic",
                    )
                    db.add(tag)
                    tags.append("topic/meeting")

                if any(word in subject_lower for word in ["urgent", "important", "asap"]):
                    tag = EmailTag(
                        id=uuid.uuid4(),
                        email_id=email.id,
                        tag="sentiment/urgent",
                        tag_category="sentiment",
                    )
                    db.add(tag)
                    tags.append("sentiment/urgent")

            email.tags_list = tags  # Store for later use

        db.commit()
        print(f"✅ Tagged {len(emails):,} emails\n")

        # ========================================
        # PHASE 2: Generate Obsidian Vault
        # ========================================
        print("📝 Phase 2: Vault Generation")
        print("-" * 80)

        vault_path = "/Users/tooeycourtemanche/Documents/Obsidian Vault - Gmail"
        vault_manager = ObsidianVaultManager(vault_path)
        note_generator = NoteGenerator()

        # Initialize vault
        print(f"📁 Creating vault at: {vault_path}")
        vault_manager.initialize_vault()

        # Group emails by sender
        emails_by_sender = defaultdict(list)
        for email in emails:
            emails_by_sender[email.sender_email].append(email)

        print(f"👥 Found {len(emails_by_sender)} unique senders\n")

        # Create contact notes
        print("👥 Creating contact notes...")
        contacts_dir = vault_manager.vault_path / "Contacts"
        contacts_dir.mkdir(exist_ok=True)

        for sender_email, sender_emails in emails_by_sender.items():
            # Get sender name from first email
            sender_name = sender_emails[0].sender_name or sender_email.split("@")[0]

            # Generate contact note
            contact_note = f"""---
type: contact
email: {sender_email}
name: {sender_name}
email_count: {len(sender_emails)}
last_contact: {sender_emails[0].date.isoformat()}
created_at: {datetime.utcnow().isoformat()}
---

# {sender_name}

**Email:** {sender_email}
**Total Emails:** {len(sender_emails)}
**Last Contact:** {sender_emails[0].date.strftime('%B %d, %Y')}

## Recent Emails

"""
            # Add links to recent emails
            for email in sorted(sender_emails, key=lambda e: e.date, reverse=True)[:10]:
                email_title = email.subject or "(No subject)"
                email_date = email.date.strftime("%Y-%m-%d")
                contact_note += f"- [[{email_date} - {email_title}]]\n"

            # Write contact note
            contact_filename = sender_name.replace("/", "-").replace(":", "-") + ".md"
            contact_path = contacts_dir / contact_filename
            contact_path.write_text(contact_note)

        print(f"✅ Created {len(emails_by_sender)} contact notes\n")

        # Create email notes
        print("📧 Creating email notes...")
        emails_dir = vault_manager.vault_path / "Emails"
        emails_dir.mkdir(exist_ok=True)

        for email in emails:
            # Create year/month folders
            year_dir = emails_dir / str(email.date.year)
            month_dir = year_dir / f"{email.date.month:02d}"
            month_dir.mkdir(parents=True, exist_ok=True)

            # Get tags
            email_tags = db.query(EmailTag).filter(EmailTag.email_id == email.id).all()
            tags = [f"#{tag.tag}" for tag in email_tags]

            # Generate email note
            email_title = email.subject or "(No subject)"
            email_date = email.date.strftime("%Y-%m-%d")
            sender_name = email.sender_name or email.sender_email

            email_note = f"""---
type: email
date: {email.date.isoformat()}
from: "[[{sender_name}]]"
to: {email.recipient_emails}
subject: "{email_title}"
tags: {tags}
created_at: {datetime.utcnow().isoformat()}
---

# 📧 {email_title}

**From:** [[{sender_name}]] <{email.sender_email}>
**To:** {email.recipient_emails}
**Date:** {email.date.strftime('%B %d, %Y %I:%M %p')}

## Summary

{email.summary or 'No summary available'}

## Tags

{' '.join(tags) if tags else 'No tags'}

---

**Gmail Message ID:** `{email.gmail_message_id}`

## Related
- Contact: [[{sender_name}]]
"""

            # Write email note
            email_filename = f"{email_date} - {email_title[:50]}.md"
            # Clean filename
            email_filename = email_filename.replace("/", "-").replace(":", "-").replace('"', "'")
            email_path = month_dir / email_filename
            email_path.write_text(email_note)

        print(f"✅ Created {len(emails):,} email notes\n")

        print("=" * 80)
        print("✅ VAULT GENERATION COMPLETE!")
        print("=" * 80 + "\n")
        print(f"📂 Vault location: {vault_path}\n")
        print("📊 Summary:")
        print(f"   - {len(emails):,} email notes")
        print(f"   - {len(emails_by_sender)} contact notes")
        print(f"\n🎯 Next steps:")
        print(f"   1. Open Obsidian")
        print(f"   2. Open vault: {vault_path}")
        print(f"   3. Explore your emails!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()


if __name__ == "__main__":
    process_existing_emails()
