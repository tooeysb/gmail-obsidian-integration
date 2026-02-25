"""
Quick Obsidian vault generator - skips database tagging, creates files directly.
"""

import os
from datetime import datetime
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Email, GmailAccount

load_dotenv()

# Database connection
engine = create_engine(os.environ["DATABASE_URL"])
SessionLocal = sessionmaker(bind=engine)


def quick_generate_vault():
    """Generate Obsidian vault quickly without database tagging."""
    db = SessionLocal()

    try:
        print("\n" + "=" * 80)
        print("🚀 QUICK OBSIDIAN VAULT GENERATION")
        print("=" * 80 + "\n")

        # Get all emails with their accounts in one query
        print("📊 Loading emails from database...")
        emails = db.query(Email).join(GmailAccount).order_by(Email.date.desc()).all()
        print(f"✅ Found {len(emails):,} emails\n")

        if not emails:
            print("❌ No emails found!")
            return

        # Create vault
        vault_path = Path("/Users/tooeycourtemanche/Documents/Obsidian Vault - Gmail")
        print(f"📁 Creating vault at: {vault_path}")
        vault_path.mkdir(exist_ok=True)
        (vault_path / ".obsidian").mkdir(exist_ok=True)

        # Create basic Obsidian config
        config_path = vault_path / ".obsidian" / "config.json"
        config_path.write_text('{"baseFontSize": 16}')

        contacts_dir = vault_path / "Contacts"
        contacts_dir.mkdir(exist_ok=True)

        emails_dir = vault_path / "Emails"
        emails_dir.mkdir(exist_ok=True)

        print(f"✅ Vault structure created\n")

        # Group emails by sender
        print("👥 Processing contacts...")
        emails_by_sender = defaultdict(list)
        for email in emails:
            emails_by_sender[email.sender_email].append(email)

        # Create contact notes
        for i, (sender_email, sender_emails) in enumerate(emails_by_sender.items(), 1):
            if i % 50 == 0:
                print(f"   {i}/{len(emails_by_sender)} contacts...")

            sender_name = sender_emails[0].sender_name or sender_email.split("@")[0]
            domain = sender_email.split("@")[-1]

            contact_note = f"""---
type: contact
email: {sender_email}
name: {sender_name}
email_count: {len(sender_emails)}
last_contact: {sender_emails[0].date.isoformat()}
domain: {domain}
---

# {sender_name}

**Email:** {sender_email}
**Emails:** {len(sender_emails)}
**Last Contact:** {sender_emails[0].date.strftime('%B %d, %Y')}

## Recent Emails

"""
            for email in sender_emails[:10]:
                title = email.subject or "(No subject)"
                date = email.date.strftime("%Y-%m-%d")
                contact_note += f"- [[{date} - {title[:50]}]]\n"

            filename = sender_name.replace("/", "-").replace(":", "-")[:100] + ".md"
            (contacts_dir / filename).write_text(contact_note)

        print(f"✅ Created {len(emails_by_sender)} contact notes\n")

        # Create email notes
        print("📧 Creating email notes...")
        for i, email in enumerate(emails, 1):
            if i % 100 == 0:
                print(f"   {i}/{len(emails)} emails...")

            # Create folder structure
            year_dir = emails_dir / str(email.date.year)
            month_dir = year_dir / f"{email.date.month:02d}"
            month_dir.mkdir(parents=True, exist_ok=True)

            # Generate tags
            account = email.account
            domain = email.sender_email.split("@")[-1] if email.sender_email else "unknown"
            tags = [
                f"#account/{account.account_label}",
                f"#domain/{domain}",
                f"#year/{email.date.year}",
            ]

            # Create note
            title = email.subject or "(No subject)"
            sender_name = email.sender_name or email.sender_email

            email_note = f"""---
type: email
date: {email.date.isoformat()}
from: "[[{sender_name}]]"
subject: "{title}"
tags: {tags}
---

# 📧 {title}

**From:** [[{sender_name}]] <{email.sender_email}>
**Date:** {email.date.strftime('%B %d, %Y at %I:%M %p')}

## Summary

{email.summary or 'No summary available'}

## Tags

{' '.join(tags)}

---

**Related:** [[{sender_name}]]
"""

            filename = f"{email.date.strftime('%Y-%m-%d')} - {title[:50]}.md"
            filename = filename.replace("/", "-").replace(":", "-").replace('"', "'")
            (month_dir / filename).write_text(email_note)

        print(f"✅ Created {len(emails):,} email notes\n")

        print("=" * 80)
        print("✅ VAULT GENERATION COMPLETE!")
        print("=" * 80 + "\n")
        print(f"📂 Vault location: {vault_path}\n")
        print("📊 Summary:")
        print(f"   • {len(emails):,} email notes")
        print(f"   • {len(emails_by_sender)} contact notes")
        print(f"   • {len(set(e.date.year for e in emails))} years of emails")
        print(f"\n🎯 Open Obsidian and select: {vault_path}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()


if __name__ == "__main__":
    quick_generate_vault()
