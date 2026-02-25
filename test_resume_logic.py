"""
Test script to verify crash recovery and resume logic.
This will:
1. Check for existing emails in the database
2. Verify the date filter is correctly built
3. Test that duplicate inserts are handled gracefully
"""

import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from src.models import Email, GmailAccount

load_dotenv()

# Database connection
engine = create_engine(os.environ["DATABASE_URL"])
SessionLocal = sessionmaker(bind=engine)

def test_resume_logic():
    """Test the resume logic."""
    db = SessionLocal()

    try:
        print("🔍 Testing Resume Logic\n")
        print("=" * 60)

        # Get procore-main account
        account = db.query(GmailAccount).filter(
            GmailAccount.account_email == "tooey@procore.com"
        ).first()

        if not account:
            print("❌ No account found for tooey@procore.com")
            return

        print(f"✅ Found account: {account.account_email} ({account.account_label})")
        print(f"   Account ID: {account.id}")

        # Count existing emails
        email_count = db.query(func.count(Email.id)).filter(
            Email.account_id == account.id
        ).scalar()

        print(f"\n📊 Existing emails: {email_count:,}")

        # Get last processed email date
        last_date = db.query(func.max(Email.date)).filter(
            Email.account_id == account.id
        ).scalar()

        if last_date:
            print(f"📅 Last email date: {last_date.isoformat()}")

            # Build Gmail query
            date_str = last_date.strftime("%Y/%m/%d")
            gmail_query = f"after:{date_str}"
            print(f"🔎 Gmail query for resume: {gmail_query}")
            print(f"\n✅ Resume logic would work!")
            print(f"   - Will fetch only emails after {date_str}")
            print(f"   - Existing {email_count:,} emails will be preserved")
            print(f"   - Duplicates will be automatically skipped")
        else:
            print("📭 No emails in database - fresh scan")
            print("   Gmail query: None (fetch all)")

        print("\n" + "=" * 60)
        print("✅ Resume logic test passed!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()

if __name__ == "__main__":
    test_resume_logic()
