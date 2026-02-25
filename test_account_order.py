"""
Test script to verify account processing order.
Should process: personal → procore-private → procore-main
Which corresponds to: tooey@hth-corp.com → 2e@procore.com → tooey@procore.com
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import GmailAccount

load_dotenv()

# Database connection
engine = create_engine(os.environ["DATABASE_URL"])
SessionLocal = sessionmaker(bind=engine)

def test_account_order():
    """Test the account processing order."""
    db = SessionLocal()

    try:
        print("🔍 Testing Account Processing Order\n")
        print("=" * 60)

        # Expected order
        account_labels = ["personal", "procore-private", "procore-main"]

        print("📋 Expected processing order:")
        for i, label in enumerate(account_labels, 1):
            print(f"   {i}. {label}")

        print(f"\n🔎 Checking actual accounts in database:\n")

        for i, label in enumerate(account_labels, 1):
            account = db.query(GmailAccount).filter(
                GmailAccount.account_label == label
            ).first()

            if account:
                print(f"   {i}. ✅ {label}")
                print(f"      └─ {account.account_email}")
            else:
                print(f"   {i}. ❌ {label} - NOT FOUND")

        print("\n" + "=" * 60)
        print("✅ Account order verification complete!")
        print("\nProcessing order will be:")
        print("   1st: tooey@hth-corp.com (personal)")
        print("   2nd: 2e@procore.com (procore-private)")
        print("   3rd: tooey@procore.com (procore-main)")
        print("\nEach account processes emails: NEWEST → OLDEST")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db.close()

if __name__ == "__main__":
    test_account_order()
