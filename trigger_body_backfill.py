"""
Trigger body backfill for all Gmail accounts.

Usage:
    python trigger_body_backfill.py [--limit N]
"""

import argparse
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.models import GmailAccount, User
from src.worker.backfill_body_tasks import queue_body_backfill

engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)


def main():
    parser = argparse.ArgumentParser(description="Trigger body backfill for Gmail accounts")
    parser.add_argument("--limit", type=int, default=None, help="Max emails to backfill per account")
    parser.add_argument("--account", type=str, default=None, help="Specific account label to backfill")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        # Get active accounts
        query = db.query(GmailAccount).filter(GmailAccount.is_active == True)  # noqa: E712
        if args.account:
            query = query.filter(GmailAccount.account_label == args.account)

        accounts = query.all()

        if not accounts:
            print("No active accounts found")
            sys.exit(1)

        print(f"Found {len(accounts)} accounts to backfill")

        for account in accounts:
            print(f"  Queuing backfill for {account.account_email} ({account.account_label})")
            queue_body_backfill.delay(str(account.id), limit=args.limit)

        print("Backfill tasks queued. Monitor with: celery -A src.worker.celery_app flower")

    finally:
        db.close()


if __name__ == "__main__":
    main()
