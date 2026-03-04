"""
Generate Obsidian relationship vault from processed email data.

Usage:
    python generate_vault.py [--discover-only] [--profile-limit N] [--vault-path PATH]

Steps:
    1. Discover contacts with bidirectional communication (SQL only, fast)
    2. Profile contacts with Claude Haiku (API calls, ~$8-12 for all contacts)
    3. Generate vault files (local filesystem)
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.core.config import settings
from src.core.database import SyncSessionLocal
from src.models.user import User
from src.services.relationships.contact_discovery import discover_contacts
from src.services.relationships.profiler import profile_contacts_batch
from src.services.obsidian.relationship_vault import RelationshipVaultGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Generate Obsidian relationship vault")
    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="Only run contact discovery (no Claude API calls)",
    )
    parser.add_argument(
        "--profile-limit",
        type=int,
        default=None,
        help="Limit number of contacts to profile (useful for testing)",
    )
    parser.add_argument(
        "--vault-path",
        type=str,
        default=None,
        help="Override vault output path",
    )
    parser.add_argument(
        "--vault-only",
        action="store_true",
        help="Skip discovery and profiling, only generate vault from existing profiles",
    )
    args = parser.parse_args()

    vault_path = args.vault_path or settings.obsidian_vault_path
    logger.info(f"Vault path: {vault_path}")

    db = SyncSessionLocal()
    try:
        # Get the user
        user = db.query(User).first()
        if not user:
            logger.error("No user found in database. Run email sync first.")
            sys.exit(1)

        logger.info(f"Using user: {user.email} (id={user.id})")

        if not args.vault_only:
            # Phase 1: Contact Discovery
            logger.info("=" * 60)
            logger.info("PHASE 1: Contact Discovery")
            logger.info("=" * 60)

            contacts = discover_contacts(user.id, db)
            logger.info(f"Discovered {len(contacts)} contacts")

            # Print top 20 by email count
            logger.info("\nTop 20 contacts by email volume:")
            for i, c in enumerate(contacts[:20], 1):
                logger.info(
                    f"  {i:2d}. {c['contact_name'] or c['contact_email']:<40s} "
                    f"({c['total_email_count']:>5d} emails, "
                    f"{c['relationship_type']})"
                )

            if args.discover_only:
                logger.info("\n--discover-only flag set, stopping here.")
                return

            # Phase 2: Relationship Profiling
            logger.info("")
            logger.info("=" * 60)
            logger.info("PHASE 2: Relationship Profiling (Claude Haiku)")
            logger.info("=" * 60)

            contacts_to_profile = contacts
            if args.profile_limit:
                contacts_to_profile = contacts[: args.profile_limit]
                logger.info(f"Limiting to {args.profile_limit} contacts")

            profiled = profile_contacts_batch(contacts_to_profile, user.id, db)
            logger.info(f"Profiled {profiled} contacts")

        # Phase 3: Vault Generation
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 3: Vault Generation")
        logger.info("=" * 60)

        generator = RelationshipVaultGenerator(vault_path)
        stats = generator.generate_vault(user.id, db)

        logger.info("")
        logger.info("=" * 60)
        logger.info("COMPLETE")
        logger.info(f"  People notes: {stats['people']}")
        logger.info(f"  Thread notes: {stats['threads']}")
        logger.info(f"  Index files:  {stats['indexes']}")
        logger.info(f"  Vault path:   {vault_path}")
        logger.info("=" * 60)

    finally:
        db.close()


if __name__ == "__main__":
    main()
