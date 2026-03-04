"""
Generate a voice profile by analyzing sent emails.

Usage:
    python generate_voice_profile.py --user-id <UUID> [--sample-size 1000] [--profile-name default]
"""

import argparse
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.config import settings
from src.services.voice.profile_generator import VoiceProfileGenerator

engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(bind=engine)


def main():
    parser = argparse.ArgumentParser(description="Generate voice profile from sent emails")
    parser.add_argument("--user-id", required=True, help="User UUID")
    parser.add_argument("--sample-size", type=int, default=1000, help="Max emails to analyze")
    parser.add_argument("--profile-name", default="default", help="Profile name")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        generator = VoiceProfileGenerator(db)
        profile = generator.generate_profile(
            user_id=args.user_id,
            profile_name=args.profile_name,
            sample_size=args.sample_size,
        )

        print(f"Voice profile generated successfully!")
        print(f"  Profile ID: {profile.id}")
        print(f"  Profile name: {profile.profile_name}")
        print(f"  Emails analyzed: {profile.sample_count}")
        print(f"  Generated at: {profile.generated_at}")

        if profile.profile_data:
            import json
            print(f"\nProfile preview:")
            print(json.dumps(profile.profile_data, indent=2)[:2000])

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
