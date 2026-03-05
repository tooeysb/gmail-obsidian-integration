"""
SQLAlchemy models package.
All models are imported here for Alembic auto-generation to detect changes.
"""

from src.models.account import GmailAccount
from src.models.base import Base
from src.models.company import Company
from src.models.company_news import CompanyNewsItem
from src.models.contact import Contact
from src.models.contact_enrichment import ContactEnrichment
from src.models.discovered_contact import DiscoveredContact
from src.models.draft_suggestion import DraftSuggestion
from src.models.email import Email, EmailTag
from src.models.email_participant import EmailParticipant
from src.models.email_queue import EmailQueue
from src.models.guardian_event import GuardianEvent
from src.models.job import SyncJob
from src.models.relationship_profile import RelationshipProfile
from src.models.user import User
from src.models.voice_profile import VoiceProfile

__all__ = [
    "Base",
    "User",
    "GmailAccount",
    "Company",
    "CompanyNewsItem",
    "Contact",
    "ContactEnrichment",
    "DiscoveredContact",
    "DraftSuggestion",
    "Email",
    "EmailTag",
    "EmailParticipant",
    "EmailQueue",
    "SyncJob",
    "GuardianEvent",
    "RelationshipProfile",
    "VoiceProfile",
]
