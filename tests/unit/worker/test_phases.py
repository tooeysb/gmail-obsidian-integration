"""
Unit tests for worker phase modules.

Covers:
  - src.worker.phases.email_sync  (_build_sync_queries, _create_email_objects)
  - src.worker.phases.theme_detection (detect_themes)
  - src.worker.phases.vault_generation (generate_vault)
"""

import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.worker.phases.email_sync import _build_sync_queries, _create_email_objects

# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

USER_ID = str(uuid.uuid4())
ACCOUNT_ID = uuid.uuid4()


def _make_email_dict(**overrides) -> dict:
    """Return a minimal Gmail-API-style email dict, with optional field overrides."""
    base = {
        "gmail_message_id": "msg1",
        "gmail_thread_id": "thread1",
        "subject": "Test Subject",
        "sender_email": "sender@example.com",
        "sender_name": "Sender Name",
        "recipient_emails": "recipient@example.com",
        "date": datetime(2024, 1, 15, 12, 0, 0),
        "snippet": "Hello world",
        "body": "Hello world body",
        "has_attachments": False,
        "attachment_count": 0,
    }
    base.update(overrides)
    return base


# ===========================================================================
# email_sync — _build_sync_queries
# ===========================================================================


class TestBuildSyncQueries:
    """Tests for the pure _build_sync_queries helper."""

    def test_no_existing_emails_returns_initial_scan(self):
        """With no oldest/newest date, returns a single 'in:anywhere' scan."""
        queries = _build_sync_queries(oldest_email_date=None, newest_email_date=None)

        assert len(queries) == 1
        assert queries[0]["query"] == "in:anywhere"
        assert "initial scan" in queries[0]["description"]

    def test_forward_only_when_newest_date_exists(self):
        """When only newest_date is set, returns a single forward-sync query."""
        newest = datetime(2024, 6, 1)
        queries = _build_sync_queries(oldest_email_date=None, newest_email_date=newest)

        # Only one query — no gap-fill (no oldest) and no historical (no oldest)
        assert len(queries) == 1
        q = queries[0]
        assert "after:2024/06/01" in q["query"]
        assert "in:anywhere" in q["query"]
        assert "forward sync" in q["description"]

    def test_historical_only_when_oldest_date_exists(self):
        """When only oldest_date is set, returns a single historical-backfill query."""
        oldest = datetime(2024, 1, 1)
        queries = _build_sync_queries(oldest_email_date=oldest, newest_email_date=None)

        # Only one query — no gap-fill (no newest) and no forward (no newest)
        assert len(queries) == 1
        q = queries[0]
        assert "before:2024/01/01" in q["query"]
        assert "in:anywhere" in q["query"]
        assert "historical backfill" in q["description"]

    def test_both_dates_returns_gap_fill_forward_and_historical(self):
        """When both dates are set, returns gap-fill category queries + forward + historical."""
        oldest = datetime(2020, 3, 1)
        newest = datetime(2024, 6, 1)
        queries = _build_sync_queries(oldest_email_date=oldest, newest_email_date=newest)

        # 4 gap-fill categories + 1 forward + 1 historical = 6
        assert len(queries) == 6

        query_strings = [q["query"] for q in queries]
        [q["description"] for q in queries]

        # Gap-fill queries for all four category labels
        for category in ["PROMOTIONS", "SOCIAL", "FORUMS", "UPDATES"]:
            assert f"label:CATEGORY_{category}" in query_strings

        # Forward sync present
        forward_queries = [q for q in queries if "forward sync" in q["description"]]
        assert len(forward_queries) == 1
        assert "after:2024/06/01" in forward_queries[0]["query"]

        # Historical backfill present
        historical_queries = [q for q in queries if "historical backfill" in q["description"]]
        assert len(historical_queries) == 1
        assert "before:2020/03/01" in historical_queries[0]["query"]

    def test_date_formatting_uses_yyyy_mm_dd(self):
        """Date strings in queries must use the Gmail YYYY/MM/DD format."""
        oldest = datetime(2023, 12, 31)
        newest = datetime(2024, 1, 5)
        queries = _build_sync_queries(oldest_email_date=oldest, newest_email_date=newest)

        query_strings = " ".join(q["query"] for q in queries)
        assert "2024/01/05" in query_strings
        assert "2023/12/31" in query_strings


# ===========================================================================
# email_sync — _create_email_objects
# ===========================================================================


class TestCreateEmailObjects:
    """Tests for the pure _create_email_objects helper."""

    def test_basic_field_mapping(self):
        """Email ORM fields are correctly mapped from the input dict."""
        d = _make_email_dict()
        emails = _create_email_objects([d], USER_ID, ACCOUNT_ID)

        assert len(emails) == 1
        e = emails[0]

        assert e.gmail_message_id == "msg1"
        assert e.gmail_thread_id == "thread1"
        assert e.subject == "Test Subject"
        assert e.sender_email == "sender@example.com"
        assert e.sender_name == "Sender Name"
        assert e.recipient_emails == "recipient@example.com"
        assert e.date == datetime(2024, 1, 15, 12, 0, 0)
        assert e.summary == "Hello world"
        assert e.body == "Hello world body"
        assert e.has_attachments is False
        assert e.attachment_count == 0
        assert e.user_id == uuid.UUID(USER_ID)
        assert e.account_id == ACCOUNT_ID

    def test_each_object_gets_a_unique_uuid(self):
        """Every created Email has a distinct UUID id."""
        dicts = [
            _make_email_dict(gmail_message_id="msg1"),
            _make_email_dict(gmail_message_id="msg2"),
            _make_email_dict(gmail_message_id="msg3"),
        ]
        emails = _create_email_objects(dicts, USER_ID, ACCOUNT_ID)

        ids = [e.id for e in emails]
        # All IDs must be valid UUIDs and all different
        assert len(set(ids)) == 3
        for eid in ids:
            assert isinstance(eid, uuid.UUID)

    def test_empty_input_returns_empty_list(self):
        """An empty list of dicts produces an empty list of Email objects."""
        result = _create_email_objects([], USER_ID, ACCOUNT_ID)
        assert result == []

    def test_summary_truncated_to_500_chars(self):
        """Snippets longer than 500 chars are truncated to exactly 500."""
        long_snippet = "A" * 600
        d = _make_email_dict(snippet=long_snippet)
        emails = _create_email_objects([d], USER_ID, ACCOUNT_ID)

        assert len(emails[0].summary) == 500
        assert emails[0].summary == "A" * 500

    def test_summary_not_truncated_when_under_limit(self):
        """Snippets at or under 500 chars are stored verbatim."""
        short_snippet = "Short snippet"
        d = _make_email_dict(snippet=short_snippet)
        emails = _create_email_objects([d], USER_ID, ACCOUNT_ID)

        assert emails[0].summary == short_snippet

    def test_missing_optional_fields_use_defaults(self):
        """Optional fields fall back to sensible defaults when absent."""
        minimal = {"gmail_message_id": "msg99"}
        emails = _create_email_objects([minimal], USER_ID, ACCOUNT_ID)

        e = emails[0]
        assert e.gmail_message_id == "msg99"
        assert e.gmail_thread_id is None
        assert e.subject == ""
        assert e.sender_email == ""
        assert e.sender_name is None
        assert e.recipient_emails == ""
        assert e.has_attachments is False
        assert e.attachment_count == 0
        assert e.body is None
        # date defaults to approximately now — just verify it is a datetime
        assert isinstance(e.date, datetime)


# ===========================================================================
# theme_detection — detect_themes
# ===========================================================================


class TestDetectThemes:
    """Tests for detect_themes phase function."""

    def _make_email_orm(self, email_id: uuid.UUID | None = None) -> MagicMock:
        """Return a mock Email ORM object."""
        mock_email = MagicMock()
        mock_email.id = email_id or uuid.uuid4()
        mock_email.account_id = ACCOUNT_ID
        return mock_email

    def _make_account(self, account_id: uuid.UUID | None = None) -> MagicMock:
        mock_account = MagicMock()
        mock_account.id = account_id or ACCOUNT_ID
        mock_account.account_label = "procore-main"
        return mock_account

    @patch("src.worker.phases.theme_detection.generate_tags")
    @patch("src.worker.phases.theme_detection.settings")
    def test_empty_email_list_does_not_crash(self, mock_settings, mock_generate_tags):
        """detect_themes with an empty email list completes without error."""
        mock_settings.claude_batch_size = 50

        db = MagicMock()
        job = MagicMock()
        theme_processor = MagicMock()
        theme_processor.process_emails_sync.return_value = {}
        progress_callback = MagicMock()

        from src.worker.phases.theme_detection import detect_themes

        detect_themes(
            db=db,
            job=job,
            all_emails=[],
            accounts=[],
            theme_processor=theme_processor,
            correlation_id="test-corr",
            progress_callback=progress_callback,
        )

        theme_processor.process_emails_sync.assert_not_called()
        mock_generate_tags.assert_not_called()

    @patch("src.worker.phases.theme_detection.generate_tags")
    @patch("src.worker.phases.theme_detection.settings")
    def test_processes_batch_and_creates_tags(self, mock_settings, mock_generate_tags):
        """Tags are created in DB for emails that have detected themes."""
        mock_settings.claude_batch_size = 50

        email_id = uuid.uuid4()
        mock_email = self._make_email_orm(email_id)
        mock_account = self._make_account()

        # Processor returns themes for the email
        theme_processor = MagicMock()
        theme_processor.process_emails_sync.return_value = {
            str(email_id): {"topics": ["project-alpha"], "interests": ["engineering"]}
        }

        # generate_tags returns two tag dicts
        mock_generate_tags.return_value = [
            {"tag": "project-alpha", "tag_category": "topic", "confidence": 0.9},
            {"tag": "engineering", "tag_category": "interest", "confidence": 0.8},
        ]

        db = MagicMock()
        # db.query(...).filter(...).first() must return truthy (email exists in DB)
        db.query.return_value.filter.return_value.first.return_value = (email_id,)

        job = MagicMock()
        progress_callback = MagicMock()

        from src.worker.phases.theme_detection import detect_themes

        detect_themes(
            db=db,
            job=job,
            all_emails=[mock_email],
            accounts=[mock_account],
            theme_processor=theme_processor,
            correlation_id="test-corr",
            progress_callback=progress_callback,
        )

        # generate_tags should have been called once
        mock_generate_tags.assert_called_once()

        # Two EmailTag objects should have been added to the session
        assert db.add.call_count == 2
        db.commit.assert_called()

    @patch("src.worker.phases.theme_detection.generate_tags")
    @patch("src.worker.phases.theme_detection.settings")
    def test_skips_tag_creation_when_email_not_in_db(self, mock_settings, mock_generate_tags):
        """If an email's id is not found in the DB, no tags are created for it."""
        mock_settings.claude_batch_size = 50

        email_id = uuid.uuid4()
        mock_email = self._make_email_orm(email_id)
        mock_account = self._make_account()

        theme_processor = MagicMock()
        theme_processor.process_emails_sync.return_value = {
            str(email_id): {"topics": ["some-topic"]}
        }
        mock_generate_tags.return_value = [
            {"tag": "some-topic", "tag_category": "topic", "confidence": 0.7}
        ]

        db = MagicMock()
        # Simulate email NOT found in DB (e.g., was a duplicate that wasn't inserted)
        db.query.return_value.filter.return_value.first.return_value = None

        job = MagicMock()
        progress_callback = MagicMock()

        from src.worker.phases.theme_detection import detect_themes

        detect_themes(
            db=db,
            job=job,
            all_emails=[mock_email],
            accounts=[mock_account],
            theme_processor=theme_processor,
            correlation_id="test-corr",
            progress_callback=progress_callback,
        )

        # generate_tags and db.add must never be called
        mock_generate_tags.assert_not_called()
        db.add.assert_not_called()


# ===========================================================================
# vault_generation — generate_vault
# ===========================================================================


class TestGenerateVault:
    """Tests for generate_vault phase function."""

    @patch("src.worker.phases.vault_generation.settings")
    def test_skips_in_non_development_environment(self, mock_settings):
        """generate_vault returns immediately without doing any work in production."""
        mock_settings.is_development = False

        db = MagicMock()
        job = MagicMock()
        vault_manager = MagicMock()
        note_generator = MagicMock()
        progress_callback = MagicMock()

        from src.worker.phases.vault_generation import generate_vault

        generate_vault(
            db=db,
            job=job,
            all_emails=[],
            merged_contacts=[],
            vault_manager=vault_manager,
            note_generator=note_generator,
            correlation_id="test-corr",
            progress_callback=progress_callback,
        )

        # Nothing should be touched when not in development
        vault_manager.initialize_vault.assert_not_called()
        note_generator.generate_contact_note.assert_not_called()
        note_generator.generate_email_note.assert_not_called()

    @patch("src.worker.phases.vault_generation.settings")
    def test_generates_contact_and_email_notes_in_development(self, mock_settings):
        """In development, vault is initialised and one note per contact/email is written."""
        mock_settings.is_development = True
        mock_settings.obsidian_vault_path = "/tmp/vault"

        # Build two fake emails
        email_id_1 = uuid.uuid4()
        email_id_2 = uuid.uuid4()
        base_date = datetime(2024, 3, 10)

        def _make_email(eid, sender, subject):
            e = MagicMock()
            e.id = eid
            e.sender_email = sender
            e.subject = subject
            e.date = base_date
            return e

        email1 = _make_email(email_id_1, "alice@example.com", "Hello")
        email2 = _make_email(email_id_2, "bob@example.com", "World")

        # Build one fake contact
        contact = MagicMock()
        contact.email = "alice@example.com"
        contact.name = "Alice"

        # DB query for tags returns empty list
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        job = MagicMock()
        progress_callback = MagicMock()

        vault_manager = MagicMock()
        note_generator = MagicMock()
        note_generator.generate_contact_note.return_value = "# Contact note"
        note_generator.generate_email_note.return_value = "# Email note"

        # vault_manager path helpers return mock Path-like objects
        mock_path = MagicMock()
        vault_manager.get_contact_path.return_value = mock_path
        vault_manager.get_email_path.return_value = mock_path

        from src.worker.phases.vault_generation import generate_vault

        generate_vault(
            db=db,
            job=job,
            all_emails=[email1, email2],
            merged_contacts=[contact],
            vault_manager=vault_manager,
            note_generator=note_generator,
            correlation_id="test-corr",
            progress_callback=progress_callback,
        )

        # Vault must be initialised exactly once
        vault_manager.initialize_vault.assert_called_once()

        # One contact note generated
        assert note_generator.generate_contact_note.call_count == 1
        note_generator.generate_contact_note.assert_called_once_with(contact, [email1])

        # Two email notes generated (one per email)
        assert note_generator.generate_email_note.call_count == 2

        # write_text called for contact + each email note = 3 total
        assert mock_path.write_text.call_count == 3

        # ensure_email_directory called once per email
        assert vault_manager.ensure_email_directory.call_count == 2
