"""
Claude Batch API processor for email theme extraction.
Uses Batch API with prompt caching for 90%+ cost savings.
"""

import json
from datetime import datetime
from typing import Any

from anthropic import Anthropic
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.config import settings
from src.core.logging import get_logger
from src.services.theme_detection.prompt_template import (
    SYSTEM_PROMPT,
    generate_user_prompt,
)

logger = get_logger(__name__)


class ThemeBatchProcessor:
    """
    Processes email theme extraction using Claude Batch API.
    Leverages prompt caching for 90%+ cost reduction on system prompts.
    """

    def __init__(self):
        """Initialize the batch processor with Anthropic client."""
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = settings.claude_model
        self.batch_size = settings.claude_batch_size

    def submit_batch(self, emails: list[dict[str, Any]]) -> str:
        """
        Submit a batch of up to 100 emails to Claude Batch API.

        Args:
            emails: List of email dictionaries with keys:
                - id: Email UUID
                - subject: Email subject
                - sender_email: Sender email address
                - sender_name: Sender display name (optional)
                - recipient_emails: Comma-separated recipients
                - date: Email date (datetime or ISO string)
                - summary: Email summary
                - account_label: Gmail account label

        Returns:
            Batch ID for polling results

        Raises:
            ValueError: If batch size exceeds limit
            Exception: If batch submission fails
        """
        if len(emails) > self.batch_size:
            raise ValueError(
                f"Batch size {len(emails)} exceeds limit of {self.batch_size}. "
                "Split into multiple batches."
            )

        if not emails:
            raise ValueError("Cannot submit empty batch")

        logger.info("Preparing batch of %d emails for Claude Batch API", len(emails))

        # Build batch requests
        requests = []
        for email in emails:
            # Handle both Email model objects and dictionaries
            email_id = str(email.id if hasattr(email, "id") else email["id"])
            email_date = email.date if hasattr(email, "date") else email["date"]
            date_str = (
                email_date.isoformat() if isinstance(email_date, datetime) else str(email_date)
            )

            # Extract fields from model or dict
            subject = email.subject if hasattr(email, "subject") else email.get("subject")
            sender_email = (
                email.sender_email if hasattr(email, "sender_email") else email["sender_email"]
            )
            sender_name = (
                email.sender_name if hasattr(email, "sender_name") else email.get("sender_name")
            )
            recipient_emails = (
                email.recipient_emails
                if hasattr(email, "recipient_emails")
                else email["recipient_emails"]
            )
            summary = email.summary if hasattr(email, "summary") else email.get("summary")

            user_prompt = generate_user_prompt(
                subject=subject,
                sender_email=sender_email,
                sender_name=sender_name,
                recipient_emails=recipient_emails,
                date=date_str,
                summary=summary,
            )

            # Create request with prompt caching on system prompt
            # The system prompt is marked for caching to save 90% on repeated calls
            request = {
                "custom_id": email_id,
                "params": {
                    "model": self.model,
                    "max_tokens": 1024,
                    "system": [
                        {
                            "type": "text",
                            "text": SYSTEM_PROMPT,
                            "cache_control": {"type": "ephemeral"},  # Enable caching
                        }
                    ],
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            }
            requests.append(request)

        # Submit batch via API
        try:
            batch_response = self.client.beta.messages.batches.create(requests=requests)
            batch_id = batch_response.id

            logger.info(
                "Batch submitted successfully. Batch ID: %s, Status: %s",
                batch_id,
                batch_response.processing_status,
            )

            return batch_id

        except Exception as e:
            logger.error("Failed to submit batch: %s", e)
            raise

    @retry(
        stop=stop_after_attempt(60),  # Max 60 attempts (30 minutes with 30s intervals)
        wait=wait_exponential(multiplier=1, min=10, max=60),
        reraise=True,
    )
    def poll_batch_results(self, batch_id: str) -> dict[str, dict[str, Any]]:
        """
        Poll for batch results until completion.

        Uses exponential backoff with tenacity retry logic.
        Polls every 10-60 seconds for up to 30 minutes.

        Args:
            batch_id: Batch ID returned from submit_batch()

        Returns:
            Dictionary mapping email_id → parsed themes
            Example:
            {
                "email-uuid-1": {
                    "explicit_topics": ["budget", "q4-planning"],
                    "implicit_interests": [],
                    "relationship_context": "colleague",
                    "action_items": ["review-proposal"],
                    "sentiment": "neutral",
                    "domains": ["work"]
                },
                ...
            }

        Raises:
            TimeoutError: If batch doesn't complete within retry limit
            Exception: If batch processing fails
        """
        logger.info("Polling batch %s for results...", batch_id)

        # Retrieve batch status
        batch = self.client.beta.messages.batches.retrieve(batch_id)

        status = batch.processing_status

        if status == "in_progress":
            # Calculate progress percentage if available
            request_counts = batch.request_counts
            total = (
                request_counts.processing
                + request_counts.succeeded
                + request_counts.errored
                + request_counts.canceled
                + request_counts.expired
            )
            completed = request_counts.succeeded + request_counts.errored

            progress_pct = (completed / total * 100) if total > 0 else 0

            logger.info(
                "Batch %s still processing: %d/%d complete (%.1f%%)",
                batch_id,
                completed,
                total,
                progress_pct,
            )
            raise Exception("Batch still processing")  # Trigger retry

        elif status == "ended":
            logger.info(
                "Batch %s completed. Succeeded: %d, Errored: %d",
                batch_id,
                batch.request_counts.succeeded,
                batch.request_counts.errored,
            )

            # Retrieve all results
            results = {}
            for result in self.client.beta.messages.batches.results(batch_id):
                email_id = result.custom_id

                if result.result.type == "succeeded":
                    try:
                        themes = self.parse_themes(result.result.message)
                        results[email_id] = themes
                    except Exception as e:
                        logger.error("Failed to parse themes for email %s: %s", email_id, e)
                        results[email_id] = self._empty_themes()
                else:
                    # Handle error result
                    error_type = getattr(result.result, "type", "unknown")
                    # Log full error object to understand structure
                    logger.error("Email %s processing failed with type: %s", email_id, error_type)
                    logger.error("Full error object: %s", result.result)
                    results[email_id] = self._empty_themes()

            return results

        else:
            # canceled, expired, or unknown status
            logger.error("Batch %s ended with unexpected status: %s", batch_id, status)
            raise Exception(f"Batch processing failed with status: {status}")

    def parse_themes(self, message: Any) -> dict[str, Any]:
        """
        Extract and parse JSON themes from Claude response.

        Args:
            message: Claude message response object with .content attribute

        Returns:
            Dictionary with extracted themes

        Raises:
            json.JSONDecodeError: If response is not valid JSON
            ValueError: If required fields are missing
        """
        # Extract text from message content
        if not message.content:
            raise ValueError("Message has no content")

        # Claude returns list of content blocks, find text block
        text_content = None
        for block in message.content:
            if block.type == "text":
                text_content = block.text
                break

        if not text_content:
            raise ValueError("No text content found in message")

        # Strip markdown code blocks if present (```json ... ```)
        text_content = text_content.strip()
        if text_content.startswith("```json"):
            # Remove opening ```json and closing ```
            text_content = text_content[7:]  # Remove ```json
            if text_content.endswith("```"):
                text_content = text_content[:-3]  # Remove closing ```
            text_content = text_content.strip()
        elif text_content.startswith("```"):
            # Handle plain ``` without json
            text_content = text_content[3:]
            if text_content.endswith("```"):
                text_content = text_content[:-3]
            text_content = text_content.strip()

        # Parse JSON response
        try:
            themes = json.loads(text_content)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON response: %s", text_content)
            raise ValueError(f"Invalid JSON response from Claude: {e}") from None

        # Validate required fields
        required_fields = {
            "explicit_topics",
            "implicit_interests",
            "relationship_context",
            "action_items",
            "sentiment",
            "domains",
        }

        missing = required_fields - set(themes.keys())
        if missing:
            logger.warning("Missing fields in theme response: %s", missing)
            # Fill in missing fields with defaults
            for field in missing:
                if field in ["explicit_topics", "implicit_interests", "action_items", "domains"]:
                    themes[field] = []
                elif field == "relationship_context":
                    themes[field] = "unknown"
                elif field == "sentiment":
                    themes[field] = "neutral"

        return themes

    def _empty_themes(self) -> dict[str, Any]:
        """Return empty themes dict for failed email processing."""
        return {
            "explicit_topics": [],
            "implicit_interests": [],
            "relationship_context": "unknown",
            "action_items": [],
            "sentiment": "neutral",
            "domains": [],
        }

    def process_emails_sync(self, emails: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """
        Process emails synchronously using direct API calls (not batch API).

        Processes each email immediately for instant results at 2x cost.
        Use this for immediate feedback instead of batch processing delays.

        Args:
            emails: List of email dictionaries with keys:
                - id: Email UUID
                - subject, sender_email, sender_name, recipient_emails, date, summary

        Returns:
            Dictionary mapping email_id → parsed themes
        """
        logger.info("Processing %d emails synchronously (immediate mode)", len(emails))

        results = {}

        for idx, email in enumerate(emails):
            # Extract email fields
            email_id = str(email.id if hasattr(email, "id") else email["id"])
            email_date = email.date if hasattr(email, "date") else email["date"]
            date_str = (
                email_date.isoformat() if isinstance(email_date, datetime) else str(email_date)
            )

            subject = email.subject if hasattr(email, "subject") else email.get("subject")
            sender_email = (
                email.sender_email if hasattr(email, "sender_email") else email["sender_email"]
            )
            sender_name = (
                email.sender_name if hasattr(email, "sender_name") else email.get("sender_name")
            )
            recipient_emails = (
                email.recipient_emails
                if hasattr(email, "recipient_emails")
                else email["recipient_emails"]
            )
            summary = email.summary if hasattr(email, "summary") else email.get("summary")

            # Generate prompt
            user_prompt = generate_user_prompt(
                subject=subject,
                sender_email=sender_email,
                sender_name=sender_name,
                recipient_emails=recipient_emails,
                date=date_str,
                summary=summary,
            )

            # Call Claude API directly (synchronous)
            try:
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    system=[
                        {
                            "type": "text",
                            "text": SYSTEM_PROMPT,
                            "cache_control": {"type": "ephemeral"},  # Enable caching
                        }
                    ],
                    messages=[{"role": "user", "content": user_prompt}],
                )

                # Parse themes from response
                themes = self.parse_themes(message)
                results[email_id] = themes

                logger.info("Processed email %d/%d: %s", idx + 1, len(emails), email_id)

            except Exception as e:
                logger.error("Failed to process email %s: %s", email_id, e)
                results[email_id] = self._empty_themes()

        logger.info("Synchronous processing complete. %d emails processed.", len(results))
        return results
