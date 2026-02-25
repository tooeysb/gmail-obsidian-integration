"""
Claude Batch API processor for email theme extraction.
Uses Batch API with prompt caching for 90%+ cost savings.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any

from anthropic import Anthropic
from tenacity import retry, stop_after_attempt, wait_exponential

from src.core.config import settings
from src.services.theme_detection.prompt_template import (
    SYSTEM_PROMPT,
    generate_user_prompt,
)

logger = logging.getLogger(__name__)


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

        logger.info(f"Preparing batch of {len(emails)} emails for Claude Batch API")

        # Build batch requests
        requests = []
        for email in emails:
            email_id = str(email["id"])
            date_str = (
                email["date"].isoformat()
                if isinstance(email["date"], datetime)
                else str(email["date"])
            )

            user_prompt = generate_user_prompt(
                subject=email.get("subject"),
                sender_email=email["sender_email"],
                sender_name=email.get("sender_name"),
                recipient_emails=email["recipient_emails"],
                date=date_str,
                summary=email.get("summary"),
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
            batch_response = self.client.messages.batches.create(requests=requests)
            batch_id = batch_response.id

            logger.info(
                f"Batch submitted successfully. Batch ID: {batch_id}, "
                f"Status: {batch_response.processing_status}"
            )

            return batch_id

        except Exception as e:
            logger.error(f"Failed to submit batch: {e}")
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
        logger.info(f"Polling batch {batch_id} for results...")

        # Retrieve batch status
        batch = self.client.messages.batches.retrieve(batch_id)

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
                f"Batch {batch_id} still processing: "
                f"{completed}/{total} complete ({progress_pct:.1f}%)"
            )
            raise Exception("Batch still processing")  # Trigger retry

        elif status == "ended":
            logger.info(
                f"Batch {batch_id} completed. "
                f"Succeeded: {batch.request_counts.succeeded}, "
                f"Errored: {batch.request_counts.errored}"
            )

            # Retrieve all results
            results = {}
            for result in self.client.messages.batches.results(batch_id):
                email_id = result.custom_id

                if result.result.type == "succeeded":
                    try:
                        themes = self.parse_themes(result.result.message)
                        results[email_id] = themes
                    except Exception as e:
                        logger.error(f"Failed to parse themes for email {email_id}: {e}")
                        results[email_id] = self._empty_themes()
                else:
                    # Handle error result
                    error_type = getattr(result.result, "type", "unknown")
                    logger.error(f"Email {email_id} processing failed: {error_type}")
                    results[email_id] = self._empty_themes()

            return results

        else:
            # canceled, expired, or unknown status
            logger.error(f"Batch {batch_id} ended with unexpected status: {status}")
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

        # Parse JSON response
        try:
            themes = json.loads(text_content.strip())
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {text_content}")
            raise ValueError(f"Invalid JSON response from Claude: {e}")

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
            logger.warning(f"Missing fields in theme response: {missing}")
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
        Submit batch and wait for results synchronously.

        Convenience method that combines submit_batch() and poll_batch_results().

        Args:
            emails: List of email dictionaries (see submit_batch for schema)

        Returns:
            Dictionary mapping email_id → parsed themes
        """
        batch_id = self.submit_batch(emails)

        logger.info(f"Waiting for batch {batch_id} to complete...")

        results = self.poll_batch_results(batch_id)

        logger.info(f"Batch {batch_id} processing complete. {len(results)} results retrieved.")

        return results
