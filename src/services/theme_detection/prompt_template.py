"""
Claude prompt template for email theme extraction.
Uses prompt caching for 90% cost savings on system prompts.
"""

import re
from typing import Any

# System prompt for theme extraction (marked for caching)
# This comprehensive prompt should be cached by Claude to save costs
SYSTEM_PROMPT = """You are an expert email analysis assistant that extracts structured themes and metadata from email conversations.

Your task is to analyze email content and extract the following information in JSON format:

**Extraction Guidelines:**

1. **Explicit Topics** (explicit_topics: list[str])
   - Clear, directly mentioned subjects or projects
   - Examples: "Q4 Budget", "Product Launch", "Team Offsite"
   - Be specific but concise (2-5 words per topic)

2. **Implicit Interests** (implicit_interests: list[str])
   - Subtle mentions of hobbies, activities, or preferences
   - Examples: "scuba diving" (from "loved our diving trip"), "photography", "cooking"
   - Only include if there's genuine interest indicated, not just passing mentions

3. **Relationship Context** (relationship_context: str)
   - Classify the relationship between sender and recipient
   - Options: "colleague", "client", "vendor", "friend", "family", "recruiter", "manager", "report", "unknown"
   - Base this on tone, content, and context clues

4. **Action Items** (action_items: list[str])
   - Concrete tasks or requests mentioned in the email
   - Examples: "Review proposal by Friday", "Schedule meeting", "Approve budget"
   - Keep to 5-10 words per action
   - Only include clear, actionable items

5. **Sentiment** (sentiment: str)
   - Overall emotional tone of the email
   - Options: "positive", "neutral", "negative", "urgent"
   - "urgent" takes precedence if time-sensitive language is present

6. **Domains** (domains: list[str])
   - High-level life/work categories the email belongs to
   - Options: "work", "finance", "travel", "health", "hobbies", "education", "personal", "shopping", "legal"
   - Can include multiple domains if applicable

**Output Format:**
Return ONLY a valid JSON object with this exact structure:
{
  "explicit_topics": ["topic1", "topic2"],
  "implicit_interests": ["interest1", "interest2"],
  "relationship_context": "colleague",
  "action_items": ["action1", "action2"],
  "sentiment": "positive",
  "domains": ["work", "travel"]
}

**Important Rules:**
- Return ONLY the JSON object, no additional text or explanation
- All list fields must be arrays (empty array [] if none found)
- All string fields must be lowercase
- Use exact values from the options provided above
- Be conservative: only extract what is clearly present in the email
- If unsure about a field, use empty array [] or "unknown" for strings
- Maximum 10 items per list field
"""


def generate_user_prompt(
    subject: str | None,
    sender_email: str,
    sender_name: str | None,
    recipient_emails: str,
    date: str,
    summary: str | None,
) -> str:
    """
    Generate user prompt for a single email.

    Args:
        subject: Email subject line
        sender_email: Sender's email address
        sender_name: Sender's display name (optional)
        recipient_emails: Comma-separated recipient emails
        date: Email date (ISO format)
        summary: 500-char email summary

    Returns:
        Formatted user prompt string
    """
    sender = f"{sender_name} <{sender_email}>" if sender_name else sender_email

    return f"""Analyze this email and extract themes:

**Email Metadata:**
- From: {sender}
- To: {recipient_emails}
- Date: {date}
- Subject: {subject or "(No subject)"}

**Email Summary:**
{summary or "(No summary available)"}

Extract themes as specified in the system prompt."""


def generate_tags(themes: dict[str, Any], account_label: str) -> list[dict[str, Any]]:
    """
    Convert extracted themes to database tag records.

    Args:
        themes: Dictionary with extracted themes from Claude
        account_label: Gmail account label (e.g., "procore-main", "personal")

    Returns:
        List of tag dictionaries with 'tag', 'tag_category', and optional 'confidence'
    """
    tags = []

    # Explicit topics → topic/* tags
    for topic in themes.get("explicit_topics", []):
        if topic:
            tag_value = _normalize_tag(topic)
            tags.append({"tag": tag_value, "tag_category": "topic", "confidence": 0.9})

    # Implicit interests → interest/* tags
    for interest in themes.get("implicit_interests", []):
        if interest:
            tag_value = _normalize_tag(interest)
            tags.append({"tag": tag_value, "tag_category": "interest", "confidence": 0.7})

    # Relationship context → relationship/* tag
    relationship = themes.get("relationship_context", "").lower()
    if relationship and relationship != "unknown":
        tags.append({"tag": relationship, "tag_category": "relationship", "confidence": 0.85})

    # Action items → action/* tags
    for action in themes.get("action_items", []):
        if action:
            # Extract key verbs/nouns from action items
            tag_value = _extract_action_tag(action)
            tags.append({"tag": tag_value, "tag_category": "action", "confidence": 0.8})

    # Sentiment → sentiment/* tag
    sentiment = themes.get("sentiment", "").lower()
    if sentiment:
        tags.append({"tag": sentiment, "tag_category": "sentiment", "confidence": 0.9})

    # Domains → domain/* tags
    for domain in themes.get("domains", []):
        if domain:
            tags.append({"tag": domain, "tag_category": "domain", "confidence": 0.85})

    # Account label → account/* tag
    tags.append({"tag": account_label, "tag_category": "account", "confidence": 1.0})

    return tags


def _normalize_tag(text: str) -> str:
    """
    Normalize tag text to lowercase, hyphenated format.

    Examples:
        "Q4 Budget" → "q4-budget"
        "Product Launch" → "product-launch"
        "scuba diving" → "scuba-diving"
    """
    # Lowercase and replace spaces with hyphens
    normalized = text.lower().strip()
    # Replace multiple spaces/special chars with single hyphen
    normalized = re.sub(r"[^\w\s-]", "", normalized)
    normalized = re.sub(r"[-\s]+", "-", normalized)
    # Remove leading/trailing hyphens
    normalized = normalized.strip("-")
    return normalized


def _extract_action_tag(action_text: str) -> str:
    """
    Extract a concise tag from an action item.

    Examples:
        "Review proposal by Friday" → "review-proposal"
        "Schedule meeting with team" → "schedule-meeting"
        "Approve budget request" → "approve-budget"
    """
    # Take first 2-3 significant words
    words = action_text.lower().split()
    # Remove common stop words
    stop_words = {"the", "a", "an", "by", "with", "for", "to", "on", "in", "at", "of"}
    significant = [w for w in words if w not in stop_words]

    # Take first 2-3 words
    tag_words = significant[:3]
    return "-".join(tag_words) if tag_words else _normalize_tag(action_text)
