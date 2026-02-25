"""Theme detection service for email analysis."""

from src.services.theme_detection.prompt_template import (
    SYSTEM_PROMPT,
    generate_tags,
    generate_user_prompt,
)

__all__ = ["SYSTEM_PROMPT", "generate_user_prompt", "generate_tags"]
