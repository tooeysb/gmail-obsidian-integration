"""
Obsidian Vault Manager.
Handles vault directory structure initialization and configuration.
"""

import json
from pathlib import Path

from src.core.config import settings


class ObsidianVaultManager:
    """Manages Obsidian vault directory structure and configuration."""

    def __init__(self, vault_path: str | None = None):
        """
        Initialize vault manager.

        Args:
            vault_path: Path to Obsidian vault. Defaults to settings.obsidian_vault_path.
        """
        self.vault_path = Path(vault_path or settings.obsidian_vault_path)

    def initialize_vault(self) -> None:
        """
        Create vault directory structure and configuration.

        Directory structure:
        - Contacts/
        - Emails/YYYY/MM/
        - .obsidian/

        Creates basic Obsidian config in .obsidian/config.json.
        """
        # Create main vault directory
        self.vault_path.mkdir(parents=True, exist_ok=True)

        # Create Contacts directory
        contacts_dir = self.vault_path / "Contacts"
        contacts_dir.mkdir(exist_ok=True)

        # Create Emails directory
        emails_dir = self.vault_path / "Emails"
        emails_dir.mkdir(exist_ok=True)

        # Create .obsidian directory
        obsidian_dir = self.vault_path / ".obsidian"
        obsidian_dir.mkdir(exist_ok=True)

        # Create config.json with basic Obsidian settings
        self._create_obsidian_config(obsidian_dir)

    def _create_obsidian_config(self, obsidian_dir: Path) -> None:
        """
        Create .obsidian/config.json with basic settings.

        Args:
            obsidian_dir: Path to .obsidian directory.
        """
        config_path = obsidian_dir / "config.json"

        # Only create if it doesn't exist to avoid overwriting user customizations
        if config_path.exists():
            return

        config = {
            "newFileLocation": "current",
            "useMarkdownLinks": False,
            "strictLineBreaks": False,
            "foldHeading": True,
            "foldIndent": True,
            "showLineNumber": False,
            "spellcheck": True,
            "vimMode": False,
            "legacyEditor": False,
            "livePreview": True,
            "readableLineLength": True,
            "showFrontmatter": True,
            "showInlineTitle": True,
        }

        config_path.write_text(json.dumps(config, indent=2))

    def ensure_email_directory(self, year: int, month: int) -> Path:
        """
        Ensure email directory exists for given year and month.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)

        Returns:
            Path to email directory (Emails/YYYY/MM/)
        """
        email_dir = self.vault_path / "Emails" / str(year) / f"{month:02d}"
        email_dir.mkdir(parents=True, exist_ok=True)
        return email_dir

    def get_contacts_directory(self) -> Path:
        """
        Get path to Contacts directory.

        Returns:
            Path to Contacts directory.
        """
        return self.vault_path / "Contacts"

    def get_email_path(self, year: int, month: int, filename: str) -> Path:
        """
        Get full path to email note file.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            filename: Email note filename (including .md extension)

        Returns:
            Full path to email note file.
        """
        return self.ensure_email_directory(year, month) / filename

    def get_contact_path(self, filename: str) -> Path:
        """
        Get full path to contact note file.

        Args:
            filename: Contact note filename (including .md extension)

        Returns:
            Full path to contact note file.
        """
        return self.get_contacts_directory() / filename
