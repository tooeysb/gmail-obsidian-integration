"""
Human-like behavior simulation for LinkedIn browsing.

Controls timing, scrolling, and work schedule to avoid detection.
All randomization uses uniform distributions for natural variation.
"""

from __future__ import annotations

import random
import time
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

from src.core.logging import get_logger

logger = get_logger(__name__)

PT = ZoneInfo("America/Los_Angeles")


class WorkPhase(Enum):
    MORNING = "morning"
    LUNCH = "lunch"
    AFTERNOON = "afternoon"
    OFF_HOURS = "off_hours"


class WorkSchedule:
    """Simulates a human work schedule in Pacific Time."""

    def __init__(self):
        # Random daily start offset (0-10 minutes past 8:00 AM)
        self.start_offset_minutes = random.randint(0, 10)
        self.morning_start = 8 * 60 + self.start_offset_minutes
        self.lunch_start = 12 * 60 + random.randint(-5, 10)
        self.lunch_duration = random.randint(50, 70)
        self.lunch_end = self.lunch_start + self.lunch_duration
        self.day_end = 17 * 60 + random.randint(-5, 5)

        # Break tracking
        self._last_break_at = time.monotonic()
        self._next_break_interval = random.uniform(30, 60) * 60  # seconds

    def current_phase(self) -> WorkPhase:
        """Determine current work phase based on Pacific Time."""
        now = datetime.now(PT)
        minutes = now.hour * 60 + now.minute
        if minutes < self.morning_start:
            return WorkPhase.OFF_HOURS
        elif minutes < self.lunch_start:
            return WorkPhase.MORNING
        elif minutes < self.lunch_end:
            return WorkPhase.LUNCH
        elif minutes < self.day_end:
            return WorkPhase.AFTERNOON
        else:
            return WorkPhase.OFF_HOURS

    def wait_for_work_hours(self) -> bool:
        """Block until work hours begin. Returns False if day is over."""
        phase = self.current_phase()

        if phase in (WorkPhase.MORNING, WorkPhase.AFTERNOON):
            return True

        if phase == WorkPhase.LUNCH:
            now = datetime.now(PT)
            lunch_end_time = now.replace(
                hour=self.lunch_end // 60,
                minute=self.lunch_end % 60,
                second=0,
                microsecond=0,
            )
            wait_seconds = max(0, (lunch_end_time - now).total_seconds())
            if wait_seconds > 0:
                logger.info("Lunch break — resuming in %.0f minutes", wait_seconds / 60)
                time.sleep(wait_seconds)
            return True

        # OFF_HOURS — check if we haven't started yet today
        now = datetime.now(PT)
        start_time = now.replace(
            hour=self.morning_start // 60,
            minute=self.morning_start % 60,
            second=0,
            microsecond=0,
        )
        if now < start_time:
            wait_seconds = (start_time - now).total_seconds()
            logger.info("Waiting %.0f minutes for work hours to begin", wait_seconds / 60)
            time.sleep(wait_seconds)
            return True

        # Past end of day
        return False

    def should_take_break(self) -> bool:
        """Check if it is time for a random micro-break."""
        elapsed = time.monotonic() - self._last_break_at
        return elapsed >= self._next_break_interval

    def take_break(self):
        """Take a 5-15 minute break and reset interval."""
        duration = random.uniform(5, 15) * 60
        logger.info("Taking a %.1f minute break", duration / 60)
        time.sleep(duration)
        self._last_break_at = time.monotonic()
        self._next_break_interval = random.uniform(30, 60) * 60


def delay_between_profiles():
    """Wait 45-120 seconds between profiles (never near exactly 60)."""
    while True:
        delay = random.uniform(45, 120)
        if delay < 58 or delay > 62:
            break
    logger.info("Waiting %.0f seconds before next profile", delay)
    time.sleep(delay)


def delay_between_clicks():
    """Wait 2-8 seconds between page interactions."""
    time.sleep(random.uniform(2, 8))


def delay_page_load():
    """Wait 3-6 seconds for page to fully render."""
    time.sleep(random.uniform(3, 6))
