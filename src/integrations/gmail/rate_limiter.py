"""
Gmail API rate limiter using token bucket algorithm.
Supports distributed rate limiting via Redis for horizontal scaling.
"""

import time
from datetime import datetime
from typing import Any, Callable

import redis
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.core.config import settings


class GmailRateLimitExceeded(Exception):
    """Raised when Gmail API rate limit is exceeded and retry should occur."""

    pass


class GmailRateLimiter:
    """
    Token bucket rate limiter for Gmail API with Redis backing.

    Implements the token bucket algorithm to enforce rate limits:
    - Tokens are added at a constant rate (refill_rate)
    - Each API call consumes one token
    - If no tokens available, request is rate limited

    Redis is used for distributed rate limiting across multiple workers/instances.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        max_tokens: int | None = None,
        refill_rate: float | None = None,
    ):
        """
        Initialize rate limiter with Redis connection.

        Args:
            redis_url: Redis connection URL (defaults to settings.redis_url)
            max_tokens: Maximum tokens in bucket (defaults to settings.gmail_rate_limit_qps)
            refill_rate: Tokens added per second (defaults to settings.gmail_rate_limit_qps)
        """
        self.redis_url = redis_url or settings.redis_url
        self.max_tokens = max_tokens or settings.gmail_rate_limit_qps
        self.refill_rate = refill_rate or float(settings.gmail_rate_limit_qps)

        # Connect to Redis
        self.redis_client = redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        # Redis key for storing token bucket state
        self.bucket_key = "gmail:rate_limiter:tokens"
        self.timestamp_key = "gmail:rate_limiter:last_refill"

    def _get_current_tokens(self) -> tuple[float, float]:
        """
        Get current token count and last refill timestamp from Redis.

        Returns:
            Tuple of (current_tokens, last_refill_timestamp)
        """
        pipe = self.redis_client.pipeline()
        pipe.get(self.bucket_key)
        pipe.get(self.timestamp_key)
        results = pipe.execute()

        current_tokens = float(results[0]) if results[0] else self.max_tokens
        last_refill = float(results[1]) if results[1] else time.time()

        return current_tokens, last_refill

    def _refill_tokens(self) -> float:
        """
        Refill tokens based on elapsed time since last refill.

        Returns:
            Current token count after refill
        """
        current_tokens, last_refill = self._get_current_tokens()
        now = time.time()
        elapsed = now - last_refill

        # Calculate tokens to add based on elapsed time and refill rate
        tokens_to_add = elapsed * self.refill_rate
        new_tokens = min(current_tokens + tokens_to_add, self.max_tokens)

        # Update Redis with new token count and timestamp
        pipe = self.redis_client.pipeline()
        pipe.set(self.bucket_key, str(new_tokens))
        pipe.set(self.timestamp_key, str(now))
        pipe.execute()

        return new_tokens

    def acquire(self, tokens: int = 1) -> bool:
        """
        Attempt to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire (default: 1)

        Returns:
            True if tokens were acquired, False if rate limit exceeded
        """
        # Refill tokens first
        current_tokens = self._refill_tokens()

        # Check if we have enough tokens
        if current_tokens >= tokens:
            # Consume tokens
            new_tokens = current_tokens - tokens
            self.redis_client.set(self.bucket_key, str(new_tokens))
            return True

        return False

    def wait_for_token(self, timeout: float = 60.0) -> None:
        """
        Block until a token is available or timeout is reached.

        Args:
            timeout: Maximum time to wait in seconds (default: 60)

        Raises:
            GmailRateLimitExceeded: If timeout is reached without acquiring token
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.acquire():
                return

            # Calculate sleep time based on refill rate
            # Sleep for time it takes to refill one token
            sleep_time = min(1.0 / self.refill_rate, 0.1)
            time.sleep(sleep_time)

        raise GmailRateLimitExceeded(
            f"Rate limit exceeded: could not acquire token within {timeout}s"
        )

    def get_token_count(self) -> float:
        """Get current token count without refilling."""
        current_tokens, _ = self._get_current_tokens()
        return current_tokens

    def reset(self) -> None:
        """Reset rate limiter to full capacity (useful for testing)."""
        pipe = self.redis_client.pipeline()
        pipe.set(self.bucket_key, str(self.max_tokens))
        pipe.set(self.timestamp_key, str(time.time()))
        pipe.execute()

    def close(self) -> None:
        """Close Redis connection."""
        self.redis_client.close()


def rate_limited(rate_limiter: GmailRateLimiter) -> Callable:
    """
    Decorator to apply rate limiting to a function.

    Args:
        rate_limiter: GmailRateLimiter instance to use

    Returns:
        Decorated function that enforces rate limiting

    Example:
        @rate_limited(limiter)
        def fetch_messages():
            # API call here
            pass
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rate_limiter.wait_for_token()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def with_retry(func: Callable) -> Callable:
    """
    Decorator to add exponential backoff retry logic for Gmail API calls.

    Retries on rate limit errors with exponential backoff:
    - Initial wait: 4 seconds
    - Maximum wait: 60 seconds
    - Maximum attempts: 5

    Args:
        func: Function to decorate

    Returns:
        Decorated function with retry logic
    """
    @retry(
        wait=wait_exponential(min=4, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((GmailRateLimitExceeded, Exception)),
        reraise=True,
    )
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log retry attempt
            error_msg = str(e)
            if "429" in error_msg or "quota" in error_msg.lower():
                raise GmailRateLimitExceeded(f"Gmail API rate limit: {error_msg}")
            raise

    return wrapper
