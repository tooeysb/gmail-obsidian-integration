"""
Gmail API rate limiter using token bucket algorithm.
Supports distributed rate limiting via Redis for horizontal scaling.

Uses an atomic Lua script to eliminate TOCTOU race conditions when
multiple workers compete for tokens concurrently.
"""

import ssl
import time
from typing import Any, Callable

import redis
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.core.config import settings
from src.core.logging import get_logger

logger = get_logger(__name__)


class GmailRateLimitExceeded(Exception):
    """Raised when Gmail API rate limit is exceeded and retry should occur."""

    pass


# Lua script for atomic token bucket acquire.
# Refills tokens based on elapsed time, then attempts to consume.
# Returns 1 if tokens acquired, 0 if insufficient tokens.
_ACQUIRE_SCRIPT = """
local bucket_key = KEYS[1]
local timestamp_key = KEYS[2]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local tokens_requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local current_tokens = tonumber(redis.call('GET', bucket_key) or max_tokens)
local last_refill = tonumber(redis.call('GET', timestamp_key) or now)

local elapsed = now - last_refill
local new_tokens = math.min(current_tokens + elapsed * refill_rate, max_tokens)

if new_tokens >= tokens_requested then
    new_tokens = new_tokens - tokens_requested
    redis.call('SET', bucket_key, tostring(new_tokens))
    redis.call('SET', timestamp_key, tostring(now))
    return 1
else
    redis.call('SET', bucket_key, tostring(new_tokens))
    redis.call('SET', timestamp_key, tostring(now))
    return 0
end
"""


class GmailRateLimiter:
    """
    Token bucket rate limiter for Gmail API with Redis backing.

    Implements the token bucket algorithm to enforce rate limits:
    - Tokens are added at a constant rate (refill_rate)
    - Each API call consumes one token
    - If no tokens available, request is rate limited

    Redis is used for distributed rate limiting across multiple workers/instances.
    All token operations are atomic via a Lua script to prevent race conditions.
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
            max_tokens: Maximum tokens in bucket (defaults to settings.gmail_rate_limit_burst)
            refill_rate: Tokens added per second (defaults to settings.gmail_rate_limit_qps)
        """
        self.redis_url = redis_url or settings.redis_url
        self.max_tokens = max_tokens or settings.gmail_rate_limit_burst
        self.refill_rate = refill_rate or float(settings.gmail_rate_limit_qps)

        # Connect to Redis with SSL configuration for Heroku (rediss://)
        # Limit max_connections to avoid exhausting Heroku Redis connection limit
        # (shared with Celery broker connections)
        connection_params = {
            "decode_responses": True,
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
            "max_connections": 2,
            "retry_on_error": [redis.ConnectionError, redis.TimeoutError],
        }

        # Add SSL configuration for rediss:// URLs (Heroku Redis)
        if self.redis_url.startswith("rediss://"):
            connection_params["ssl_cert_reqs"] = ssl.CERT_NONE

        self.redis_client = redis.from_url(
            self.redis_url,
            **connection_params,
        )

        # Redis keys for storing token bucket state
        self.bucket_key = "gmail:rate_limiter:tokens"
        self.timestamp_key = "gmail:rate_limiter:last_refill"

        # Register Lua script for atomic acquire
        self._acquire_script = self.redis_client.register_script(_ACQUIRE_SCRIPT)

    def acquire(self, tokens: int = 1) -> bool:
        """
        Attempt to acquire tokens from the bucket atomically.

        Args:
            tokens: Number of tokens to acquire (default: 1)

        Returns:
            True if tokens were acquired, False if rate limit exceeded
        """
        try:
            result = self._acquire_script(
                keys=[self.bucket_key, self.timestamp_key],
                args=[self.max_tokens, self.refill_rate, tokens, time.time()],
            )
            return bool(result)
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis connection error in acquire: {e}")
            return False

    def wait_for_token(self, tokens: int = 1, timeout: float = 60.0) -> None:
        """
        Block until tokens are available or timeout is reached.

        Args:
            tokens: Number of tokens to acquire (default: 1)
            timeout: Maximum time to wait in seconds (default: 60)

        Raises:
            GmailRateLimitExceeded: If timeout is reached without acquiring tokens
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.acquire(tokens=tokens):
                return

            # Sleep for time proportional to refill rate
            sleep_time = min(1.0 / self.refill_rate, 0.1)
            time.sleep(sleep_time)

        raise GmailRateLimitExceeded(
            f"Rate limit exceeded: could not acquire {tokens} token(s) within {timeout}s"
        )

    def get_token_count(self) -> float:
        """Get current token count without modifying state."""
        pipe = self.redis_client.pipeline()
        pipe.get(self.bucket_key)
        pipe.get(self.timestamp_key)
        results = pipe.execute()

        current_tokens = float(results[0]) if results[0] else self.max_tokens
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
