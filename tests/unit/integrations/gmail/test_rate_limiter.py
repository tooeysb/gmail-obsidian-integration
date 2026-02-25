"""
Unit tests for Gmail rate limiter.
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.integrations.gmail.rate_limiter import (
    GmailRateLimiter,
    GmailRateLimitExceeded,
    rate_limited,
    with_retry,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    with patch("src.integrations.gmail.rate_limiter.redis.from_url") as mock:
        redis_mock = MagicMock()
        mock.return_value = redis_mock

        # Default: full bucket of tokens
        redis_mock.get.side_effect = lambda key: None
        redis_mock.pipeline.return_value.execute.return_value = [None, None]

        yield redis_mock


@pytest.fixture
def rate_limiter(mock_redis):
    """Create rate limiter instance with mocked Redis."""
    limiter = GmailRateLimiter(
        redis_url="redis://localhost:6379/0",
        max_tokens=10,
        refill_rate=10.0,
    )
    return limiter


class TestGmailRateLimiter:
    """Test suite for GmailRateLimiter."""

    def test_init(self, mock_redis):
        """Test rate limiter initialization."""
        limiter = GmailRateLimiter(
            redis_url="redis://localhost:6379/0",
            max_tokens=250,
            refill_rate=250.0,
        )

        assert limiter.max_tokens == 250
        assert limiter.refill_rate == 250.0
        assert limiter.redis_url == "redis://localhost:6379/0"

    def test_acquire_success(self, rate_limiter, mock_redis):
        """Test successful token acquisition."""
        # Setup: bucket has 10 tokens
        mock_redis.pipeline.return_value.execute.return_value = ["10.0", str(time.time())]

        result = rate_limiter.acquire(tokens=1)

        assert result is True
        # Verify token was consumed
        mock_redis.set.assert_called()

    def test_acquire_failure(self, rate_limiter, mock_redis):
        """Test token acquisition failure when bucket is empty."""
        # Setup: bucket has 0 tokens
        mock_redis.pipeline.return_value.execute.return_value = ["0.0", str(time.time())]

        result = rate_limiter.acquire(tokens=1)

        assert result is False

    def test_refill_tokens(self, rate_limiter, mock_redis):
        """Test token refill over time."""
        # Setup: bucket has 5 tokens, last refill was 1 second ago
        past_time = time.time() - 1.0
        mock_redis.pipeline.return_value.execute.return_value = ["5.0", str(past_time)]

        tokens = rate_limiter._refill_tokens()

        # Should have refilled 10 tokens (10 tokens/sec * 1 sec)
        # But capped at max_tokens (10)
        assert tokens == 10.0

    def test_refill_tokens_capped_at_max(self, rate_limiter, mock_redis):
        """Test token refill is capped at max_tokens."""
        # Setup: bucket has 8 tokens, last refill was 5 seconds ago
        past_time = time.time() - 5.0
        mock_redis.pipeline.return_value.execute.return_value = ["8.0", str(past_time)]

        tokens = rate_limiter._refill_tokens()

        # Should try to add 50 tokens (10 tokens/sec * 5 sec)
        # But capped at max_tokens (10)
        assert tokens == 10.0

    def test_wait_for_token_success(self, rate_limiter, mock_redis):
        """Test wait_for_token successfully acquires token."""
        # Setup: bucket has tokens
        mock_redis.pipeline.return_value.execute.return_value = ["10.0", str(time.time())]

        # Should not raise exception
        rate_limiter.wait_for_token(timeout=1.0)

    def test_wait_for_token_timeout(self, rate_limiter, mock_redis):
        """Test wait_for_token times out when no tokens available."""
        # Setup: bucket always has 0 tokens
        mock_redis.pipeline.return_value.execute.return_value = ["0.0", str(time.time())]

        with pytest.raises(GmailRateLimitExceeded):
            rate_limiter.wait_for_token(timeout=0.2)

    def test_get_token_count(self, rate_limiter, mock_redis):
        """Test getting current token count."""
        mock_redis.pipeline.return_value.execute.return_value = ["7.5", str(time.time())]

        count = rate_limiter.get_token_count()

        assert count == 7.5

    def test_reset(self, rate_limiter, mock_redis):
        """Test resetting rate limiter to full capacity."""
        rate_limiter.reset()

        # Verify Redis was updated with max tokens
        calls = mock_redis.pipeline.return_value.set.call_args_list
        assert len(calls) >= 2
        # First call should set tokens to max_tokens
        assert calls[0][0][1] == "10.0"

    def test_close(self, rate_limiter, mock_redis):
        """Test closing Redis connection."""
        rate_limiter.close()

        mock_redis.close.assert_called_once()


class TestRateLimitedDecorator:
    """Test suite for rate_limited decorator."""

    def test_rate_limited_decorator(self, rate_limiter, mock_redis):
        """Test rate_limited decorator enforces rate limiting."""
        mock_redis.pipeline.return_value.execute.return_value = ["10.0", str(time.time())]

        call_count = 0

        @rate_limited(rate_limiter)
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = test_function()

        assert result == "success"
        assert call_count == 1


class TestWithRetryDecorator:
    """Test suite for with_retry decorator."""

    def test_with_retry_success(self):
        """Test with_retry decorator on successful call."""
        call_count = 0

        @with_retry
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = test_function()

        assert result == "success"
        assert call_count == 1

    def test_with_retry_on_rate_limit_error(self):
        """Test with_retry decorator retries on rate limit error."""
        call_count = 0

        @with_retry
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise GmailRateLimitExceeded("Rate limit exceeded")
            return "success"

        result = test_function()

        assert result == "success"
        assert call_count == 3

    def test_with_retry_on_quota_error(self):
        """Test with_retry decorator retries on quota error."""
        call_count = 0

        @with_retry
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Quota exceeded")
            return "success"

        result = test_function()

        assert result == "success"
        assert call_count == 2

    def test_with_retry_max_attempts(self):
        """Test with_retry decorator stops after max attempts."""
        call_count = 0

        @with_retry
        def test_function():
            nonlocal call_count
            call_count += 1
            raise GmailRateLimitExceeded("Rate limit exceeded")

        with pytest.raises(GmailRateLimitExceeded):
            test_function()

        assert call_count == 5  # Should retry 5 times


class TestDistributedRateLimiting:
    """Test suite for distributed rate limiting scenarios."""

    def test_multiple_instances_share_state(self, mock_redis):
        """Test multiple rate limiter instances share Redis state."""
        limiter1 = GmailRateLimiter(
            redis_url="redis://localhost:6379/0",
            max_tokens=10,
            refill_rate=10.0,
        )
        limiter2 = GmailRateLimiter(
            redis_url="redis://localhost:6379/0",
            max_tokens=10,
            refill_rate=10.0,
        )

        # Both should use same Redis keys
        assert limiter1.bucket_key == limiter2.bucket_key
        assert limiter1.timestamp_key == limiter2.timestamp_key

    def test_token_consumption_across_instances(self, mock_redis):
        """Test token consumption is tracked across instances."""
        # Setup: bucket has 5 tokens
        mock_redis.pipeline.return_value.execute.return_value = ["5.0", str(time.time())]

        limiter1 = GmailRateLimiter(
            redis_url="redis://localhost:6379/0",
            max_tokens=10,
            refill_rate=10.0,
        )

        # Instance 1 acquires token
        result = limiter1.acquire(tokens=1)
        assert result is True

        # Verify Redis was updated (token consumed)
        mock_redis.set.assert_called()
