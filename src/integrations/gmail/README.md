# Gmail Integration

This module provides a robust Gmail API client with rate limiting and batch operations.

## Components

### GmailRateLimiter (`rate_limiter.py`)

Token bucket rate limiter with Redis backing for distributed rate limiting.

**Features:**
- Token bucket algorithm for smooth rate limiting
- Redis-backed state for multi-worker coordination
- Configurable QPS limit (default: 250 from `settings.gmail_rate_limit_qps`)
- Exponential backoff retry decorator
- Thread-safe token acquisition

**Usage:**
```python
from src.integrations.gmail.rate_limiter import GmailRateLimiter, rate_limited, with_retry

# Create rate limiter
limiter = GmailRateLimiter()

# Use as decorator
@rate_limited(limiter)
def api_call():
    # Your API call here
    pass

# Or manually
if limiter.acquire():
    # Make API call
    pass
```

### GmailClient (`client.py`)

Gmail API client with comprehensive email and contact fetching capabilities.

**Features:**
- OAuth2 credential management with auto-refresh
- Pagination support for large datasets
- Batch operations to avoid N+1 queries
- Email metadata parsing
- Contact fetching from People API
- Automatic rate limiting on all API calls

**Usage:**

```python
from src.integrations.gmail.client import GmailClient

# Initialize client with OAuth2 credentials
credentials = {
    "access_token": "...",
    "refresh_token": "...",
    "token_uri": "...",
    "client_id": "...",
    "client_secret": "...",
    "scopes": ["..."],
}

client = GmailClient(credentials)

# Fetch contacts with pagination
contacts, next_token = client.fetch_contacts(page_size=1000)
while next_token:
    more_contacts, next_token = client.fetch_contacts(
        page_size=1000,
        page_token=next_token
    )
    contacts.extend(more_contacts)

# Fetch email IDs
message_ids, next_token = client.fetch_emails_chunked(batch_size=500)

# Batch fetch full message details (avoids N+1)
emails = client.fetch_message_batch(message_ids)

for email in emails:
    print(f"From: {email['sender_email']}")
    print(f"Subject: {email['subject']}")
    print(f"Date: {email['date']}")

# Get full message body (use sparingly)
body = client.get_message_body(message_id)

# Clean up
client.close()
```

## Configuration

Set these environment variables:

```env
REDIS_URL=redis://localhost:6379/0
GMAIL_RATE_LIMIT_QPS=250
GMAIL_BATCH_SIZE=500
```

## Rate Limiting

The rate limiter enforces Gmail API quotas:
- Default: 250 queries per second (QPS)
- Uses token bucket algorithm
- Automatic exponential backoff on errors (4s min, 60s max)
- Maximum 5 retry attempts

## Error Handling

All methods use `@with_retry` decorator for resilience:
- Retries on rate limit errors (429)
- Retries on quota exceeded errors
- Exponential backoff between retries
- Raises `GmailClientError` after max retries

## Batch Operations

To avoid N+1 query problems:

```python
# ❌ BAD: N+1 queries
message_ids = client.fetch_emails_chunked()[0]
for msg_id in message_ids:  # N API calls
    email = client.get_message(msg_id)

# ✅ GOOD: Batch fetch
message_ids = client.fetch_emails_chunked()[0]
emails = client.fetch_message_batch(message_ids)  # 1 batch API call
```

## Testing

Run unit tests:

```bash
pytest tests/unit/integrations/gmail/test_rate_limiter.py -v
pytest tests/unit/integrations/gmail/test_client.py -v
```

## Checkpoint Support

All pagination methods return `(data, next_page_token)` tuples for resumable operations:

```python
# Save checkpoint
message_ids, checkpoint = client.fetch_emails_chunked(batch_size=500)
save_to_db(checkpoint_token=checkpoint)

# Resume from checkpoint
message_ids, checkpoint = client.fetch_emails_chunked(
    batch_size=500,
    page_token=checkpoint
)
```

## Architecture

```
┌─────────────────┐
│  Celery Worker  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  GmailClient    │─────▶│ Gmail API    │
└────────┬────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│ RateLimiter     │─────▶│ Redis        │
└─────────────────┘      └──────────────┘
```

## Dependencies

- `google-api-python-client` - Gmail and People API client
- `google-auth` - OAuth2 authentication
- `redis` - Distributed rate limiting
- `tenacity` - Retry logic with exponential backoff
