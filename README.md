# Gmail-to-Obsidian Integration

Automated system to process Gmail history across multiple accounts and create a structured Obsidian vault for knowledge management and relationship tracking.

## Features

- **Multi-Account Support**: Authenticate 3 Gmail accounts (Procore main, Procore private, Personal)
- **Smart Contact Merging**: Unifies contacts across accounts by email address
- **AI Theme Detection**: Uses Claude AI to extract explicit and implicit themes from emails
- **Comprehensive Tagging**: Auto-tags emails with topics, interests, relationships, sentiment, domains
- **Obsidian Vault Generation**: Creates structured markdown notes with wikilinks and Dataview queries
- **Cost Optimized**: Uses Claude Batch API with prompt caching for 90%+ cost savings

## Architecture

```
FastAPI Web Service
├── OAuth2 Authentication (3 accounts)
├── Celery Workers (Redis-backed)
│   ├── Gmail Sync Worker
│   ├── Theme Detection Worker (Claude Batch API)
│   └── Vault Writer Worker
└── Supabase PostgreSQL Database
    └── Obsidian Vault (Filesystem)
```

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, SQLAlchemy 2.0
- **Task Queue**: Celery + Redis
- **Database**: Supabase (PostgreSQL)
- **APIs**: Gmail API, Claude API (Anthropic)
- **Deployment**: Heroku (web + worker dynos)

## Prerequisites

- Python 3.11+
- Redis (local or Heroku addon)
- Supabase account (free tier works)
- Google Cloud Project (for Gmail API)
- Anthropic API key (for Claude)
- Heroku account (for production deployment)

## Local Development Setup

### 1. Clone and Setup Environment

```bash
# Clone repository
git clone <repo-url>
cd Obsidian

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
# Copy example env file
cp .env.example .env

# Edit .env with your credentials
# Required:
# - SUPABASE_URL, SUPABASE_KEY, DATABASE_URL
# - GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
# - ANTHROPIC_API_KEY
# - REDIS_URL
# - SECRET_KEY (generate with: openssl rand -hex 32)
```

### 3. Setup Google OAuth2

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable Gmail API and People API
4. Create OAuth 2.0 credentials (Web application)
5. Add authorized redirect URI: `http://localhost:8000/auth/callback`
6. Copy Client ID and Client Secret to `.env`

### 4. Setup Supabase

1. Create project at [supabase.com](https://supabase.com)
2. Get your project URL and anon key from Settings > API
3. Get database URL from Settings > Database > Connection String (URI)
4. Add to `.env` file

### 5. Run Database Migrations

```bash
# Initialize Alembic (if not already done)
alembic init migrations

# Run migrations
alembic upgrade head
```

### 6. Start Services

You'll need 4 terminal windows:

**Terminal 1: Redis**
```bash
redis-server
```

**Terminal 2: FastAPI**
```bash
uvicorn src.api.main:app --reload
```

**Terminal 3: Celery Worker**
```bash
celery -A src.worker.celery_app worker --loglevel=info
```

**Terminal 4: Flower (Optional - Monitoring)**
```bash
celery -A src.worker.celery_app flower
```

### 7. Access Application

- **API Docs**: http://localhost:8000/docs
- **Flower Dashboard**: http://localhost:5555

## Usage

### 1. Authenticate Gmail Accounts

Visit each auth URL to authorize:
- `GET /auth/login/procore-main`
- `GET /auth/login/procore-private`
- `GET /auth/login/personal`

Check authentication status:
```bash
GET /auth/status?user_id=<your-user-id>
```

### 2. Start Full Scan

```bash
POST /scan/start
{
  "user_id": "<your-user-id>",
  "account_labels": ["procore-main", "procore-private", "personal"]
}
```

Response:
```json
{
  "job_id": "abc-123-def-456",
  "status": "queued",
  "accounts": ["procore-main", "procore-private", "personal"],
  "status_url": "/status/abc-123-def-456"
}
```

### 3. Monitor Progress

```bash
GET /status/{job_id}
```

Response:
```json
{
  "job_id": "abc-123-def-456",
  "status": "running",
  "phase": "themes",
  "progress": 45,
  "emails_processed": 4500,
  "started_at": "2026-02-25T10:00:00Z"
}
```

### 4. Open Vault in Obsidian

Once complete, open Obsidian and select:
`/Users/tooeycourtemanche/Documents/Obsidian Vault - Gmail`

## Production Deployment (Heroku)

### 1. Create Heroku App

```bash
heroku create gmail-obsidian-sync
```

### 2. Add Addons

```bash
# PostgreSQL (or use existing Supabase)
heroku addons:create heroku-postgresql:mini

# Redis
heroku addons:create heroku-redis:mini
```

### 3. Set Environment Variables

```bash
heroku config:set ANTHROPIC_API_KEY=sk-ant-...
heroku config:set GOOGLE_CLIENT_ID=...
heroku config:set GOOGLE_CLIENT_SECRET=...
heroku config:set SUPABASE_URL=...
heroku config:set SUPABASE_KEY=...
heroku config:set SECRET_KEY=$(openssl rand -hex 32)
# ... set all other env vars from .env
```

### 4. Deploy

```bash
git push heroku main
```

### 5. Scale Dynos

```bash
# 1 web dyno, 2 worker dynos
heroku ps:scale web=1 worker=2
```

### 6. Run Migrations

```bash
heroku run alembic upgrade head
```

## Project Structure

```
Obsidian/
├── src/
│   ├── core/
│   │   └── config.py              # Pydantic settings
│   ├── models/                    # SQLAlchemy models
│   │   ├── user.py
│   │   ├── account.py
│   │   ├── contact.py
│   │   ├── email.py
│   │   └── job.py
│   ├── integrations/
│   │   ├── gmail/
│   │   │   ├── auth.py           # OAuth2 flow
│   │   │   ├── client.py         # Gmail API client
│   │   │   └── rate_limiter.py   # Token bucket rate limiter
│   │   └── claude/
│   │       └── batch_processor.py # Claude Batch API
│   ├── services/
│   │   ├── theme_detection/
│   │   │   └── prompt_template.py # Theme extraction prompts
│   │   └── obsidian/
│   │       ├── vault_manager.py   # Vault initialization
│   │       └── note_generator.py  # Markdown generation
│   ├── api/
│   │   ├── main.py               # FastAPI app
│   │   └── routers/
│   │       ├── auth.py           # Auth endpoints
│   │       └── scan.py           # Scan endpoints
│   └── worker/
│       ├── celery_app.py         # Celery configuration
│       └── tasks.py              # Main orchestration task
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── migrations/                    # Alembic migrations
├── .env.example
├── requirements.txt
├── pyproject.toml
├── Procfile                       # Heroku deployment
└── README.md
```

## Cost Estimates

### LLM API (30,000 emails)
- **With Batch API + Prompt Caching**: ~$12
- **Without optimization**: ~$120 (10x more)

### Infrastructure (Heroku)
- Web dyno: $7/month
- 2x Worker dynos: $14/month
- Redis: $15/month
- PostgreSQL: $9/month (or use Supabase free tier)
- **Total**: ~$45/month

### Self-Hosted (Free)
- Run locally with free Supabase tier

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_contact_merging.py

# Run E2E test (requires setup)
pytest tests/e2e/test_full_scan.py -v
```

## Development Commands

```bash
# Format code
black src tests

# Lint code
ruff check src tests

# Type checking
mypy src

# Create migration
alembic revision -m "description"

# Run migration
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

## Troubleshooting

### OAuth2 Issues
- Verify redirect URI matches Google Cloud Console
- Check that Gmail API and People API are enabled
- Ensure credentials aren't expired

### Rate Limiting
- Gmail API: 250 QPS default (adjust in .env)
- Exponential backoff with tenacity handles 429 errors

### Database Connection
- Verify DATABASE_URL format: `postgresql://user:pass@host:port/db`
- Check Supabase connection pooling settings

### Celery Not Processing
- Verify Redis is running: `redis-cli ping`
- Check Celery logs: `celery -A src.worker.celery_app inspect active`
- Monitor with Flower: http://localhost:5555

## Security

- **OAuth credentials**: Encrypted in database using pgcrypto
- **API keys**: Stored in `.env`, never committed
- **Logs**: All sensitive data redacted with `[REDACTED]`
- **No email bodies**: Only 500-char summaries stored

## License

MIT

## Support

For issues or questions:
- GitHub Issues: <repo-url>/issues
- Documentation: See `/docs` in API
