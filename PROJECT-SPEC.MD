# PROJECT-SPEC.MD

> Single source of truth for requirements and decisions. Updated by both Claude and Tooey.
> All entries require attribution and dates. Format: `Claude [M.D.YY]` or `Tooey [M.D.YY]`

---

## Project Overview

| Field | Value |
|-------|-------|
| **Name** | CRM-HTH |
| **Purpose** | Autonomous email processing pipeline + CRM for relationship intelligence across ENR 400 construction companies |
| **Tech Stack** | Python 3.11, FastAPI, SQLAlchemy 2.0, PostgreSQL (Supabase), Celery + Redis, Playwright, Claude API, Gmail API |
| **Hosting** | Heroku (3 dynos: web, worker, monitor) |
| **Status** | Active |
| **Created** | Feb 25, 2026 |
| **Last Updated** | Mar 10, 2026 |

---

## Requirements

### Core Requirements

| ID | Requirement | Priority | Status | Added By |
|----|-------------|----------|--------|----------|
| REQ-001 | Index all historical Gmail across procore-main account (~1.16M emails) and extract relationship intelligence | High | Implemented | Claude [3.9.26] — inferred from codebase scan |
| REQ-002 | Maintain a CRM database of ENR 400 construction companies (General Contractors, Specialty Contractors, Owners, Trade Groups) with contacts, domains, and enrichment data | High | Implemented | Claude [3.9.26] — inferred from codebase scan |
| REQ-003 | Enrich contacts with LinkedIn profile data (title, company, LinkedIn URL) via automated browser scraping | High | Implemented | Claude [3.9.26] — inferred from codebase scan |
| REQ-004 | Discover company leadership (C-suite executives) and add them as contacts using LLM-based research and website scraping | High | Implemented | Claude [3.9.26] — inferred from codebase scan |
| REQ-005 | Monitor LinkedIn for job changes, title changes, and post activity across tiered contacts | High | Implemented | Claude [3.9.26] — inferred from codebase scan |
| REQ-006 | Scrape news daily and surface ONLY articles that mention a specific contact by name; generate personalized outreach drafts to that contact about their mention/achievement | High | Implemented | Tooey [3.10.26] — narrowed from general company news to contact-specific mentions |
| REQ-007 | Provide a web-based CRM UI for browsing companies, contacts, reports, outreach drafts, and LinkedIn activity | High | Implemented | Claude [3.9.26] — inferred from codebase scan |

### Functional Requirements

| ID | Requirement | Parent REQ | Status | Added By |
|----|-------------|-----------|--------|----------|
| FR-001 | Paginated Companies tab with search, sorting (ARR, name, domain), and filtering by type/tier/ENR | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-002 | Paginated People tab with search, sorting (email count, name, title), and filtering by VIP/type/tags | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-003 | Company detail panel with contacts, discovered contacts, news archive, leadership discovery status, and logo verification | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-004 | Contact detail panel with email history, relationship profile, LinkedIn data, and tier assignment | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-005 | Global search across companies, contacts, and emails | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-006 | Admin tab with 22+ specialized reports (needs LinkedIn, needs enrichment, job changes, leadership discovery, logo review, etc.) | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-007 | Soft delete for companies and contacts (deleted_at column, filtered in all queries) | REQ-002 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-008 | Company merge functionality (combine contacts, emails, enrichment data) | REQ-002 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-009 | Two-phase company LinkedIn enrichment: Google search for URL candidates, then LinkedIn profile extraction | REQ-003 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-010 | Logo verification via perceptual hash comparison between website logo and LinkedIn logo | REQ-003 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-011 | LLM leadership finder using Claude Sonnet + web search tool to discover C-suite executives at GC/SC companies | REQ-004 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-012 | Website-based leadership page discovery (probes 22+ URL patterns, scrapes names/titles/bios) | REQ-004 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-013 | Contact monitoring tier assignment (A/B/C) based on email signal strength (count + recency + direct communication) | REQ-005 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-014 | LinkedIn post activity scraping with tier-based frequency (A=2x/week, B=weekly, C=monthly) | REQ-005 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-015 | Job change detection: flag contacts whose LinkedIn company doesn't match CRM company | REQ-005 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-016 | Outreach tab with "Drafts" sub-tab (edit, send via mailto, dismiss, snooze) and "News Feed" sub-tab (24h/7d/30d filters) | REQ-006 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-017 | Daily news pipeline at 5 AM UTC: scrape → filter for articles mentioning contact names → generate personalized draft to that contact | REQ-006 | Implemented | Tooey [3.10.26] — narrowed to contact-name-mention filter |
| FR-018 | ENR Top 400 data import from parsed PDF/Excel, matched to existing companies by rank or fuzzy name | REQ-002 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-019 | Daily domain-based contact discovery: scan all company domains against email corpus, populate discovered_contacts cache | REQ-001 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-020 | ID-first parallel email processing strategy (10 workers claiming batches with skip_locked) for 5x throughput | REQ-001 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-021 | Voice profile generation from sent emails + voice-matched draft composition | REQ-006 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-022 | Obsidian vault generation: relationship-based People/ notes, Thread/ notes, Dataview dashboards | REQ-001 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-023 | LinkedIn tab showing new posts, job changes, title changes, and tier management | REQ-007 | Implemented | Claude [3.9.26] — inferred from codebase scan |
| FR-024 | Column sorting on Companies and People tabs (clickable headers) | REQ-007 | Planned | Claude [3.9.26] — inferred from MEMORY.md |
| FR-025 | Job change outreach: when LinkedIn monitoring detects a job change (FR-015), auto-generate a congratulatory outreach draft to that contact | REQ-005, REQ-006 | Implemented | Claude [3.10.26] |

### Non-Functional Requirements

| ID | Requirement | Category | Status | Added By |
|----|-------------|----------|--------|----------|
| NFR-001 | API key authentication via X-API-Key header; optional SSO via HTH Corp Portal JWT cookies | Security | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-002 | Correlation IDs (X-Request-ID) on every request for traceability across logs | Observability | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-003 | Automatic credential redaction in production logs | Security | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-004 | Redis-backed token bucket rate limiting for Gmail API (40 QPS, 80% safety margin) | Performance | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-005 | Eager loading (selectinload) for all relationship queries to prevent N+1 | Performance | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-006 | PgBouncer-compatible async database driver (statement_cache_size=0 for asyncpg) | Infrastructure | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-007 | Graceful shutdown handling (SIGINT/SIGTERM) in all enrichment scripts with state persistence for resume | Reliability | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-008 | Human-like behavior simulation in Playwright scripts (random delays, work schedule awareness, scroll patterns) | Anti-Detection | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-009 | Claude prompt caching (cache_control: ephemeral) for 90%+ cost reduction on repeated system prompts | Cost | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-010 | 276+ unit tests passing with Black (line-length 100) + Ruff lint clean | Quality | Implemented | Claude [3.9.26] — inferred from codebase scan |
| NFR-011 | Sentry error tracking in production | Observability | Implemented | Claude [3.9.26] — inferred from codebase scan |

---

## Decision Log

### Architecture & Design Decisions

| # | Date | Decision | Made By | Rationale | Status |
|---|------|----------|---------|-----------|--------|
| AD-001 | Feb 25, 2026 | Use FastAPI + SQLAlchemy 2.0 with sync sessions for API routes, async for Celery workers | Claude [3.9.26] | FastAPI for modern async Python web framework; SQLAlchemy for ORM; sync sessions avoid complexity in request-response cycle while async enables parallel worker processing | Active |
| AD-002 | Feb 25, 2026 | PostgreSQL on Supabase (remote), not local | Claude [3.9.26] | Managed database with backups, accessible from both local dev and Heroku production | Active |
| AD-003 | Feb 25, 2026 | Celery + Redis for task queue with Beat scheduler | Claude [3.9.26] | Mature task queue for background email processing, news scraping, daily crons | Active |
| AD-004 | Mar 2, 2026 | CRM models (Company, Contact) as first-class entities separate from email processing models | Claude [3.9.26] | Clean separation between email intelligence layer and CRM relationship management | Active |
| AD-005 | Mar 4, 2026 | News intelligence pipeline: Google News RSS per company + company website scraping + Claude analysis | Claude [3.9.26] | Multi-source news discovery; Claude Haiku for cheap analysis; draft generation for actionable outreach | Active |
| AD-006 | Mar 5, 2026 | Soft deletes (deleted_at) instead of hard deletes for companies and contacts | Claude [3.9.26] | Preserve data integrity, allow recovery, maintain audit trail | Active |
| AD-007 | Mar 5, 2026 | discovered_contacts cache table populated by daily cron (4 AM UTC) instead of on-demand scanning | Claude [3.9.26] | Keyset pagination on remote Supabase is slow; pre-computed cache avoids per-company query overhead | Active |
| AD-008 | Mar 5, 2026 | Job change detection: contacts stay linked to old company but marked is_active=false | Claude [3.9.26] | Preserves historical relationship context; only reactivated when manually moved to new company | Active |
| AD-009 | Mar 7, 2026 | Three-tier monitoring system (A/B/C) based on email signal strength | Claude [3.9.26] | Focus LinkedIn monitoring resources on highest-value contacts; auto-computed with manual override | Active |
| AD-010 | Mar 8, 2026 | LLM-based leadership discovery (Claude Sonnet + web search) replacing brittle HTML scraping | Claude [3.9.26] | HTML scraping failed for 178/299 companies; LLM approach finds executives even without dedicated leadership pages | Active |
| AD-011 | Mar 5, 2026 | Playwright with saved auth state for LinkedIn automation (no login flow in scripts) | Claude [3.9.26] | Avoid LinkedIn login detection; reuse authenticated browser session via cookies | Active |
| AD-012 | Mar 9, 2026 | News articles filtered by published_at (not created_at) with 30-day staleness cutoff | Claude [3.9.26] | Google News RSS returns articles from years ago; filter ensures only genuinely recent news surfaces | Active |
| AD-013 | Feb 25, 2026 | Only use procore-main Gmail account (tooey@procore.com) for email scanning and discovery | Claude [3.9.26] | User preference: ignore personal and procore-private accounts | Active |
| AD-014 | Mar 10, 2026 | News pipeline filters articles by contact name mention: scrape company news → check if any known contact name appears in title/body → only surface those matches | Claude [3.10.26] | Implements narrowed REQ-006; reduces noise to only personally actionable articles | Active |
| AD-015 | Mar 10, 2026 | Job change events (from LinkedIn monitoring) generate outreach drafts automatically, same as news mentions | Claude [3.10.26] | Job moves are a natural outreach trigger; reuses existing draft infrastructure | Active |

### Clarification Decisions

| # | Date | Question | Asked By | Answer | Answered By | Status |
|---|------|----------|----------|--------|-------------|--------|
| CD-001 | Mar 5, 2026 | What are the valid company types? | Claude | "General Contractor", "Owner", "Specialty Contractor", "Trade Group" | Tooey | Active |
| CD-002 | Mar 5, 2026 | Are ENR-ranked companies always General Contractor? | Claude | Yes, all ENR-ranked companies are always General Contractor | Tooey | Active |
| CD-003 | Mar 5, 2026 | Which Gmail accounts should be used for scanning? | Claude | Only procore-main (tooey@procore.com); ignore personal and procore-private | Tooey | Active |

---

## Changed Decisions Log

> When we reverse or modify a previous decision. Never delete the original — log the change here.

| # | Date | Original # | What Changed | Changed By | Reason |
|---|------|-----------|--------------|------------|--------|
| CHG-001 | Mar 10, 2026 | REQ-006 | Narrowed from "scrape all company news" to "only articles that mention a contact by name." General company news was too noisy and not actionable for personal outreach. | Tooey [3.10.26] | Volume was unmanageable; goal is personal emails about a contact's specific achievement/mention, not general company updates |
| CHG-002 | Mar 10, 2026 | FR-017 | Pipeline now filters articles for contact name mentions instead of surfacing all company news | Tooey [3.10.26] | Follows from CHG-001 |
| CHG-003 | Mar 10, 2026 | AD-005 | News pipeline scope narrowed: still scrape Google News per company, but only keep articles that mention a known contact's name | Tooey [3.10.26] | Follows from CHG-001; also added job change outreach as a new trigger (FR-025) |

---

## Open Questions

> Unresolved items. Move to Decision Log once answered.

| # | Date | Question | Raised By | Priority |
|---|------|----------|-----------|----------|
| OQ-001 | Mar 9, 2026 | Should Sales Navigator Core be integrated for job change alerts, or continue with custom LinkedIn scraping? | Claude [3.9.26] | Medium |
| OQ-002 | Mar 9, 2026 | Should column sorting be implemented for all Companies/People tab headers (FR-024)? | Claude [3.9.26] | Low |
| OQ-003 | Mar 9, 2026 | What should happen with the ~99 companies that failed LLM leadership discovery due to connectivity issues? Auto-retry on next run? | Claude [3.9.26] | Medium |
