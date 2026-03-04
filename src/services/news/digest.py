"""
Digest data assembly service.

Builds structured data for daily and weekly email digest reports
from the company news intelligence pipeline.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func
from sqlalchemy.orm import Session, joinedload

from src.core.logging import get_logger
from src.models.company import Company
from src.models.company_news import CompanyNewsItem
from src.models.draft_suggestion import DraftSuggestion

logger = get_logger(__name__)


@dataclass
class ArticleSummary:
    """Lightweight article representation for digests."""

    title: str
    url: str
    company_name: str
    source_type: str
    category: str | None = None
    relevance_score: float | None = None
    published_at: datetime | None = None


@dataclass
class CompanyNewsGroup:
    """Articles grouped by company."""

    company_name: str
    articles: list[ArticleSummary] = field(default_factory=list)


@dataclass
class DailyDigestData:
    """Data for the daily digest email."""

    date: datetime
    total_articles: int = 0
    companies_mentioned: int = 0
    top_articles: list[ArticleSummary] = field(default_factory=list)
    by_company: list[CompanyNewsGroup] = field(default_factory=list)
    new_drafts: int = 0
    pending_drafts: int = 0
    source_breakdown: dict[str, int] = field(default_factory=dict)


@dataclass
class WeeklyDigestData:
    """Data for the weekly rollup email."""

    week_start: datetime
    week_end: datetime
    total_articles: int = 0
    category_breakdown: dict[str, int] = field(default_factory=dict)
    top_companies: list[tuple[str, int]] = field(default_factory=list)
    top_articles: list[ArticleSummary] = field(default_factory=list)
    draft_stats: dict[str, int] = field(default_factory=dict)
    source_breakdown: dict[str, int] = field(default_factory=dict)


class DigestService:
    """Assembles digest data from the database."""

    def __init__(self, db: Session):
        self.db = db

    def _to_article_summary(self, item: CompanyNewsItem) -> ArticleSummary:
        analysis = item.analysis or {}
        return ArticleSummary(
            title=item.title,
            url=item.source_url,
            company_name=item.company.name if item.company else "Unknown",
            source_type=item.source_type,
            category=analysis.get("category"),
            relevance_score=analysis.get("relevance_score"),
            published_at=item.published_at,
        )

    @staticmethod
    def _count_sources(items: list[CompanyNewsItem]) -> dict[str, int]:
        """Count articles by source type."""
        breakdown: dict[str, int] = {}
        for item in items:
            breakdown[item.source_type] = breakdown.get(item.source_type, 0) + 1
        return breakdown

    def build_daily_digest(self, user_id: str) -> DailyDigestData:
        """Build daily digest for articles found in the last 24 hours."""
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=24)

        items = (
            self.db.query(CompanyNewsItem)
            .options(joinedload(CompanyNewsItem.company))
            .filter(
                CompanyNewsItem.user_id == user_id,
                CompanyNewsItem.created_at >= since,
            )
            .order_by(CompanyNewsItem.created_at.desc())
            .all()
        )

        data = DailyDigestData(date=now)
        data.total_articles = len(items)

        if not items:
            return data

        # Build ArticleSummary objects once, reuse for top_articles and by_company
        summaries = [self._to_article_summary(i) for i in items]
        scored_pairs = [
            (s, i) for s, i in zip(summaries, items)
            if i.analysis and i.analysis.get("relevance_score")
        ]
        scored_pairs.sort(key=lambda p: p[1].analysis["relevance_score"], reverse=True)
        data.top_articles = [s for s, _ in scored_pairs[:10]]

        # Group by company
        company_groups: dict[str, list[ArticleSummary]] = {}
        for summary in summaries:
            company_groups.setdefault(summary.company_name, []).append(summary)

        data.companies_mentioned = len(company_groups)
        data.by_company = sorted(
            [CompanyNewsGroup(company_name=k, articles=v) for k, v in company_groups.items()],
            key=lambda g: len(g.articles),
            reverse=True,
        )

        data.source_breakdown = self._count_sources(items)

        # Draft stats — single query with conditional counts
        draft_row = (
            self.db.query(
                func.count(case((DraftSuggestion.created_at >= since, 1))).label("new"),
                func.count(case((DraftSuggestion.status == "pending", 1))).label("pending"),
            )
            .filter(DraftSuggestion.user_id == user_id)
            .one()
        )
        data.new_drafts = draft_row.new or 0
        data.pending_drafts = draft_row.pending or 0

        return data

    def build_weekly_digest(self, user_id: str) -> WeeklyDigestData:
        """Build weekly rollup for the last 7 days."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        items = (
            self.db.query(CompanyNewsItem)
            .options(joinedload(CompanyNewsItem.company))
            .filter(
                CompanyNewsItem.user_id == user_id,
                CompanyNewsItem.created_at >= week_start,
            )
            .order_by(CompanyNewsItem.created_at.desc())
            .all()
        )

        data = WeeklyDigestData(week_start=week_start, week_end=now)
        data.total_articles = len(items)

        if not items:
            return data

        # Category breakdown
        for item in items:
            cat = (item.analysis or {}).get("category", "uncategorized")
            data.category_breakdown[cat] = data.category_breakdown.get(cat, 0) + 1

        # Top companies by article count
        company_counts: dict[str, int] = {}
        for item in items:
            name = item.company.name if item.company else "Unknown"
            company_counts[name] = company_counts.get(name, 0) + 1
        data.top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        # Top articles by relevance
        scored = [i for i in items if i.analysis and i.analysis.get("relevance_score")]
        scored.sort(key=lambda i: i.analysis["relevance_score"], reverse=True)
        data.top_articles = [self._to_article_summary(i) for i in scored[:15]]

        data.source_breakdown = self._count_sources(items)

        # Draft stats
        drafts = (
            self.db.query(DraftSuggestion.status, func.count(DraftSuggestion.id))
            .filter(
                DraftSuggestion.user_id == user_id,
                DraftSuggestion.created_at >= week_start,
            )
            .group_by(DraftSuggestion.status)
            .all()
        )
        data.draft_stats = {status: count for status, count in drafts}

        return data
