"""Tests for the digest data assembly service."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from src.services.news.digest import DailyDigestData, DigestService, WeeklyDigestData


def _make_news_item(
    title="Test Article",
    source_type="google_news",
    company_name="Turner Construction",
    category="project_win",
    relevance_score=0.85,
    hours_ago=2,
):
    """Create a mock CompanyNewsItem."""
    item = MagicMock()
    item.id = uuid.uuid4()
    item.title = title
    item.source_url = f"https://example.com/{uuid.uuid4().hex[:8]}"
    item.source_type = source_type
    item.company = MagicMock()
    item.company.name = company_name
    item.analysis = {"category": category, "relevance_score": relevance_score}
    item.published_at = datetime.now(UTC) - timedelta(hours=hours_ago)
    item.created_at = datetime.now(UTC) - timedelta(hours=hours_ago)
    return item


class TestDailyDigest:
    """Tests for build_daily_digest."""

    def test_empty_day_returns_zero_stats(self):
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            []
        )

        service = DigestService(db)
        data = service.build_daily_digest("user-123")

        assert isinstance(data, DailyDigestData)
        assert data.total_articles == 0
        assert data.companies_mentioned == 0
        assert data.top_articles == []
        assert data.by_company == []

    def test_articles_grouped_by_company(self):
        items = [
            _make_news_item(title="Turner wins $500M project", company_name="Turner Construction"),
            _make_news_item(title="Turner hires new CFO", company_name="Turner Construction"),
            _make_news_item(title="Kiewit lands highway deal", company_name="Kiewit Corp."),
        ]
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            items
        )
        db.query.return_value.filter.return_value.scalar.return_value = 0

        service = DigestService(db)
        data = service.build_daily_digest("user-123")

        assert data.total_articles == 3
        assert data.companies_mentioned == 2
        # Turner should be first (more articles)
        assert data.by_company[0].company_name == "Turner Construction"
        assert len(data.by_company[0].articles) == 2
        assert data.by_company[1].company_name == "Kiewit Corp."

    def test_top_articles_sorted_by_relevance(self):
        items = [
            _make_news_item(title="Low relevance", relevance_score=0.3),
            _make_news_item(title="High relevance", relevance_score=0.95),
            _make_news_item(title="Medium relevance", relevance_score=0.6),
        ]
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            items
        )
        db.query.return_value.filter.return_value.scalar.return_value = 0

        service = DigestService(db)
        data = service.build_daily_digest("user-123")

        assert data.top_articles[0].title == "High relevance"
        assert data.top_articles[0].relevance_score == 0.95
        assert data.top_articles[-1].title == "Low relevance"

    def test_source_breakdown_counts(self):
        items = [
            _make_news_item(source_type="google_news"),
            _make_news_item(source_type="google_news"),
            _make_news_item(source_type="enr"),
        ]
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            items
        )
        db.query.return_value.filter.return_value.scalar.return_value = 0

        service = DigestService(db)
        data = service.build_daily_digest("user-123")

        assert data.source_breakdown["google_news"] == 2
        assert data.source_breakdown["enr"] == 1


class TestWeeklyDigest:
    """Tests for build_weekly_digest."""

    def test_empty_week_returns_zero_stats(self):
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            []
        )

        service = DigestService(db)
        data = service.build_weekly_digest("user-123")

        assert isinstance(data, WeeklyDigestData)
        assert data.total_articles == 0
        assert data.top_companies == []
        assert data.top_articles == []

    def test_category_breakdown(self):
        items = [
            _make_news_item(category="project_win"),
            _make_news_item(category="project_win"),
            _make_news_item(category="expansion"),
            _make_news_item(category="executive_hire"),
        ]
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            items
        )
        db.query.return_value.filter.return_value.group_by.return_value.all.return_value = []

        service = DigestService(db)
        data = service.build_weekly_digest("user-123")

        assert data.category_breakdown["project_win"] == 2
        assert data.category_breakdown["expansion"] == 1
        assert data.category_breakdown["executive_hire"] == 1

    def test_top_companies_sorted_by_count(self):
        items = [
            _make_news_item(company_name="Turner Construction"),
            _make_news_item(company_name="Turner Construction"),
            _make_news_item(company_name="Turner Construction"),
            _make_news_item(company_name="Kiewit Corp."),
            _make_news_item(company_name="Skanska"),
        ]
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.order_by.return_value.all.return_value = (
            items
        )
        db.query.return_value.filter.return_value.group_by.return_value.all.return_value = []

        service = DigestService(db)
        data = service.build_weekly_digest("user-123")

        assert data.top_companies[0] == ("Turner Construction", 3)
        assert len(data.top_companies) == 3
