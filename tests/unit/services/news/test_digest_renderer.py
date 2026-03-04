"""Tests for the HTML digest email renderer."""

from datetime import UTC, datetime

from src.services.news.digest import (
    ArticleSummary,
    CompanyNewsGroup,
    DailyDigestData,
    WeeklyDigestData,
)
from src.services.news.digest_renderer import render_daily_digest, render_weekly_digest


def _sample_article(
    title="Turner wins $500M hospital project",
    company="Turner Construction",
    category="project_win",
    score=0.92,
):
    return ArticleSummary(
        title=title,
        url="https://example.com/article",
        company_name=company,
        source_type="google_news",
        category=category,
        relevance_score=score,
    )


class TestDailyRenderer:
    def test_subject_includes_counts_and_date(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=15,
            companies_mentioned=5,
        )
        subject, _ = render_daily_digest(data)
        assert "15 articles" in subject
        assert "5 companies" in subject
        assert "March 04, 2026" in subject

    def test_html_contains_article_titles(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=1,
            companies_mentioned=1,
            top_articles=[_sample_article()],
            by_company=[
                CompanyNewsGroup(
                    company_name="Turner Construction",
                    articles=[_sample_article()],
                )
            ],
        )
        _, html = render_daily_digest(data)
        assert "Turner wins $500M hospital project" in html
        assert "Turner Construction" in html

    def test_html_contains_category_badges(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=1,
            top_articles=[_sample_article(category="expansion")],
        )
        _, html = render_daily_digest(data)
        assert "Expansion" in html

    def test_html_contains_relevance_score(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=1,
            top_articles=[_sample_article(score=0.92)],
        )
        _, html = render_daily_digest(data)
        assert "92%" in html

    def test_html_contains_source_breakdown(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=3,
            source_breakdown={"google_news": 2, "enr": 1},
        )
        _, html = render_daily_digest(data)
        assert "Google News" in html
        assert "ENR" in html

    def test_html_is_valid_structure(self):
        data = DailyDigestData(
            date=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=1,
            top_articles=[_sample_article()],
        )
        _, html = render_daily_digest(data)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "News Intelligence Digest" in html


class TestWeeklyRenderer:
    def test_subject_includes_date_range(self):
        data = WeeklyDigestData(
            week_start=datetime(2026, 2, 25, tzinfo=UTC),
            week_end=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=42,
        )
        subject, _ = render_weekly_digest(data)
        assert "42 articles" in subject
        assert "Feb 25" in subject
        assert "Mar 04, 2026" in subject

    def test_html_contains_company_table(self):
        data = WeeklyDigestData(
            week_start=datetime(2026, 2, 25, tzinfo=UTC),
            week_end=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=5,
            top_companies=[("Turner Construction", 3), ("Kiewit Corp.", 2)],
        )
        _, html = render_weekly_digest(data)
        assert "Turner Construction" in html
        assert "3 articles" in html
        assert "Kiewit Corp." in html

    def test_html_contains_category_badges(self):
        data = WeeklyDigestData(
            week_start=datetime(2026, 2, 25, tzinfo=UTC),
            week_end=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=2,
            category_breakdown={"project_win": 2},
        )
        _, html = render_weekly_digest(data)
        assert "Project Win" in html

    def test_html_contains_draft_stats(self):
        data = WeeklyDigestData(
            week_start=datetime(2026, 2, 25, tzinfo=UTC),
            week_end=datetime(2026, 3, 4, tzinfo=UTC),
            total_articles=10,
            draft_stats={"pending": 5, "sent": 3},
        )
        _, html = render_weekly_digest(data)
        # Total drafts = 8, sent = 3
        assert ">8<" in html
        assert ">3<" in html
