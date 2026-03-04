"""
HTML email renderer for news digest reports.

Produces email-client-compatible HTML using inline CSS and table-based layouts.
"""

from src.services.news.digest import (
    ArticleSummary,
    DailyDigestData,
    WeeklyDigestData,
)

CATEGORY_COLORS = {
    "project_win": ("#059669", "#ECFDF5"),
    "project_completion": ("#2563EB", "#EFF6FF"),
    "executive_hire": ("#7C3AED", "#F5F3FF"),
    "expansion": ("#D97706", "#FFFBEB"),
    "partnership": ("#0D9488", "#F0FDFA"),
    "award": ("#CA8A04", "#FEFCE8"),
    "financial_results": ("#4F46E5", "#EEF2FF"),
}

CATEGORY_LABELS = {
    "project_win": "Project Win",
    "project_completion": "Completion",
    "executive_hire": "New Hire",
    "expansion": "Expansion",
    "partnership": "Partnership",
    "award": "Award",
    "financial_results": "Financial",
}

SOURCE_LABELS = {
    "rss_construction_dive": "Construction Dive",
    "google_news": "Google News",
    "enr": "ENR",
    "bisnow": "Bisnow",
    "bldup": "BldUp",
    "company_website": "Company Site",
}


def _html_wrapper(title: str, body: str) -> str:
    """Wrap content in a full HTML email template."""
    return f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
</head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f3f4f6;">
<tr><td align="center" style="padding:24px 16px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:12px;overflow:hidden;max-width:600px;width:100%;">
<!-- Header -->
<tr><td style="background:linear-gradient(135deg,#4F46E5,#7C3AED);padding:28px 32px;">
<h1 style="margin:0;color:#ffffff;font-size:22px;font-weight:700;">{title}</h1>
</td></tr>
<!-- Body -->
<tr><td style="padding:24px 32px;">
{body}
</td></tr>
<!-- Footer -->
<tr><td style="padding:16px 32px;border-top:1px solid #e5e7eb;background-color:#f9fafb;">
<p style="margin:0;color:#9ca3af;font-size:12px;text-align:center;">
News Intelligence Digest &bull; CRM Pipeline
</p>
</td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""


def _relevance_color(score: float | None) -> str:
    if not score:
        return "#9CA3AF"
    if score >= 0.7:
        return "#059669"
    if score >= 0.4:
        return "#D97706"
    return "#9CA3AF"


def _category_badge(category: str | None) -> str:
    if not category:
        return ""
    color, bg = CATEGORY_COLORS.get(category, ("#6B7280", "#F3F4F6"))
    label = CATEGORY_LABELS.get(category, category.replace("_", " ").title())
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
        f'font-size:11px;font-weight:600;color:{color};background-color:{bg};">'
        f"{label}</span>"
    )


def _source_label(source_type: str) -> str:
    return SOURCE_LABELS.get(source_type, source_type.replace("_", " ").title())


def _render_source_breakdown(source_breakdown: dict[str, int]) -> str:
    """Render source breakdown pills. Shared by daily and weekly digests."""
    if not source_breakdown:
        return ""
    pills = " ".join(
        f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;background-color:#F3F4F6;font-size:12px;color:#374151;margin:2px;">'
        f"{_source_label(src)}: {cnt}</span>"
        for src, cnt in sorted(source_breakdown.items(), key=lambda x: x[1], reverse=True)
    )
    return (
        f'<div style="margin-top:20px;padding-top:16px;border-top:1px solid #e5e7eb;">'
        f'<span style="font-size:12px;color:#6B7280;font-weight:600;">Sources: </span>{pills}'
        f"</div>"
    )


def _render_article_row(article: ArticleSummary) -> str:
    badge = _category_badge(article.category)
    rel_color = _relevance_color(article.relevance_score)
    rel_text = (
        f"{int(article.relevance_score * 100)}%" if article.relevance_score else ""
    )
    source = _source_label(article.source_type)

    return f"""\
<tr>
<td style="padding:10px 0;border-bottom:1px solid #f3f4f6;">
<a href="{article.url}" style="color:#1F2937;text-decoration:none;font-size:14px;font-weight:500;line-height:1.4;">
{article.title}</a>
<div style="margin-top:4px;">
{badge}
<span style="font-size:11px;color:{rel_color};font-weight:600;margin-left:6px;">{rel_text}</span>
<span style="font-size:11px;color:#9CA3AF;margin-left:6px;">{article.company_name}</span>
<span style="font-size:11px;color:#D1D5DB;margin-left:4px;">&bull;</span>
<span style="font-size:11px;color:#9CA3AF;margin-left:4px;">{source}</span>
</div>
</td>
</tr>"""


def render_daily_digest(data: DailyDigestData) -> tuple[str, str]:
    """Render the daily digest email. Returns (subject, html)."""
    date_str = data.date.strftime("%B %d, %Y")
    subject = f"News Digest: {data.total_articles} articles, {data.companies_mentioned} companies - {date_str}"

    # Stats row
    stats = f"""\
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
<tr>
<td style="padding:12px;background-color:#EEF2FF;border-radius:8px;text-align:center;width:33%;">
<div style="font-size:24px;font-weight:700;color:#4F46E5;">{data.total_articles}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Articles</div>
</td>
<td style="width:8px;"></td>
<td style="padding:12px;background-color:#ECFDF5;border-radius:8px;text-align:center;width:33%;">
<div style="font-size:24px;font-weight:700;color:#059669;">{data.companies_mentioned}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Companies</div>
</td>
<td style="width:8px;"></td>
<td style="padding:12px;background-color:#FFF7ED;border-radius:8px;text-align:center;width:33%;">
<div style="font-size:24px;font-weight:700;color:#EA580C;">{data.pending_drafts}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Pending Drafts</div>
</td>
</tr>
</table>"""

    # Top articles
    articles_html = ""
    if data.top_articles:
        rows = "".join(_render_article_row(a) for a in data.top_articles)
        articles_html = f"""\
<h2 style="margin:0 0 12px;font-size:16px;color:#1F2937;">Top Articles by Relevance</h2>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0">
{rows}
</table>"""

    # By company
    company_html = ""
    if data.by_company:
        sections = []
        for group in data.by_company[:8]:
            article_links = "".join(
                f'<li style="margin-bottom:4px;"><a href="{a.url}" style="color:#4F46E5;text-decoration:none;font-size:13px;">{a.title}</a></li>'
                for a in group.articles[:5]
            )
            sections.append(
                f'<div style="margin-bottom:16px;">'
                f'<h3 style="margin:0 0 6px;font-size:14px;color:#1F2937;">{group.company_name} ({len(group.articles)})</h3>'
                f'<ul style="margin:0;padding-left:18px;list-style-type:disc;">{article_links}</ul>'
                f"</div>"
            )
        company_html = (
            f'<h2 style="margin:24px 0 12px;font-size:16px;color:#1F2937;">By Company</h2>'
            + "".join(sections)
        )

    source_html = _render_source_breakdown(data.source_breakdown)

    body = stats + articles_html + company_html + source_html
    html = _html_wrapper(f"Daily News Digest - {date_str}", body)
    return subject, html


def render_weekly_digest(data: WeeklyDigestData) -> tuple[str, str]:
    """Render the weekly rollup email. Returns (subject, html)."""
    start_str = data.week_start.strftime("%b %d")
    end_str = data.week_end.strftime("%b %d, %Y")
    subject = f"Weekly News Rollup: {data.total_articles} articles - {start_str} to {end_str}"

    # Stats
    total_drafts = sum(data.draft_stats.values())
    sent_drafts = data.draft_stats.get("sent", 0)

    stats = f"""\
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
<tr>
<td style="padding:12px;background-color:#EEF2FF;border-radius:8px;text-align:center;width:25%;">
<div style="font-size:24px;font-weight:700;color:#4F46E5;">{data.total_articles}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Articles</div>
</td>
<td style="width:6px;"></td>
<td style="padding:12px;background-color:#ECFDF5;border-radius:8px;text-align:center;width:25%;">
<div style="font-size:24px;font-weight:700;color:#059669;">{len(data.top_companies)}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Companies</div>
</td>
<td style="width:6px;"></td>
<td style="padding:12px;background-color:#FFF7ED;border-radius:8px;text-align:center;width:25%;">
<div style="font-size:24px;font-weight:700;color:#EA580C;">{total_drafts}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Drafts</div>
</td>
<td style="width:6px;"></td>
<td style="padding:12px;background-color:#F0FDF4;border-radius:8px;text-align:center;width:25%;">
<div style="font-size:24px;font-weight:700;color:#16A34A;">{sent_drafts}</div>
<div style="font-size:11px;color:#6B7280;margin-top:2px;">Sent</div>
</td>
</tr>
</table>"""

    # Category breakdown
    category_html = ""
    if data.category_breakdown:
        pills = " ".join(
            f"{_category_badge(cat)} "
            f'<span style="font-size:12px;color:#374151;margin-right:8px;">{cnt}</span>'
            for cat, cnt in sorted(
                data.category_breakdown.items(), key=lambda x: x[1], reverse=True
            )
        )
        category_html = (
            f'<div style="margin-bottom:20px;">'
            f'<h2 style="margin:0 0 8px;font-size:16px;color:#1F2937;">Categories</h2>'
            f"<div>{pills}</div></div>"
        )

    # Top companies
    companies_html = ""
    if data.top_companies:
        rows = "".join(
            f'<tr><td style="padding:6px 0;font-size:13px;color:#1F2937;border-bottom:1px solid #f3f4f6;">{name}</td>'
            f'<td style="padding:6px 0;font-size:13px;color:#4F46E5;font-weight:600;text-align:right;border-bottom:1px solid #f3f4f6;">{cnt} articles</td></tr>'
            for name, cnt in data.top_companies
        )
        companies_html = (
            f'<h2 style="margin:0 0 8px;font-size:16px;color:#1F2937;">Top Companies</h2>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">{rows}</table>'
        )

    # Top articles
    articles_html = ""
    if data.top_articles:
        rows = "".join(_render_article_row(a) for a in data.top_articles)
        articles_html = (
            f'<h2 style="margin:0 0 12px;font-size:16px;color:#1F2937;">Top Articles</h2>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0">{rows}</table>'
        )

    source_html = _render_source_breakdown(data.source_breakdown)

    body = stats + category_html + companies_html + articles_html + source_html
    html = _html_wrapper(f"Weekly News Rollup - {start_str} to {end_str}", body)
    return subject, html
