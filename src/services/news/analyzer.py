"""
News analysis service.

Classifies company news items using Claude Haiku to determine
relevance and suggest outreach angles.
"""

import json
from datetime import UTC, datetime

import httpx
from anthropic import Anthropic
from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.logging import get_logger
from src.models.company_news import CompanyNewsItem

logger = get_logger(__name__)

NEWS_ANALYSIS_SYSTEM_PROMPT = """You are a construction industry intelligence analyst. \
Analyze the following news article from a construction company website and classify it \
for CRM outreach purposes.

Return ONLY valid JSON (no markdown, no explanation) with these fields:
{
    "category": "project_win|project_completion|executive_hire|expansion|partnership|award|financial_results|other",
    "relevance_score": 0.0 to 1.0,
    "entities": ["key entity names mentioned"],
    "outreach_angle": "one sentence suggesting why and how to reach out",
    "summary": "2-3 sentence factual summary of the article"
}

Relevance scoring guide:
- 0.8-1.0: Direct outreach trigger (major project win, executive hire, significant expansion)
- 0.6-0.7: Strong signal (award, partnership, project milestone)
- 0.3-0.5: Moderate signal (financial results, industry recognition)
- 0.0-0.2: Weak/no signal (thought leadership, generic blog, marketing content)"""


class NewsAnalysisService:
    """Classifies news items using Claude Haiku."""

    def __init__(self, db: Session):
        self.db = db
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = settings.claude_model  # Haiku for cost efficiency
        self.http_client = httpx.Client(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRM-NewsBot/1.0)"},
        )

    def close(self):
        self.http_client.close()

    def _fetch_article_text(self, url: str) -> str:
        """Fetch and extract plain text from an article URL."""
        try:
            resp = self.http_client.get(url)
            resp.raise_for_status()
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(resp.text, "lxml")

            # Remove navigation, footer, sidebar
            for tag in soup.find_all(["nav", "footer", "aside", "header", "script", "style"]):
                tag.decompose()

            # Try article content first
            article = soup.find("article") or soup.find(
                class_=lambda c: c and "content" in str(c).lower()
            )
            if article:
                text = article.get_text(separator="\n", strip=True)
            else:
                text = soup.get_text(separator="\n", strip=True)

            # Truncate to ~4000 chars for cost efficiency
            return text[:4000]
        except Exception as e:
            logger.warning("Failed to fetch article text from %s: %s", url, e)
            return ""

    def analyze_item(self, item: CompanyNewsItem) -> dict | None:
        """
        Classify a single news item with Claude Haiku.
        Updates item.analysis and item.status in place.
        """
        # Build article context from title, summary, and fetched text
        article_text = ""
        if item.summary:
            article_text = item.summary
        else:
            article_text = self._fetch_article_text(item.source_url)

        if not article_text:
            article_text = item.title

        company_name = item.company.name if item.company else "Unknown"

        user_prompt = (
            f"Company: {company_name}\n"
            f"Article Title: {item.title}\n"
            f"Source: {item.source_type}\n\n"
            f"Article Content:\n{article_text}"
        )

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                system=[
                    {
                        "type": "text",
                        "text": NEWS_ANALYSIS_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            )

            response_text = message.content[0].text.strip()

            # Strip markdown code blocks if present
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1]
            if response_text.endswith("```"):
                response_text = response_text.rsplit("```", 1)[0]
            response_text = response_text.strip()

            analysis = json.loads(response_text)

            item.analysis = analysis
            item.analyzed_at = datetime.now(UTC)
            item.status = "analyzed"

            return analysis

        except json.JSONDecodeError:
            logger.error("Failed to parse Claude response for %s: %s", item.title, response_text)
            return None
        except Exception:
            logger.exception("Analysis failed for news item %s", item.id)
            return None

    def analyze_batch(self, user_id: str, limit: int = 100) -> dict:
        """
        Analyze up to `limit` unanalyzed news items.
        Returns stats: {analyzed, errors, high_relevance}
        """
        items = (
            self.db.query(CompanyNewsItem)
            .filter(
                CompanyNewsItem.user_id == user_id,
                CompanyNewsItem.status == "new",
            )
            .order_by(CompanyNewsItem.created_at)
            .limit(limit)
            .all()
        )

        stats = {"analyzed": 0, "errors": 0, "high_relevance": 0}

        for i, item in enumerate(items):
            logger.info("[%d/%d] Analyzing: %s", i + 1, len(items), item.title[:80])

            result = self.analyze_item(item)
            if result:
                stats["analyzed"] += 1
                if result.get("relevance_score", 0) >= settings.news_relevance_threshold:
                    stats["high_relevance"] += 1
            else:
                stats["errors"] += 1

            # Commit in batches of 10
            if (i + 1) % 10 == 0:
                self.db.commit()

        self.db.commit()
        logger.info("Analysis complete: %s", stats)
        return stats
