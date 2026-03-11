"""
Celery tasks for the Company News Intelligence pipeline.

Daily schedule (via Celery Beat):
  5:00 AM UTC: run_news_pipeline -- scrape, analyze, generate drafts
"""

from src.core.database import WorkerSessionLocal as SessionLocal
from src.core.logging import get_logger
from src.worker.celery_app import celery_app

logger = get_logger(__name__)


@celery_app.task(bind=True, name="scrape_company_news")
def scrape_company_news(self, user_id: str) -> dict:
    """Scrape all enabled company news pages + RSS feeds."""
    db = SessionLocal()
    try:
        from src.services.news.scraper import NewsScraperService

        scraper = NewsScraperService(db)
        try:
            company_stats = scraper.scrape_all_companies(user_id)
            google_stats = scraper.scrape_google_news_per_company(user_id)
            rss_stats = scraper.scrape_rss_feeds(user_id)
            web_stats = scraper.scrape_web_feeds(user_id)
        finally:
            scraper.close()

        return {
            "companies": company_stats,
            "google_news": google_stats,
            "rss": rss_stats,
            "web": web_stats,
        }
    except Exception:
        logger.exception("scrape_company_news failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="analyze_news_items")
def analyze_news_items(self, user_id: str) -> dict:
    """Classify new articles with Claude Haiku."""
    db = SessionLocal()
    try:
        from src.services.news.analyzer import NewsAnalysisService

        analyzer = NewsAnalysisService(db)
        try:
            return analyzer.analyze_batch(user_id, limit=500)
        finally:
            analyzer.close()
    except Exception:
        logger.exception("analyze_news_items failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="generate_draft_suggestions")
def generate_draft_suggestions(self, user_id: str) -> dict:
    """Generate email drafts for high-relevance news."""
    db = SessionLocal()
    try:
        from src.services.news.draft_generator import NewsDraftGeneratorService

        generator = NewsDraftGeneratorService(db)
        return generator.generate_all_pending(user_id)
    except Exception:
        logger.exception("generate_draft_suggestions failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="generate_job_change_drafts")
def generate_job_change_drafts(self, user_id: str) -> dict:
    """Generate congratulatory drafts for contacts with detected job changes."""
    db = SessionLocal()
    try:
        from src.services.news.job_change_drafter import JobChangeDraftService

        service = JobChangeDraftService(db)
        return service.generate_all_pending(user_id)
    except Exception:
        logger.exception("generate_job_change_drafts failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="send_daily_digest")
def send_daily_digest(self, user_id: str) -> dict:
    """Build and send the daily news digest email."""
    from src.core.config import get_settings

    settings = get_settings()
    if not settings.digest_enabled:
        logger.info("Digest disabled, skipping daily digest")
        return {"status": "disabled"}

    db = SessionLocal()
    try:
        from src.services.news.digest import DigestService
        from src.services.news.digest_renderer import render_daily_digest
        from src.services.news.email_sender import DigestEmailSender

        data = DigestService(db).build_daily_digest(user_id)
        if data.total_articles == 0:
            logger.info("No articles for daily digest, skipping")
            return {"status": "empty", "articles": 0}

        subject, html = render_daily_digest(data)
        sent = DigestEmailSender().send(settings.digest_to_email, subject, html)

        return {
            "status": "sent" if sent else "failed",
            "articles": data.total_articles,
            "companies": data.companies_mentioned,
        }
    except Exception:
        logger.exception("send_daily_digest failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="send_weekly_digest")
def send_weekly_digest(self, user_id: str) -> dict:
    """Build and send the weekly news rollup email."""
    from src.core.config import get_settings

    settings = get_settings()
    if not settings.digest_enabled:
        logger.info("Digest disabled, skipping weekly digest")
        return {"status": "disabled"}

    db = SessionLocal()
    try:
        from src.services.news.digest import DigestService
        from src.services.news.digest_renderer import render_weekly_digest
        from src.services.news.email_sender import DigestEmailSender

        data = DigestService(db).build_weekly_digest(user_id)
        if data.total_articles == 0:
            logger.info("No articles for weekly digest, skipping")
            return {"status": "empty", "articles": 0}

        subject, html = render_weekly_digest(data)
        sent = DigestEmailSender().send(settings.digest_to_email, subject, html)

        return {
            "status": "sent" if sent else "failed",
            "articles": data.total_articles,
            "companies": len(data.top_companies),
        }
    except Exception:
        logger.exception("send_weekly_digest failed")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="run_news_pipeline")
def run_news_pipeline(self, user_id: str) -> dict:
    """Run the complete news intelligence pipeline sequentially."""
    logger.info("Starting news intelligence pipeline for user %s", user_id)

    scrape_result = scrape_company_news(user_id)
    logger.info("Scrape phase complete: %s", scrape_result)

    analyze_result = analyze_news_items(user_id)
    logger.info("Analysis phase complete: %s", analyze_result)

    draft_result = generate_draft_suggestions(user_id)
    logger.info("Draft generation complete: %s", draft_result)

    job_change_result = generate_job_change_drafts(user_id)
    logger.info("Job change drafts: %s", job_change_result)

    # Send daily digest (non-blocking — failures don't break pipeline)
    try:
        digest_result = send_daily_digest(user_id)
        logger.info("Daily digest: %s", digest_result)
    except Exception:
        logger.exception("Daily digest failed (non-blocking)")
        digest_result = {"status": "error"}

    return {
        "scrape": scrape_result,
        "analysis": analyze_result,
        "drafts": draft_result,
        "job_changes": job_change_result,
        "digest": digest_result,
    }
