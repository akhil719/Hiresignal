"""
scheduler/jobs.py

Sets up APScheduler to run the pipeline automatically.

Decision: IntervalScheduler every 6 hours balances freshness vs. rate limit
courtesy to RemoteOK. Configurable via SCRAPE_INTERVAL_HOURS env var.
"""

import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_HOURS = int(os.getenv("SCRAPE_INTERVAL_HOURS", "6"))

_scheduler = BackgroundScheduler()


def _run_pipeline_job():
    """Wrapper so APScheduler can import without circular deps."""
    from pipeline import run_pipeline
    logger.info("Scheduler: triggering pipeline run")
    result = run_pipeline()
    logger.info(f"Scheduler: pipeline result = {result}")


def start_scheduler():
    """Start the background scheduler. Called once on app startup."""
    if _scheduler.running:
        return
    _scheduler.add_job(
        func=_run_pipeline_job,
        trigger=IntervalTrigger(hours=SCRAPE_INTERVAL_HOURS),
        id="pipeline_job",
        name="Scrape & ETL pipeline",
        replace_existing=True,
        misfire_grace_time=300,  # 5 min grace period for missed fires
    )
    _scheduler.start()
    logger.info(f"Scheduler started — pipeline runs every {SCRAPE_INTERVAL_HOURS}h")


def stop_scheduler():
    """Gracefully stop the scheduler on app shutdown."""
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
