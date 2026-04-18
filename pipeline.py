"""
pipeline.py

Orchestrates the full ETL pipeline:
  Scrape → Clean → AI Enrich → Store

This is the single entry point called by the scheduler and by manual triggers.
It writes a ScrapeRun audit log entry for every execution.
"""

import asyncio
import logging
from datetime import datetime, timezone

from db.models import SessionLocal, JobPosting, ScrapeRun
from scraper.remoteok import scrape
from cleaner.clean import clean_batch
from ai.enricher import classify_seniority_batch, detect_surges

logger = logging.getLogger(__name__)


def run_pipeline() -> dict:
    """
    Synchronous entry point for the scheduler.
    Returns a summary dict with run stats.
    """
    db = SessionLocal()
    run = ScrapeRun(started_at=datetime.now(timezone.utc), status="running")
    db.add(run)
    db.commit()

    try:
        # ── 1. Scrape ──────────────────────────────────────────────────────────
        logger.info("Pipeline: starting scrape")
        raw_jobs = scrape()
        run.jobs_found = len(raw_jobs)
        db.commit()

        # ── 2. Clean ───────────────────────────────────────────────────────────
        logger.info("Pipeline: cleaning data")
        cleaned_jobs = clean_batch(raw_jobs)

        # ── 3. Dedup against DB ────────────────────────────────────────────────
        # Decision: check source_id uniqueness before AI calls to avoid
        # wasting tokens on jobs we already have.
        existing_ids = {
            row[0] for row in db.query(JobPosting.source_id).all()
        }
        new_jobs = [j for j in cleaned_jobs if j["source_id"] not in existing_ids]
        logger.info(f"Pipeline: {len(new_jobs)} new jobs after dedup (skipped {len(cleaned_jobs) - len(new_jobs)})")

        if not new_jobs:
            run.status = "success"
            run.jobs_new = 0
            run.finished_at = datetime.now(timezone.utc)
            db.commit()
            return {"status": "success", "jobs_found": len(raw_jobs), "jobs_new": 0}

        # ── 4. AI Enrichment ───────────────────────────────────────────────────
        logger.info("Pipeline: running AI enrichment")

        # Surge detection uses existing DB records as baseline
        all_existing = db.query(JobPosting).all()
        new_jobs = detect_surges(all_existing, new_jobs)

        # Seniority classification (async, batched)
        new_jobs = asyncio.run(classify_seniority_batch(new_jobs))

        # ── 5. Insert into DB ──────────────────────────────────────────────────
        logger.info(f"Pipeline: inserting {len(new_jobs)} records")
        inserted = 0
        for job_data in new_jobs:
            try:
                job = JobPosting(**job_data)
                db.add(job)
                inserted += 1
            except Exception as e:
                logger.warning(f"Failed to insert job {job_data.get('source_id', '?')}: {e}")
                db.rollback()

        db.commit()

        run.status = "success"
        run.jobs_new = inserted
        run.finished_at = datetime.now(timezone.utc)
        db.commit()

        logger.info(f"Pipeline complete: {inserted} new jobs inserted")
        return {
            "status":     "success",
            "jobs_found": len(raw_jobs),
            "jobs_new":   inserted,
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        run.status = "failed"
        run.error_msg = str(e)
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        return {"status": "failed", "error": str(e)}

    finally:
        db.close()
