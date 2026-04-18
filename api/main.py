"""
api/main.py  —  FastAPI application

Endpoints:
  GET  /                     → Dashboard UI (HTML)
  GET  /api/jobs             → Paginated job listings with filters
  GET  /api/companies        → Company hiring activity summary
  GET  /api/trends           → Hiring velocity + surge signals
  GET  /api/stats            → Overall pipeline stats
  POST /api/trigger          → Manually trigger a pipeline run
  GET  /health               → Health check
"""

import os
import sys
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Depends, Query, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

# Make sure project root is on path when running from subdirectory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.models import init_db, get_db, JobPosting, ScrapeRun
from scheduler.jobs import start_scheduler, stop_scheduler
from pipeline import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB + start scheduler. Shutdown: stop scheduler."""
    logger.info("App startup")
    init_db()

    # Run pipeline once on startup if DB is empty
    from db.models import SessionLocal
    db = SessionLocal()
    count = db.query(func.count(JobPosting.id)).scalar()
    db.close()
    if count == 0:
        logger.info("DB is empty — running initial pipeline on startup")
        import threading
        threading.Thread(target=run_pipeline, daemon=True).start()

    start_scheduler()
    yield
    stop_scheduler()
    logger.info("App shutdown")


app = FastAPI(
    title="Hiring Intelligence API",
    description="B2B hiring signal data pipeline — scrapes, cleans, and surfaces job market intelligence.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── Jobs endpoint ──────────────────────────────────────────────────────────────

@app.get("/api/jobs")
def get_jobs(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
    seniority: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    surge_only: bool = Query(False),
    days: Optional[int] = Query(None, description="Only jobs posted in last N days"),
):
    """
    Paginated job listings with optional filters.
    Used by the dashboard table and by B2B consumers of the API.
    """
    q = db.query(JobPosting)

    if company:
        q = q.filter(JobPosting.company.ilike(f"%{company}%"))
    if tag:
        q = q.filter(JobPosting.tags.ilike(f"%{tag}%"))
    if seniority:
        q = q.filter(JobPosting.seniority_level == seniority.lower())
    if location:
        q = q.filter(JobPosting.location.ilike(f"%{location}%"))
    if surge_only:
        q = q.filter(JobPosting.is_surge == True)
    if days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        q = q.filter(JobPosting.date_scraped >= cutoff)

    total = q.count()
    jobs = (
        q.order_by(desc(JobPosting.date_scraped))
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "jobs": [_job_to_dict(j) for j in jobs],
    }


# ── Companies endpoint ─────────────────────────────────────────────────────────

@app.get("/api/companies")
def get_companies(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    days: int = Query(30),
):
    """
    Top companies by job posting volume in the last N days.
    Core B2B signal: who is growing their team fastest?
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    results = (
        db.query(
            JobPosting.company,
            func.count(JobPosting.id).label("job_count"),
            func.max(JobPosting.date_scraped).label("last_seen"),
            func.sum(func.cast(JobPosting.is_surge, int)).label("surge_count"),
        )
        .filter(JobPosting.date_scraped >= cutoff)
        .group_by(JobPosting.company)
        .order_by(desc("job_count"))
        .limit(limit)
        .all()
    )

    return [
        {
            "company":     r.company,
            "job_count":   r.job_count,
            "last_seen":   r.last_seen.isoformat() if r.last_seen else None,
            "has_surge":   (r.surge_count or 0) > 0,
        }
        for r in results
    ]


# ── Trends endpoint ────────────────────────────────────────────────────────────

@app.get("/api/trends")
def get_trends(
    db: Session = Depends(get_db),
    days: int = Query(7),
):
    """
    Hiring velocity trends:
    - Jobs per day over the last N days
    - Top tags (skills in demand)
    - Seniority breakdown
    - Surge companies
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    recent = db.query(JobPosting).filter(JobPosting.date_scraped >= cutoff).all()

    # Jobs per day
    daily: dict = {}
    for job in recent:
        day = job.date_scraped.date().isoformat() if job.date_scraped else "unknown"
        daily[day] = daily.get(day, 0) + 1

    # Top tags
    tag_counter: dict = {}
    for job in recent:
        if job.tags:
            for tag in job.tags.split(","):
                t = tag.strip()
                if t:
                    tag_counter[t] = tag_counter.get(t, 0) + 1
    top_tags = sorted(tag_counter.items(), key=lambda x: -x[1])[:15]

    # Seniority breakdown
    seniority_counter: dict = {}
    for job in recent:
        level = job.seniority_level or "unknown"
        seniority_counter[level] = seniority_counter.get(level, 0) + 1

    # Surge companies
    surge_jobs = [j for j in recent if j.is_surge]
    surge_companies = list({j.company for j in surge_jobs})

    return {
        "period_days":       days,
        "total_jobs":        len(recent),
        "jobs_per_day":      [{"date": k, "count": v} for k, v in sorted(daily.items())],
        "top_tags":          [{"tag": t, "count": c} for t, c in top_tags],
        "seniority_split":   seniority_counter,
        "surge_companies":   surge_companies,
    }


# ── Stats endpoint ─────────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)):
    """Pipeline health + overall dataset stats."""
    total_jobs = db.query(func.count(JobPosting.id)).scalar()
    last_run = db.query(ScrapeRun).order_by(desc(ScrapeRun.started_at)).first()
    total_companies = db.query(func.count(func.distinct(JobPosting.company))).scalar()
    surge_count = db.query(func.count(JobPosting.id)).filter(JobPosting.is_surge == True).scalar()

    return {
        "total_jobs":       total_jobs,
        "total_companies":  total_companies,
        "surge_jobs":       surge_count,
        "last_run": {
            "started_at":  last_run.started_at.isoformat() if last_run else None,
            "status":      last_run.status if last_run else None,
            "jobs_new":    last_run.jobs_new if last_run else 0,
        } if last_run else None,
    }


# ── Manual trigger ─────────────────────────────────────────────────────────────

@app.post("/api/trigger")
def trigger_pipeline(background_tasks: BackgroundTasks):
    """Manually trigger a pipeline run (runs in background)."""
    background_tasks.add_task(run_pipeline)
    return {"message": "Pipeline triggered", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── Dashboard HTML ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve the main dashboard UI."""
    with open(os.path.join(os.path.dirname(__file__), "..", "templates", "dashboard.html"), encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


# ── Helpers ────────────────────────────────────────────────────────────────────

def _job_to_dict(job: JobPosting) -> dict:
    return {
        "id":              job.id,
        "source_id":       job.source_id,
        "source":          job.source,
        "title":           job.title,
        "company":         job.company,
        "location":        job.location,
        "url":             job.url,
        "tags":            job.tags.split(",") if job.tags else [],
        "salary_min":      job.salary_min,
        "salary_max":      job.salary_max,
        "salary_raw":      job.salary_raw,
        "date_posted":     job.date_posted.isoformat() if job.date_posted else None,
        "date_scraped":    job.date_scraped.isoformat() if job.date_scraped else None,
        "seniority_level": job.seniority_level,
        "is_surge":        job.is_surge,
    }
