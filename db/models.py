"""
db/models.py
Database models and engine setup using SQLAlchemy.
Decision: SQLite for local/dev, easily swappable to PostgreSQL via DATABASE_URL env var.
"""

import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    Text, Float, Boolean, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./hiring_intel.db")

# SQLite needs check_same_thread=False; other DBs don't need this arg
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class JobPosting(Base):
    """
    Core table storing each scraped job posting.
    Decisions:
    - source_id is the unique identifier from the source site (used for dedup)
    - salary_min/max are normalized to USD/year floats; NULL if not provided
    - tags stored as comma-separated string (simple, queryable, no join overhead for this scale)
    - seniority_level populated by AI classification layer (bonus)
    - is_surge flagged by AI hiring-surge detection (bonus)
    """
    __tablename__ = "job_postings"

    id            = Column(Integer, primary_key=True, index=True)
    source_id     = Column(String(255), unique=True, nullable=False)   # dedup key
    source        = Column(String(50), nullable=False)                  # e.g. "remoteok"
    title         = Column(String(255), nullable=False)
    company       = Column(String(255), nullable=False)
    location      = Column(String(255), default="Remote")
    url           = Column(Text, nullable=False)
    tags          = Column(Text, default="")                            # comma-separated
    salary_min    = Column(Float, nullable=True)
    salary_max    = Column(Float, nullable=True)
    salary_raw    = Column(String(255), nullable=True)                  # original string kept for audit
    date_posted   = Column(DateTime, nullable=True)
    date_scraped  = Column(DateTime, default=datetime.utcnow)
    description   = Column(Text, default="")

    # AI-enriched fields (bonus layer)
    seniority_level = Column(String(50), nullable=True)   # junior / mid / senior / lead / exec
    is_surge        = Column(Boolean, default=False)       # part of a hiring surge?

    __table_args__ = (
        Index("ix_company", "company"),
        Index("ix_date_posted", "date_posted"),
        Index("ix_source", "source"),
    )


class ScrapeRun(Base):
    """
    Audit log for every pipeline run.
    Decisions: keeps a lightweight record so we can detect failures, 
    measure freshness, and surface pipeline health in the dashboard.
    """
    __tablename__ = "scrape_runs"

    id          = Column(Integer, primary_key=True, index=True)
    started_at  = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status      = Column(String(20), default="running")   # running / success / failed
    jobs_found  = Column(Integer, default=0)
    jobs_new    = Column(Integer, default=0)
    error_msg   = Column(Text, nullable=True)


def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """FastAPI dependency — yields a DB session and ensures it's closed."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
