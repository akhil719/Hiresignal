"""
cleaner/clean.py

Transforms raw parsed job dicts into clean, DB-ready records.

Every cleaning decision is documented inline. The philosophy:
- Prefer keeping data over dropping it — NULLs are fine, lost rows are not
- Standardise formats so the API layer never has to guess
- Deduplication is handled here (not in the scraper, not in the DB insert)
"""

import re
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Title normalisation ────────────────────────────────────────────────────────

# Decision: common abbreviations expanded for consistent search/filter
TITLE_EXPANSIONS = {
    r"\bsr\.?\s*":        "Senior ",
    r"\bjr\.?\s*":        "Junior ",
    r"\beng\.?\b":        "Engineer",
    r"\bmgr\.?\b":        "Manager",
    r"\bdev\.?\b":        "Developer",
    r"\bfull[-\s]?stack\b": "Full Stack",
}

def _normalise_title(title: str) -> str:
    """
    Decision: title case + expand known abbreviations.
    We don't strip seniority words here — that's the AI layer's job.
    """
    if not title:
        return "Unknown Title"
    t = title.strip()
    for pattern, replacement in TITLE_EXPANSIONS.items():
        t = re.sub(pattern, replacement, t, flags=re.IGNORECASE)
    # Title-case but preserve acronyms (2 chars or less stay uppercase)
    return " ".join(
        word if len(word) <= 2 else word.capitalize()
        for word in t.split()
    )


# ── Company normalisation ──────────────────────────────────────────────────────

def _normalise_company(company: str) -> str:
    """
    Decision: strip common legal suffixes for grouping consistency.
    E.g. "Stripe, Inc." and "Stripe Inc" both become "Stripe".
    We keep the raw value in a separate audit column (handled at DB level via source).
    """
    if not company:
        return "Unknown Company"
    c = company.strip()
    # Remove trailing legal suffixes
    c = re.sub(r",?\s*(inc\.?|llc\.?|ltd\.?|corp\.?|gmbh|s\.a\.s?|b\.v\.)$", "", c, flags=re.IGNORECASE)
    return c.strip()


# ── Location normalisation ─────────────────────────────────────────────────────

REMOTE_PATTERNS = re.compile(
    r"\b(remote|worldwide|anywhere|global|distributed)\b", re.IGNORECASE
)

def _normalise_location(location: Optional[str]) -> str:
    """
    Decision: if any remote keyword found, standardise to "Remote".
    Otherwise title-case the location string.
    Missing location → "Remote" (since source is a remote-jobs board).
    """
    if not location:
        return "Remote"
    if REMOTE_PATTERNS.search(location):
        return "Remote"
    return location.strip().title()


# ── Tags normalisation ─────────────────────────────────────────────────────────

# Decision: map common tag variants to a canonical form
TAG_ALIASES = {
    "js":           "javascript",
    "ts":           "typescript",
    "node":         "node.js",
    "nodejs":       "node.js",
    "react.js":     "react",
    "reactjs":      "react",
    "vue.js":       "vue",
    "vuejs":        "vue",
    "postgres":     "postgresql",
    "mongo":        "mongodb",
    "k8s":          "kubernetes",
    "aws":          "amazon web services",
    "gcp":          "google cloud",
    "ml":           "machine learning",
    "ai":           "artificial intelligence",
    "devops":       "devops",
}

def _normalise_tags(tags: list) -> str:
    """
    Decision:
    - Lowercase all tags
    - Apply alias map for consistency
    - Deduplicate
    - Store as comma-separated string (simple, no extra table needed)
    - Cap at 20 tags to avoid noise
    """
    if not tags:
        return ""
    seen = set()
    normalised = []
    for tag in tags[:30]:  # process max 30 before dedup
        t = str(tag).lower().strip()
        t = TAG_ALIASES.get(t, t)
        if t and t not in seen:
            seen.add(t)
            normalised.append(t)
    return ",".join(normalised[:20])


# ── Date normalisation ─────────────────────────────────────────────────────────

def _normalise_date(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Decision: ensure all dates are UTC-aware.
    Future dates (data errors) are nullified.
    Very old dates (>3 years) are kept — historical data has value.
    """
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    # Make naive datetimes UTC-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Reject future dates (clock skew / bad data)
    if dt > now:
        logger.debug(f"Future date rejected: {dt}")
        return None
    return dt


# ── URL cleaning ───────────────────────────────────────────────────────────────

def _clean_url(url: Optional[str], source_id: str) -> str:
    """
    Decision: ensure URL is absolute.
    RemoteOK sometimes returns relative paths like /remote-jobs/123.
    """
    if not url:
        return f"https://remoteok.com/remote-jobs/{source_id}"
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return "https://remoteok.com" + url
    return url


# ── Description cleaning ───────────────────────────────────────────────────────

def _clean_description(desc: Optional[str]) -> str:
    """
    Decision: strip HTML tags from descriptions (RemoteOK returns raw HTML).
    We keep plain text only — the UI will handle rendering.
    Truncate at 5000 chars to keep DB rows lean.
    """
    if not desc:
        return ""
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", desc)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:5000]


# ── Main clean function ────────────────────────────────────────────────────────

def clean_job(raw: dict) -> Optional[dict]:
    """
    Clean a single parsed job dict.
    Returns None if the record is fundamentally unusable (no source_id or title).

    Decision: we don't raise exceptions here — bad records are logged and skipped.
    """
    source_id = str(raw.get("source_id", "")).strip()
    if not source_id:
        logger.warning("Skipping record with no source_id")
        return None

    title = _normalise_title(raw.get("title", ""))
    if title == "Unknown Title" and not raw.get("description"):
        logger.warning(f"Skipping empty record: source_id={source_id}")
        return None

    return {
        "source_id":    source_id,
        "source":       str(raw.get("source", "unknown")).lower().strip(),
        "title":        title,
        "company":      _normalise_company(raw.get("company", "")),
        "location":     _normalise_location(raw.get("location")),
        "url":          _clean_url(raw.get("url"), source_id),
        "tags":         _normalise_tags(raw.get("tags", [])),
        "salary_raw":   raw.get("salary_raw"),
        "salary_min":   raw.get("salary_min"),
        "salary_max":   raw.get("salary_max"),
        "date_posted":  _normalise_date(raw.get("date_posted")),
        "description":  _clean_description(raw.get("description", "")),
        # AI fields default to None — enriched separately
        "seniority_level": None,
        "is_surge":        False,
    }


def clean_batch(raw_jobs: list[dict]) -> list[dict]:
    """
    Clean a list of raw parsed jobs.
    Returns only valid cleaned records.
    Logs a summary of what was dropped and why.
    """
    cleaned = []
    dropped = 0

    for raw in raw_jobs:
        try:
            result = clean_job(raw)
            if result:
                cleaned.append(result)
            else:
                dropped += 1
        except Exception as e:
            dropped += 1
            logger.warning(f"Unexpected clean error for {raw.get('source_id', '?')}: {e}")

    logger.info(
        f"Cleaning complete: {len(cleaned)} valid, {dropped} dropped "
        f"({dropped/(len(raw_jobs) or 1)*100:.1f}% drop rate)"
    )
    return cleaned
