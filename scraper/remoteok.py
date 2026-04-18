"""
scraper/remoteok.py

Scrapes Remotive's public JSON API for remote job postings.

Why Remotive?
- Public JSON endpoint (no login required, no JS rendering needed)
- Clearly structured data with company, title, tags, salary, date
- No IP blocking or rate limiting on server requests
- Terms of service allow non-commercial data access

Design decisions:
- Use httpx with timeout + retry logic instead of raw requests
- Exponential backoff on failure (max 3 retries)
- All fields treated as optional — missing = None, never a crash
- Raw data exported exactly as received; cleaning happens in cleaner/
"""

import httpx
import time
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

REMOTEOK_URL = "https://remotive.com/api/remote-jobs"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 10]  # seconds between retries


def _safe_get(d: dict, key: str, default=None):
    """Get a value from a dict; return default if missing or empty string."""
    val = d.get(key, default)
    if val == "" or val is None:
        return default
    return val


def _parse_date(date_str) -> Optional[datetime]:
    """
    Convert ISO date string to UTC datetime.
    Returns None on any failure — we never crash on a bad date.
    """
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        return None


def _parse_salary(raw: Optional[str]) -> tuple[Optional[float], Optional[float]]:
    """
    Extract numeric min/max salary from strings like:
      "$80k - $120k", "60000-90000", "$100,000", "80k"
    Returns (min_usd_annual, max_usd_annual) as floats.
    Returns (None, None) if unparseable — we keep salary_raw for audit.
    """
    if not raw:
        return None, None

    import re
    cleaned = re.sub(r"[$,\s]", "", raw.lower())
    nums = re.findall(r"(\d+(?:\.\d+)?)(k?)", cleaned)

    values = []
    for num, suffix in nums:
        val = float(num)
        if suffix == "k":
            val *= 1000
        values.append(val)

    if not values:
        return None, None
    if len(values) == 1:
        return values[0], values[0]
    return min(values), max(values)


def fetch_jobs() -> list[dict]:
    """
    Fetch job listings from Remotive API.

    Remotive returns all jobs in a single JSON object with a 'jobs' key.
    Returns a list of raw job dicts (unmodified from source).
    Raises RuntimeError if all retries are exhausted.
    """
    raw_jobs = []

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching Remotive jobs (attempt {attempt + 1})")
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                response = client.get(REMOTEOK_URL, headers=HEADERS)
                response.raise_for_status()

            data = response.json()
            raw_jobs = data.get("jobs", [])
            logger.info(f"Fetched {len(raw_jobs)} raw jobs from Remotive")
            break  # success — exit retry loop

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code} on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"Remotive scrape failed after {MAX_RETRIES} attempts: {e}")

        except httpx.RequestError as e:
            logger.error(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"Remotive scrape failed after {MAX_RETRIES} attempts: {e}")

        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"Unexpected scrape failure: {e}")

    return raw_jobs


def parse_job(raw: dict) -> dict:
    """
    Transform a raw Remotive job dict into a structured intermediate dict.

    All field access is defensive — missing fields become None.
    We preserve salary_raw alongside parsed values for audit purposes.
    This output feeds directly into the cleaner pipeline.
    """
    salary_raw = _safe_get(raw, "salary")
    salary_min, salary_max = _parse_salary(salary_raw)

    # Tags come as a list from Remotive
    tags_raw = _safe_get(raw, "tags", [])
    if isinstance(tags_raw, str):
        tags_raw = [t.strip() for t in tags_raw.split(",") if t.strip()]

    return {
        "source_id":    str(_safe_get(raw, "id", "")),
        "source":       "remotive",
        "title":        _safe_get(raw, "title", "Unknown Title"),
        "company":      _safe_get(raw, "company_name", "Unknown Company"),
        "location":     _safe_get(raw, "candidate_required_location", "Remote"),
        "url":          _safe_get(raw, "url", ""),
        "tags":         tags_raw,
        "salary_raw":   salary_raw,
        "salary_min":   salary_min,
        "salary_max":   salary_max,
        "date_posted":  _parse_date(_safe_get(raw, "publication_date")),
        "description":  _safe_get(raw, "description", ""),
    }


def scrape() -> list[dict]:
    """
    Main entry point.
    Returns a list of parsed (but uncleaned) job dicts.
    """
    raw_jobs = fetch_jobs()
    parsed = []
    errors = 0

    for raw in raw_jobs:
        try:
            parsed.append(parse_job(raw))
        except Exception as e:
            errors += 1
            logger.warning(f"Failed to parse job record: {e} | raw={raw.get('id', '?')}")

    logger.info(f"Parsed {len(parsed)} jobs ({errors} parse errors ignored)")
    return parsed