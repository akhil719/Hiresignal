"""
scraper/remoteok.py

Scrapes RemoteOK's public JSON API for remote job postings.

Why RemoteOK?
- Public JSON endpoint (no login required, no JS rendering needed)
- Clearly structured data with company, title, tags, salary, date
- Returns paginated-style bulk data — we filter by date window
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

REMOTEOK_URL = "https://remoteok.com/api"

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


def _parse_epoch(epoch_val) -> Optional[datetime]:
    """
    Convert epoch timestamp (int or string) to UTC datetime.
    Returns None on any failure — we never crash on a bad date.
    """
    try:
        return datetime.fromtimestamp(int(epoch_val), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _parse_salary(raw: Optional[str]) -> tuple[Optional[float], Optional[float]]:
    """
    Extract numeric min/max salary from strings like:
      "$80k - $120k", "60000-90000", "$100,000", "80k"
    Returns (min_usd_annual, max_usd_annual) as floats.
    Returns (None, None) if unparseable — we keep salary_raw for audit.

    Decision: we normalise to annual USD by assuming 'k' = 1000.
    Hourly rates are not converted (too ambiguous without context).
    """
    if not raw:
        return None, None

    import re
    # Strip currency symbols, commas, spaces
    cleaned = re.sub(r"[$,\s]", "", raw.lower())
    # Find all numeric values (possibly with 'k' suffix)
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


def fetch_jobs(max_pages: int = 3) -> list[dict]:
    """
    Fetch job listings from RemoteOK.

    RemoteOK returns all jobs in a single JSON array (first element is metadata).
    We treat this as one "page" but support future pagination via max_pages param.

    Returns a list of raw job dicts (unmodified from source).
    Raises RuntimeError if all retries are exhausted.
    """
    raw_jobs = []

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching RemoteOK jobs (attempt {attempt + 1})")
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                response = client.get(REMOTEOK_URL, headers=HEADERS)
                response.raise_for_status()

            data = response.json()

            # First element is always a metadata/legal object — skip it
            if isinstance(data, list) and len(data) > 1:
                raw_jobs = [item for item in data[1:] if isinstance(item, dict)]
                logger.info(f"Fetched {len(raw_jobs)} raw jobs from RemoteOK")
            else:
                logger.warning("Unexpected response structure from RemoteOK")
                raw_jobs = []

            break  # success — exit retry loop

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code} on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"RemoteOK scrape failed after {MAX_RETRIES} attempts: {e}")

        except httpx.RequestError as e:
            logger.error(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"RemoteOK scrape failed after {MAX_RETRIES} attempts: {e}")

        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
            else:
                raise RuntimeError(f"Unexpected scrape failure: {e}")

    return raw_jobs


def parse_job(raw: dict) -> dict:
    """
    Transform a raw RemoteOK job dict into a structured intermediate dict.

    All field access is defensive — missing fields become None.
    We preserve salary_raw alongside parsed values for audit purposes.
    This output feeds directly into the cleaner pipeline.
    """
    salary_raw = _safe_get(raw, "salary")
    salary_min, salary_max = _parse_salary(salary_raw)

    # Tags come as a list; coerce to list safely
    tags_raw = _safe_get(raw, "tags", [])
    if isinstance(tags_raw, str):
        tags_raw = [t.strip() for t in tags_raw.split(",") if t.strip()]

    return {
        "source_id":    _safe_get(raw, "id", ""),
        "source":       "remoteok",
        "title":        _safe_get(raw, "position", "Unknown Title"),
        "company":      _safe_get(raw, "company", "Unknown Company"),
        "location":     _safe_get(raw, "location", "Remote"),
        "url":          _safe_get(raw, "url", ""),
        "tags":         tags_raw,           # list at this stage
        "salary_raw":   salary_raw,
        "salary_min":   salary_min,
        "salary_max":   salary_max,
        "date_posted":  _parse_epoch(_safe_get(raw, "epoch")),
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
            # One bad record should never kill the whole run
            errors += 1
            logger.warning(f"Failed to parse job record: {e} | raw={raw.get('id', '?')}")

    logger.info(f"Parsed {len(parsed)} jobs ({errors} parse errors ignored)")
    return parsed
