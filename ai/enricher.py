"""
ai/enricher.py  — Bonus AI/ML Layer

What it does:
  1. Seniority classification  — reads job title + description, outputs:
     junior / mid / senior / lead / exec
  2. Hiring surge detection    — groups companies, flags those posting
     significantly more than their rolling average (surge signal)

Why this approach:
  - LLM classification >> regex for seniority: job titles are inconsistent
    ("Staff Engineer" vs "L5" vs "Principal" vs "Senior II") — an LLM handles
    the full semantic space without maintaining a brittle keyword list.
  - Surge detection is rule-based (statistical) not LLM-based because:
    (a) it requires aggregate data, not per-record understanding
    (b) it's faster and cheaper at scale
    (c) explainability matters for a business signal — "3x normal volume" is
        clearer to a sales team than "the model says so"

Trade-offs:
  - LLM calls add latency (~1-2s per job). We batch them and run async.
  - Cost: ~$0.001 per job at current claude-haiku-4-5 pricing → negligible for
    hundreds of jobs, worth monitoring at millions.
  - Seniority is sometimes ambiguous (e.g. "Growth Engineer") — we default to
    "mid" when uncertain rather than hallucinating.
  - Surge threshold (3× average) is configurable; tune per use case.
"""

import os
import json
import logging
import asyncio
from collections import Counter, defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SURGE_MULTIPLIER = float(os.getenv("SURGE_MULTIPLIER", "2.5"))  # flag if >2.5x avg


# ── Seniority classification ───────────────────────────────────────────────────

SENIORITY_LEVELS = ["junior", "mid", "senior", "lead", "exec"]

SENIORITY_SYSTEM_PROMPT = """You are a job classification engine. Given a job title and brief description, output exactly one JSON object with a single key "level" and value from: junior, mid, senior, lead, exec.

Rules:
- junior: 0-2 years expected, entry-level, associate, apprentice, trainee
- mid: 2-5 years, no prefix or "mid-level"
- senior: 5+ years, "senior", "sr", "staff", "principal" at IC level
- lead: people management implied, "lead", "manager", "head of", "director" (IC-adjacent)
- exec: VP, C-suite, "chief", "president", "founder"

When uncertain, output mid. Output ONLY valid JSON. No explanation."""


async def classify_seniority_batch(jobs: list[dict]) -> list[dict]:
    """
    Classify seniority for a batch of jobs using Claude.
    Uses asyncio.gather for concurrent API calls.
    Falls back gracefully if ANTHROPIC_API_KEY is not set.
    """
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — skipping AI seniority classification")
        for job in jobs:
            job["seniority_level"] = _heuristic_seniority(job["title"])
        return jobs

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

        async def classify_one(job: dict) -> dict:
            title = job.get("title", "")
            desc  = (job.get("description", "") or "")[:300]  # trim for cost
            prompt = f"Title: {title}\nDescription snippet: {desc}"
            try:
                response = await client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=20,
                    system=SENIORITY_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = response.content[0].text.strip()
                data = json.loads(text)
                level = data.get("level", "mid")
                if level not in SENIORITY_LEVELS:
                    level = "mid"
                job["seniority_level"] = level
            except Exception as e:
                logger.debug(f"AI classify failed for '{title}': {e}")
                job["seniority_level"] = _heuristic_seniority(title)
            return job

        # Concurrency cap: 10 at a time to respect rate limits
        semaphore = asyncio.Semaphore(10)

        async def classify_with_sem(job):
            async with semaphore:
                return await classify_one(job)

        results = await asyncio.gather(*[classify_with_sem(j) for j in jobs])
        logger.info(f"AI seniority classification complete for {len(results)} jobs")
        return list(results)

    except ImportError:
        logger.warning("anthropic package not installed — using heuristic fallback")
        for job in jobs:
            job["seniority_level"] = _heuristic_seniority(job["title"])
        return jobs


def _heuristic_seniority(title: str) -> str:
    """
    Regex fallback when AI is unavailable.
    Less accurate but always available.
    """
    import re
    t = title.lower()
    if re.search(r"\b(vp|vice president|chief|cto|ceo|coo|cfo|president)\b", t):
        return "exec"
    if re.search(r"\b(director|head of|lead|manager|engineering manager)\b", t):
        return "lead"
    if re.search(r"\b(senior|sr\.?|staff|principal|architect)\b", t):
        return "senior"
    if re.search(r"\b(junior|jr\.?|entry|associate|intern|apprentice|trainee)\b", t):
        return "junior"
    return "mid"


# ── Hiring surge detection ─────────────────────────────────────────────────────

def detect_surges(all_db_jobs: list, new_jobs: list[dict]) -> list[dict]:
    """
    Flag jobs from companies that are posting at surge levels.

    Algorithm:
    1. Count historical posts per company (all_db_jobs)
    2. Count new posts per company (new_jobs)
    3. Flag companies where new_count >= SURGE_MULTIPLIER * (historical_avg + 1)
       (+1 avoids division-by-zero and penalises brand-new companies fairly)

    Decision: we use a simple multiplier rather than z-score because:
    - Data volume is small (hundreds, not millions)
    - Business users understand "X posted 3x more than usual" intuitively
    - Z-score would require storing rolling stats — overkill for this scale
    """
    # Historical post counts per company
    historical: Counter = Counter()
    for job in all_db_jobs:
        company = getattr(job, "company", None) or job.get("company", "")
        if company:
            historical[company] += 1

    # New post counts per company
    new_counts: Counter = Counter()
    for job in new_jobs:
        company = job.get("company", "")
        if company:
            new_counts[company] += 1

    # Total companies seen historically
    total_companies = len(historical) or 1
    total_historical = sum(historical.values())
    global_avg_per_company = total_historical / total_companies

    surge_companies = set()
    for company, new_count in new_counts.items():
        hist_count = historical.get(company, 0)
        baseline = max(hist_count, global_avg_per_company) + 1
        if new_count >= SURGE_MULTIPLIER * baseline:
            surge_companies.add(company)
            logger.info(
                f"SURGE detected: {company} — {new_count} new posts "
                f"vs baseline {baseline:.1f} ({SURGE_MULTIPLIER}× threshold)"
            )

    for job in new_jobs:
        job["is_surge"] = job.get("company", "") in surge_companies

    return new_jobs
