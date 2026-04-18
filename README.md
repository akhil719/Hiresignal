# HireSignal — B2B Hiring Intelligence Pipeline

> A fully automated data pipeline that scrapes, cleans, enriches, and serves real-time hiring signals — built for B2B use cases like sales prospecting, talent platforms, and market intelligence.

![Python](https://img.shields.io/badge/Python-3.11+-blue) ![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## The Problem Being Solved

B2B companies — recruiters, HR-tech platforms, sales teams, investors — need to know **who is hiring, for what, and at what velocity**. This is a proxy for:

- Company growth and budget availability (for sales teams)
- Talent supply/demand shifts (for HR platforms)
- Market expansion signals (for investors and analysts)

This data is expensive to license and hard to keep fresh. **HireSignal** automates the full pipeline: scrape → clean → enrich → serve, with zero manual intervention after setup.

---

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
│   Scraper    │────▶│   Cleaner    │────▶│   AI Enrichment      │────▶│   Database   │
│  (RemoteOK)  │     │ (normalise,  │     │ (seniority classify  │     │  (SQLite /   │
│  httpx +     │     │  dedup,      │     │  + surge detection)  │     │  PostgreSQL) │
│  retry logic │     │  document)   │     │  Anthropic API       │     │  SQLAlchemy  │
└──────────────┘     └──────────────┘     └──────────────────────┘     └──────────────┘
                                                                                │
                              ┌─────────────────────────────────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │   FastAPI App    │
                    │  /api/jobs       │
                    │  /api/companies  │
                    │  /api/trends     │
                    │  /api/stats      │
                    │  /api/trigger    │
                    │  / (dashboard)   │
                    └──────────────────┘
                              │
                    ┌──────────────────┐
                    │  APScheduler     │
                    │  Runs every 6h   │
                    │  (no cron needed)│
                    └──────────────────┘
```

---

## Features

| Feature | Details |
|---|---|
| **Scraper** | RemoteOK public JSON API · pagination-aware · exponential backoff (3 retries) · all fields treated as optional |
| **Cleaner** | Title normalisation · company deduplication · tag aliasing · salary parsing · HTML stripping · every decision documented |
| **Database** | SQLAlchemy ORM · SQLite locally · swap to PostgreSQL via `DATABASE_URL` · full audit log of every run |
| **Scheduler** | APScheduler background job · configurable interval · misfire-safe · no cron or external queue needed |
| **API** | FastAPI · paginated jobs endpoint · company rankings · trends · pipeline stats · manual trigger |
| **Dashboard** | Live HTML dashboard · filterable table · skill cloud · seniority chart · surge indicators |
| **AI Layer** | Claude (haiku) seniority classification · rule-based hiring surge detection · graceful fallback if API key absent |

---

## Project Structure

```
hiring-intel/
├── main.py                  # Entry point — uvicorn app
├── pipeline.py              # Orchestrates full ETL run
├── requirements.txt
├── render.yaml              # One-click Render deploy config
├── .env.example
│
├── scraper/
│   ├── __init__.py
│   └── remoteok.py          # Scraper: fetch, parse, retry logic
│
├── cleaner/
│   ├── __init__.py
│   └── clean.py             # Cleaner: all decisions documented inline
│
├── db/
│   ├── __init__.py
│   └── models.py            # SQLAlchemy models + session
│
├── ai/
│   ├── __init__.py
│   └── enricher.py          # Seniority classify + surge detection
│
├── scheduler/
│   ├── __init__.py
│   └── jobs.py              # APScheduler setup
│
├── api/
│   ├── __init__.py
│   └── main.py              # FastAPI routes + lifespan
│
└── templates/
    └── dashboard.html       # Live dashboard UI
```

---

## Running Locally

### 1. Clone & install

```bash
git clone https://github.com/your-username/hiring-intel.git
cd hiring-intel
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
cp .env.example .env
# Edit .env — only ANTHROPIC_API_KEY is needed for the AI bonus layer
# Everything else works with defaults
```

### 3. Run

```bash
python main.py
```

The app will:
1. Create the SQLite database (`hiring_intel.db`) automatically
2. Trigger an **initial scrape on first run** (DB is empty)
3. Start the background scheduler (runs every 6 hours)
4. Serve the dashboard at **http://localhost:8000**

### 4. API docs

Interactive Swagger UI at **http://localhost:8000/docs**

---

## One-Command Setup (Docker)

```bash
docker build -t hiring-intel .
docker run -p 8000:8000 -e ANTHROPIC_API_KEY=your_key hiring-intel
```

---

## API Reference

### `GET /api/jobs`
Paginated job listings with filters.

| Param | Type | Description |
|---|---|---|
| `page` | int | Page number (default 1) |
| `page_size` | int | Results per page (max 100, default 20) |
| `company` | string | Filter by company name (partial match) |
| `tag` | string | Filter by skill/tag |
| `seniority` | string | junior / mid / senior / lead / exec |
| `days` | int | Only jobs scraped in last N days |
| `surge_only` | bool | Only show surge-flagged jobs |

### `GET /api/companies`
Top companies by job volume. Params: `limit`, `days`

### `GET /api/trends`
Hiring velocity, top skills, seniority split, surge companies. Params: `days`

### `GET /api/stats`
Pipeline health: total jobs, last run time, status.

### `POST /api/trigger`
Manually trigger a pipeline run (runs in background, returns immediately).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | _(none)_ | Enables AI seniority classification. Without it, heuristic fallback is used. |
| `DATABASE_URL` | `sqlite:///./hiring_intel.db` | SQLAlchemy connection string. Swap to `postgresql://...` for production. |
| `SCRAPE_INTERVAL_HOURS` | `6` | How often the pipeline auto-runs. |
| `SURGE_MULTIPLIER` | `2.5` | Flag a company as surging if new postings ≥ this × their baseline. |
| `PORT` | `8000` | HTTP port. |
| `ENV` | `production` | Set to `development` to enable uvicorn auto-reload. |

---

## Deploying to Render (Free Tier)

1. Push your repo to GitHub
2. Go to [render.com](https://render.com) → New Web Service → connect your repo
3. Render auto-detects `render.yaml` — no manual config needed
4. Add `ANTHROPIC_API_KEY` in the Render environment variables dashboard
5. Deploy

> **Note on SQLite in production:** Render's free tier has an ephemeral filesystem — the SQLite DB resets on redeploy. For persistent storage, set `DATABASE_URL` to a PostgreSQL connection string (Render offers a free Postgres instance).

---

## Data Cleaning Decisions

All decisions are documented inline in `cleaner/clean.py`. Key choices:

| Decision | Rationale |
|---|---|
| Expand title abbreviations (`sr.` → `Senior`) | Enables consistent filtering without maintaining exhaustive keyword lists |
| Strip legal suffixes from company names (`Inc.`, `LLC`) | Groups "Stripe, Inc." and "Stripe" as the same company |
| Standardise remote keywords → `"Remote"` | Source is a remote-jobs board; "Worldwide", "Anywhere" all mean the same thing |
| Tag aliasing (`js` → `javascript`) | Prevents fragmented skill counts across equivalent tags |
| Keep `salary_raw` alongside parsed values | Audit trail — never silently lose original data |
| Cap at 20 tags per job | Reduces noise; most value is in the first 10-15 tags |
| Reject future dates | Clock skew or data errors; avoids polluting "latest jobs" queries |
| HTML strip on descriptions | RemoteOK returns raw HTML; we store plain text |

---

## AI/ML Bonus Layer

### Seniority Classification
**Approach:** LLM-based (Claude claude-haiku-4-5) via the Anthropic API.

**Why LLM over regex?** Job titles are semantically inconsistent across companies. `"Staff Engineer"`, `"L5"`, `"Principal"`, `"Senior II"`, and `"IC4"` all mean senior-level — a regex list would need constant maintenance and still miss edge cases. A language model handles the full semantic space zero-shot.

**Trade-offs:**
- Adds ~1-2s latency per job (mitigated by async batching with concurrency cap)
- Small API cost (~$0.001/job with claude-haiku-4-5) — negligible at this scale
- Gracefully degrades to heuristic regex if API key is absent

### Hiring Surge Detection
**Approach:** Rule-based statistical comparison (not LLM).

**Why not LLM here?** Surge detection requires aggregate data across companies, not per-record understanding. A rule — "flag companies posting ≥ 2.5× their historical average" — is faster, cheaper, fully explainable, and more trustworthy for a business signal. Sales teams need to explain why a company is flagged; "the model says so" is not acceptable.

**Algorithm:** Compare new post count per company against their rolling historical baseline. Configurable multiplier via `SURGE_MULTIPLIER` env var.

---

## License

MIT
