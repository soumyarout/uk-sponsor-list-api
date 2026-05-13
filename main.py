import asyncio
import csv
import io
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Query, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="UK Sponsor List API", version="1.0.0")

GOV_PAGE_URL = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
REFRESH_INTERVAL_SECONDS = 3600  # re-check every hour

# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
store: dict = {
    "records": {},      # normalized_name -> {"name": str, "entries": [...]}
    "last_updated": None,
    "csv_url": None,
    "company_count": 0,
    "entry_count": 0,
}


def normalize(name: str) -> str:
    return name.lower().strip()


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

async def fetch_latest_csv_url(client: httpx.AsyncClient) -> Optional[str]:
    """Scrape the gov.uk page to find the current Worker CSV asset URL."""
    try:
        r = await client.get(GOV_PAGE_URL, follow_redirects=True, timeout=30)
        r.raise_for_status()
        pattern = (
            r'https://assets\.publishing\.service\.gov\.uk'
            r'/[^\s"\'<>]+Worker_and_Temporary_Worker[^\s"\'<>]*\.csv'
        )
        matches = re.findall(pattern, r.text)
        if matches:
            return matches[-1]
        logger.warning("No Worker CSV link found on gov.uk page")
    except Exception as exc:
        logger.error("Failed to fetch gov.uk page: %s", exc)
    return None


async def load_csv(url: str, client: httpx.AsyncClient) -> tuple[dict, int, int]:
    """Download and index the CSV. Returns (records, company_count, entry_count)."""
    r = await client.get(url, follow_redirects=True, timeout=60)
    r.raise_for_status()

    content = r.content.decode("utf-8-sig")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(content))

    records: dict = {}
    entry_count = 0

    for row in reader:
        name = row.get("Organisation Name", "").strip()
        if not name:
            continue

        key = normalize(name)
        if key not in records:
            records[key] = {
                "name": name,
                "entries": [],
            }

        records[key]["entries"].append({
            "town": row.get("Town/City", "").strip(),
            "county": row.get("County", "").strip(),
            "type_rating": row.get("Type & Rating", "").strip(),
            "route": row.get("Route", "").strip(),
        })
        entry_count += 1

    return records, len(records), entry_count


# ---------------------------------------------------------------------------
# Background refresh loop
# ---------------------------------------------------------------------------

async def refresh() -> None:
    async with httpx.AsyncClient(headers={"User-Agent": "uk-sponsor-api/1.0"}) as client:
        url = await fetch_latest_csv_url(client)
        if not url:
            if store["records"]:
                logger.info("Using cached data (could not reach gov.uk)")
            else:
                logger.warning("No data and could not fetch CSV URL")
            return

        if url == store["csv_url"] and store["records"]:
            logger.info("CSV URL unchanged — skipping reload")
            return

        logger.info("Loading CSV: %s", url)
        try:
            records, company_count, entry_count = await load_csv(url, client)
            store["records"] = records
            store["csv_url"] = url
            store["last_updated"] = datetime.now(timezone.utc).isoformat()
            store["company_count"] = company_count
            store["entry_count"] = entry_count
            logger.info("Loaded %d companies / %d entries", company_count, entry_count)
        except Exception as exc:
            logger.error("Failed to load CSV: %s", exc)


async def refresh_loop() -> None:
    while True:
        await refresh()
        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)


@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(refresh_loop())


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    return {
        "last_updated": store["last_updated"],
        "csv_url": store["csv_url"],
        "company_count": store["company_count"],
        "entry_count": store["entry_count"],
    }


@app.get("/check")
async def check(company: str = Query(..., description="Exact or close company name")):
    """
    O(1) exact-match lookup. Falls back to up to 5 partial matches.
    Returns sponsored=true/false/null.
    """
    if not store["records"]:
        raise HTTPException(503, detail="Sponsor list not yet loaded — try again shortly")

    key = normalize(company)

    # Exact match — fastest path
    if key in store["records"]:
        rec = store["records"][key]
        return {
            "sponsored": True,
            "name": rec["name"],
            "entries": rec["entries"],
        }

    # Partial / substring match fallback
    hits = [v for k, v in store["records"].items() if key in k]
    if hits:
        return {
            "sponsored": None,
            "message": "No exact match. Possible matches below.",
            "suggestions": [h["name"] for h in hits[:5]],
        }

    return {
        "sponsored": False,
        "name": company,
        "message": "Not found in UK licensed sponsor register",
    }


@app.get("/search")
async def search(q: str = Query(..., min_length=2, description="Partial company name")):
    """Substring search across all company names (case-insensitive)."""
    if not store["records"]:
        raise HTTPException(503, detail="Sponsor list not yet loaded — try again shortly")

    q_norm = normalize(q)
    hits = [v for k, v in store["records"].items() if q_norm in k]
    return {
        "query": q,
        "total": len(hits),
        "results": [{"name": h["name"], "entries": h["entries"]} for h in hits[:20]],
    }
