"""
data_retrieval_storage_news_engine_ca.py
----------------------------------------
Canada‑only fork of AU v1.2  (31 Jul 2025)

• Uses TSX Composite queries & CA geo settings
• Writes to CA‑suffixed worksheets so AU data is never touched
"""

# ---------- stdlib ----------
import asyncio
import datetime as dt
import time
from typing import List

# ---------- third‑party ----------
import gspread
import httpx
from bs4 import BeautifulSoup
import streamlit as st
from google.oauth2.service_account import Credentials
from pytrends.exceptions import TooManyRequestsError
from serpapi import GoogleSearch

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
CAP_NEWS        = 40
CAP_TOP_STORIES = 40
CAP_TRENDS      = 20
DEBUG_COUNTS    = False

TAB_SUFFIX = " CA"                       # <— single leading space then “CA”

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
}

# ---------------------------------------------------------------------
# 1  Google Sheets client & SerpAPI key
# ---------------------------------------------------------------------
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds_dict = st.secrets["service_account"]
creds      = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
client     = gspread.authorize(creds)

# 👉 Replace with your shared workbook ID (or keep the AU ID if you prefer)
SPREADSHEET_ID = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet          = client.open_by_key(SPREADSHEET_ID)

SERP_API_KEY = st.secrets["serpapi"]["api_key"]

# ---------------------------------------------------------------------
# 2  SerpAPI fetch helpers – Canada settings
# ---------------------------------------------------------------------
def fetch_google_news() -> List[dict]:
    params = {
        "api_key": SERP_API_KEY,
        "engine": "google",
        "no_cache": "true",
        "q": "tsx today",
        "google_domain": "google.ca",
        "tbs": "qdr:d",
        "gl": "ca",
        "hl": "en",
        "location": "Canada",
        "tbm": "nws",
        "num": "40",
    }
    return GoogleSearch(params).get_dict().get("news_results", [])


def fetch_google_top_stories() -> List[dict]:
    params = {
        "api_key": SERP_API_KEY,
        "q": "tsx+today",
        "hl": "en",
        "gl": "ca",
    }
    return GoogleSearch(params).get_dict().get("top_stories", [])


def fetch_google_trends():
    params = {
        "api_key": SERP_API_KEY,
        "engine": "google_trends",
        "q": "/m/09qwc",          # S&P/TSX Composite Index topic‑ID
        "geo": "CA",
        "data_type": "RELATED_QUERIES",
        "tz": "-300",             # Eastern Std Time (UTC‑5). Use -240 for EDT.
        "date": "now 4-H",
    }

    attempts = 0
    while attempts < 5:
        try:
            results = GoogleSearch(params).get_dict()
            rel = results.get("related_queries", {})
            return rel.get("rising", []), rel.get("top", [])
        except TooManyRequestsError:
            wait = (2 ** attempts) * 10
            print(f"Google Trends rate‑limited – sleeping {wait}s")
            time.sleep(wait)
            attempts += 1
    raise RuntimeError("Google Trends fetch failed after multiple attempts.")

# ---------------------------------------------------------------------
# 3  Worksheet helpers
# ---------------------------------------------------------------------
def ensure_worksheet_exists(sheet_obj, title: str):
    try:
        return sheet_obj.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sheet_obj.add_worksheet(title=title, rows="100", cols="20")


def overwrite_worksheet(ws, header: List[str], rows: List[List]):
    ws.resize(rows=len(rows) + 1, cols=len(header))
    ws.update(
        range_name="A1",
        values=[header] + rows,
        value_input_option="USER_ENTERED",
    )

# ---------------------------------------------------------------------
# 4  Dedup helper
# ---------------------------------------------------------------------
def dedupe_rows(rows: List[List], key_index: int, keep_n: int) -> List[List]:
    seen, out = set(), []
    for row in rows:
        key = row[key_index]
        if key not in seen:
            seen.add(key)
            out.append(row)
        if len(out) >= keep_n:
            break
    return out

# ---------------------------------------------------------------------
# 5  Async meta‑description fetch (unchanged)
# ---------------------------------------------------------------------
async def _grab_desc(session: httpx.AsyncClient, url: str) -> str:
    if not url or not url.startswith("http"):
        return "Invalid URL"
    try:
        r = await session.get(url, timeout=10, headers=BROWSER_HEADERS)
        if r.status_code != 200:
            return f"HTTP {r.status_code}"
        soup = BeautifulSoup(r.content, "lxml")
        tag  = soup.find("meta", attrs={"name": "description"})
        return (
            tag["content"].strip()
            if tag and "content" in tag.attrs and tag["content"].strip()
            else "No Meta Description"
        )
    except Exception:
        return "Error Fetching Description"

async def fetch_meta_descriptions(urls: List[str], limit: int = 10) -> List[str]:
    sem = asyncio.Semaphore(limit)
    async with httpx.AsyncClient(follow_redirects=True) as session:
        async def bound(u):
            async with sem:
                return await _grab_desc(session, u)
        return await asyncio.gather(*(bound(u) for u in urls))

# ---------------------------------------------------------------------
# 6  Storage orchestrator – writes to CA tabs
# ---------------------------------------------------------------------
def store_data_in_google_sheets(news_data, top_stories_data, rising_data, top_data):

    # ---------- Google News ----------
    news_rows = [
        [a.get("title") or "No Title",
         a.get("link")  or "No Link",
         a.get("snippet") or "No Snippet"]
        for a in news_data
    ]
    news_rows = dedupe_rows(news_rows, key_index=1, keep_n=CAP_NEWS)
    snippet_lookup_news = {r[1]: r[2] for r in news_rows}

    news_meta = asyncio.run(fetch_meta_descriptions([r[1] for r in news_rows]))
    for row, meta in zip(news_rows, news_meta):
        row.append(meta if meta else "No Meta Description")
        if meta.startswith(("HTTP", "Error")):
            row[-1] = snippet_lookup_news.get(row[1], "No Meta Description")

    overwrite_worksheet(
        ensure_worksheet_exists(sheet, f"Google News{TAB_SUFFIX}"),
        ["Title", "Link", "Snippet", "Meta Description"],
        news_rows,
    )

    # ---------- Top Stories ----------
    top_rows = [
        [s.get("title") or "No Title",
         s.get("link")  or "No Link",
         s.get("snippet") or "No Snippet"]
        for s in top_stories_data
    ]
    top_rows = dedupe_rows(top_rows, key_index=1, keep_n=CAP_TOP_STORIES)
    snippet_lookup_top = {r[1]: r[2] for r in top_rows}

    top_meta = asyncio.run(fetch_meta_descriptions([r[1] for r in top_rows]))
    for row, meta in zip(top_rows, top_meta):
        row.append(meta if meta else "No Meta Description")
        if meta.startswith(("HTTP", "Error")):
            row[-1] = snippet_lookup_top.get(row[1], "No Meta Description")

    overwrite_worksheet(
        ensure_worksheet_exists(sheet, f"Top Stories{TAB_SUFFIX}"),
        ["Title", "Link", "Snippet", "Meta Description"],
        top_rows,
    )

    # ---------- Google Trends Rising ----------
    rising_rows = [[q.get("query"), q.get("value")] for q in rising_data][:CAP_TRENDS]
    overwrite_worksheet(
        ensure_worksheet_exists(sheet, f"Google Trends Rising{TAB_SUFFIX}"),
        ["Query", "Value"],
        rising_rows,
    )

    # ---------- Google Trends Top ----------
    top_rows_q = [[q.get("query"), q.get("value")] for q in top_data][:CAP_TRENDS]
    overwrite_worksheet(
        ensure_worksheet_exists(sheet, f"Google Trends Top{TAB_SUFFIX}"),
        ["Query", "Value"],
        top_rows_q,
    )

# ---------------------------------------------------------------------
# 7  Main entry point
# ---------------------------------------------------------------------
def main():
    now_utc = dt.datetime.now(dt.UTC)
    print(f"=== CA scrape started {now_utc:%Y-%m-%d %H:%M:%S}Z ===")

    news = fetch_google_news()
    tops = fetch_google_top_stories()
    rising, top = fetch_google_trends()

    store_data_in_google_sheets(news, tops, rising, top)
    print("=== CA scrape finished ===")


if __name__ == "__main__":
    main()
