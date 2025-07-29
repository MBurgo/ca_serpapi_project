"""
Summarise Canadian data into 5 journalist briefs and store it.
Reads only *CA‑suffixed* worksheets and appends the result to “Summaries CA”.
Compatible with openai>=1.0.0 (v1 client).
"""

# ── std / third‑party ──────────────────────────────────────────────────────────
import streamlit as st
import gspread
import pandas as pd
import time
import datetime as dt
import pytz
from google.oauth2.service_account import Credentials
from openai import OpenAI           # v1 client

TAB_SUFFIX = " CA"                  # must match engine suffix & tab names

# ── Google Sheets client ──────────────────────────────────────────────────────
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = st.secrets["service_account"]
creds      = Credentials.from_service_account_info(creds_dict, scopes=scope)
client_gs  = gspread.authorize(creds)

SPREADSHEET_ID = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet = client_gs.open_by_key(SPREADSHEET_ID)

# ── OpenAI client ─────────────────────────────────────────────────────────────
client_oa = OpenAI(api_key=st.secrets["openai"]["api_key"])

# ── helpers ───────────────────────────────────────────────────────────────────
def ensure_ws(title: str):
    """Get or create a worksheet by title."""
    try:
        return sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title, rows="100", cols="20")

def read_data(title: str) -> pd.DataFrame:
    """Return a DataFrame from the given worksheet (blank DF if empty)."""
    ws = ensure_ws(title)
    return pd.DataFrame(ws.get_all_records())

# ── prompt helpers ────────────────────────────────────────────────────────────
def format_data_for_prompt(news_df, top_df, rising_df, top_tr_df) -> str:
    """Flatten four dataframes into a readable text block for the prompt."""
    out = "Google News Data (Canada):\n"
    for _, r in news_df.iterrows():
        out += f"- Title: {r['Title']}, Link: {r['Link']}, Snippet: {r['Snippet']}\n"

    out += "\nTop Stories Data (Canada):\n"
    for _, r in top_df.iterrows():
        out += f"- Title: {r['Title']}, Link: {r['Link']}, Snippet: {r['Snippet']}\n"

    out += "\nGoogle Trends Rising:\n"
    for _, r in rising_df.iterrows():
        out += f"- Query: {r['Query']}, Value: {r['Value']}\n"

    out += "\nGoogle Trends Top:\n"
    for _, r in top_tr_df.iterrows():
        out += f"- Query: {r['Query']}, Value: {r['Value']}\n"

    return out

def summarize_data(full_prompt: str) -> str:
    """Call GPT‑4o via the v1 client and return the summary text."""
    resp = client_oa.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": full_prompt}],
    )
    return resp.choices[0].message.content

def store_summary(text: str):
    """Append the summary to the Summaries CA sheet."""
    ensure_ws(f"Summaries{TAB_SUFFIX}").append_row([text])
    time.sleep(1)  # gentle on quota

# ── main callable ─────────────────────────────────────────────────────────────
def generate_summary() -> str:
    # 1  load data
    news_df   = read_data(f"Google News{TAB_SUFFIX}")
    top_df    = read_data(f"Top Stories{TAB_SUFFIX}")
    rising_df = read_data(f"Google Trends Rising{TAB_SUFFIX}")
    top_tr_df = read_data(f"Google Trends Top{TAB_SUFFIX}")

    data_block = format_data_for_prompt(news_df, top_df, rising_df, top_tr_df)

    # 2  build instructions (AU‑style richness)
    local_tz = pytz.timezone("America/Toronto")
    today    = dt.datetime.now(local_tz).strftime("%Y-%m-%d")

    instructions = f"""
You are a seasoned financial news editor for a Canadian publisher focused on TSX‑listed stocks.

Your tasks:
1. Analyse **Google Trends Rising** – list the top 10 rising queries and flag any “Breakout”.
2. Analyse **Google Trends Top** – highlight consistently high‑volume queries.
3. Review **Google News CA** articles for recurring themes and notable entities.
4. Review **Top Stories** for “TSX Composite” for significant headlines.

**Formatting rules**
* plain text, single asterisks (*) for bold
* horizontal rules are lines of hyphens (-----)
* no Markdown headers (`###`)
* start major sections with an emoji for visual scanning

--------------------------------------------------
*Summary of Findings [{today}]*  
--------------------------------------------------
*Google Trends Insights*: top 10 rising queries (with volumes)  

*Key Trends & Recurring Themes*: top 5 themes (one line each)  

*Notable Entities*: companies, sectors, indexes  

--------------------------------------------------
*5 Detailed Briefs for Journalists*  
--------------------------------------------------

For **each** brief use the structure below:

--------------------------------------------------
*Brief Title*  
--------------------------------------------------
1. *Synopsis*  
2. *Key Themes*  
3. *Entities*  
4. *Source Insights*  
5. *Suggested Angles*  
"""

    full_prompt = f"{instructions}\n\nHere is the data to analyse:\n{data_block}"
    summary = summarize_data(full_prompt)
    store_summary(summary)
    return summary

# ── CLI entry ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    generate_summary()
