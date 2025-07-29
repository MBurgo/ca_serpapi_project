"""
Summarise Canadian data into 5 journalist briefs and store it.
Reads only the *CA‑suffixed* worksheets and appends the summary to
“Summaries CA”.
Compatible with openai>=1.0.0 (v1 client).
"""

import streamlit as st
import gspread
import pandas as pd
import time
import datetime as dt
import pytz
from google.oauth2.service_account import Credentials
from openai import OpenAI   # ← NEW: v1 client import

TAB_SUFFIX = " CA"          # must match engine suffix

# ---------- Google Sheets client ----------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = st.secrets["service_account"]
creds      = Credentials.from_service_account_info(creds_dict, scopes=scope)
client_gs  = gspread.authorize(creds)

SPREADSHEET_ID = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet = client_gs.open_by_key(SPREADSHEET_ID)

# ---------- OpenAI client ----------
client_oa = OpenAI(api_key=st.secrets["openai"]["api_key"])

# ---------- helpers ----------
def ensure_ws(title: str):
    """Return the worksheet, creating it if needed."""
    try:
        return sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title, rows="100", cols="20")

def read_data(title: str) -> pd.DataFrame:
    """Read a worksheet into a DataFrame (returns empty DF if sheet is blank)."""
    ws = ensure_ws(title)
    records = ws.get_all_records()
    return pd.DataFrame(records)

# ---------- prompt formatting ----------
def format_data_for_prompt(news_df, top_df, rising_df, top_tr_df) -> str:
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

# ---------- OpenAI call ----------
def summarize_data(big_prompt: str) -> str:
    """Generate the briefing summary with GPT‑4o via the new v1 client."""
    response = client_oa.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": big_prompt}],
    )
    return response.choices[0].message.content

# ---------- store summary ----------
def store_summary(summary_text: str):
    ws = ensure_ws(f"Summaries{TAB_SUFFIX}")
    ws.append_row([summary_text])
    time.sleep(1)   # small pause to avoid Sheets quota bursts

# ---------- main callable ----------
def generate_summary() -> str:
    news_df   = read_data(f"Google News{TAB_SUFFIX}")
    top_df    = read_data(f"Top Stories{TAB_SUFFIX}")
    rising_df = read_data(f"Google Trends Rising{TAB_SUFFIX}")
    top_tr_df = read_data(f"Google Trends Top{TAB_SUFFIX}")

    prompt_data = format_data_for_prompt(news_df, top_df, rising_df, top_tr_df)

    local_tz = pytz.timezone("America/Toronto")
    today    = dt.datetime.now(local_tz).strftime("%Y-%m-%d")

    instructions = (
        "You are a seasoned financial news editor for a Canadian publisher focused on TSX‑listed stocks. "
        "Identify key trends and draft 5 detailed briefs for journalists.\n\n"
        "--------------------------------------------------\n"
        f"*Summary of Findings [{today}]*\n"
        "--------------------------------------------------\n"
        "*Google Trends Insights*: List the top 10 rising queries.\n\n"
        "*Key Trends & Recurring Themes*: Top 5 themes.\n\n"
        "*Notable Entities*: Companies, sectors, indexes.\n\n"
        "--------------------------------------------------\n"
        "*5 Detailed Briefs for Journalists*\n"
        "--------------------------------------------------\n"
        "For each brief include Synopsis, Key Themes, Entities, Source Insights, Suggested Angles.\n"
        "Use single asterisks for bold and separator lines of hyphens. Add emojis to section headers."
    )

    summary_text = summarize_data(f"{instructions}\n\nHere is the data:\n{prompt_data}")
    store_summary(summary_text)
    return summary_text

# ---------- CLI entry ----------
if __name__ == "__main__":
    generate_summary()
