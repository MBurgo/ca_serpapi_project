"""
Summarise Canadian data into 5 journalist briefs and store it.
Writes to 'Summaries CA' and reads only CA‑suffixed tabs.
"""

import streamlit as st
import openai
import gspread
import pandas as pd
import time
import datetime as dt
import pytz
from google.oauth2.service_account import Credentials

TAB_SUFFIX = " CA"                    # must match engine suffix

# ---------- Google Sheets client ----------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = st.secrets["service_account"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet = client.open_by_key(SPREADSHEET_ID)

openai.api_key = st.secrets["openai"]["api_key"]

# ---------- helper ----------
def ensure_ws(title: str):
    try:
        return sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title, rows="100", cols="20")

# ---------- data pull ----------
def read_data(title):
    """Returns a pandas DF from the given CA worksheet."""
    ws = ensure_ws(title)
    return pd.DataFrame(ws.get_all_records())

# ---------- formatting ----------
def format_data_for_prompt(news_df, top_df, rising_df, top_tr_df):
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

# ---------- model ----------
def summarize_data(big_prompt):
    """Calls GPT‑4o to create the summary briefs."""
    response = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": big_prompt}],
    )
    return response["choices"][0]["message"]["content"]

# ---------- store summary ----------
def store_summary(summary_text):
    ws = ensure_ws(f"Summaries{TAB_SUFFIX}")
    ws.append_row([summary_text])
    time.sleep(1)

# ---------- main callable ----------
def generate_summary():
    news   = read_data(f"Google News{TAB_SUFFIX}")
    top    = read_data(f"Top Stories{TAB_SUFFIX}")
    rising = read_data(f"Google Trends Rising{TAB_SUFFIX}")
    top_tr = read_data(f"Google Trends Top{TAB_SUFFIX}")

    prompt_data = format_data_for_prompt(news, top, rising, top_tr)

    local_tz = pytz.timezone("America/Toronto")
    today = dt.datetime.now(local_tz).strftime("%Y-%m-%d")

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

    summary = summarize_data(f"{instructions}\n\nHere is the data:\n{prompt_data}")
    store_summary(summary)

    return summary

# ---------- CLI entry ----------
if __name__ == "__main__":
    generate_summary()
