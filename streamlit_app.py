# ------------------------------------------------------------
# Foolish Financial Briefings – Canada edition (Streamlit UI)
# ------------------------------------------------------------
import streamlit as st
import datetime as dt
import pytz
import gspread
from google.oauth2.service_account import Credentials

TAB_SUFFIX = " CA"

# 1) PAGE CONFIG & BRAND CSS  --------------------------------
st.set_page_config(
    page_title="Burgo's Briefing App – Canada",
    page_icon="🍁",
    layout="centered",
)

# --- paste the same BRAND_CSS block you use in AU ---
# (omitted for brevity)

# 2) GOOGLE SHEETS CLIENT ------------------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = st.secrets["service_account"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet = client.open_by_key(SPREADSHEET_ID)

# 3) IMPORT JOB SCRIPTS --------------------------------------
from data_retrieval_storage_news_engine_ca import main as retrieve_and_store_data
from step2_summarisation_with_easier_reading_ca import generate_summary

# 4) WORKSHEET UTILS -----------------------------------------
def ensure_ws(title):
    try:
        return sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title, rows="100", cols="20")

# metadata sheet is CA‑specific
def get_last_run_info():
    ws = ensure_ws(f"Metadata{TAB_SUFFIX}")
    last_time = ws.cell(2, 1).value
    last_sum  = ws.cell(2, 2).value
    if last_time:
        dt_naive = dt.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
        return dt_naive.replace(tzinfo=dt.UTC), last_sum
    return None, None

def set_last_run_info(summary_text):
    ws = ensure_ws(f"Metadata{TAB_SUFFIX}")
    now_utc = dt.datetime.now(dt.UTC)
    ws.update_cell(2, 1, now_utc.strftime("%Y-%m-%d %H:%M:%S"))
    ws.update_cell(2, 2, summary_text)

def format_utc_as_local(utc_dt):
    if utc_dt is None:
        return "No previous run recorded"
    local = pytz.timezone("America/Toronto")
    return utc_dt.astimezone(local).strftime("%Y-%m-%d %H:%M:%S")

# 5) COOLDOWN LOGIC ------------------------------------------
def run_all(cooldown_hours=3):
    now = dt.datetime.now(dt.UTC)
    last_run, last_summary = get_last_run_info()

    elapsed = 9999 if last_run is None else (now - last_run).total_seconds() / 3600
    if elapsed < cooldown_hours:
        st.write(f"**Last run:** {format_utc_as_local(last_run)} local")
        st.write(f"Please wait **{cooldown_hours - elapsed:.1f} h** before running again.")
        return last_summary

    st.write("Step 1 / 2 – Fetching data …")
    retrieve_and_store_data()

    st.write("Step 2 / 2 – Generating summary …")
    summary = generate_summary()

    set_last_run_info(summary)
    return summary

# 6) MAIN APP -------------------------------------------------
def main():
    st.title("Foolish Financial Briefings – Canada")

    st.markdown(
        """
        ℹ️ **About This Tool**

        • Scrapes Canadian Google News, Top Stories, and Google Trends every run  
        • Produces 5 journalist briefs via GPT‑4o  
        • Cooldown: one run every 3 hours
        """
    )

    st.write("Press the button to get your fresh Canadian briefs!")

    if st.button("Get Canadian Briefs"):
        summary_text = run_all()
        st.success("Done!")
        st.subheader("AI‑Generated Summary")
        st.write(summary_text)

if __name__ == "__main__":
    main()
