import streamlit as st
import openai
import gspread
import pandas as pd
import time
import datetime as dt
import pytz
from google.oauth2.service_account import Credentials

# Define the scope for Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load the service account info from Streamlit secrets
creds_dict = st.secrets["service_account"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

# Spreadsheet ID remains the same (update if needed)
spreadsheet_id = "1BzTJgX7OgaA0QNfzKs5AgAx2rvZZjDdorgAz0SD9NZg"
sheet = client.open_by_key(spreadsheet_id)

# Set your OpenAI API key from Streamlit secrets
openai.api_key = st.secrets["openai"]["api_key"]


def read_data(sheet, title):
    """Reads data from a specified Google Sheet worksheet into a pandas DataFrame."""
    worksheet = sheet.worksheet(title)
    data = worksheet.get_all_records()
    return pd.DataFrame(data)


def format_data_for_prompt(news_data, top_stories_data, rising_data, top_data):
    """
    Formats data from four different sources (news, top stories, trends rising, trends top)
    into a single string for the prompt.
    """
    formatted_data = "Google News Data:\n"
    for index, row in news_data.iterrows():
        formatted_data += f"- Title: {row['Title']}, Link: {row['Link']}, Snippet: {row['Snippet']}\n"

    formatted_data += "\nTop Stories Data:\n"
    for index, row in top_stories_data.iterrows():
        formatted_data += f"- Title: {row['Title']}, Link: {row['Link']}, Snippet: {row['Snippet']}\n"

    formatted_data += "\nGoogle Trends Rising Data:\n"
    for index, row in rising_data.iterrows():
        formatted_data += f"- Query: {row['Query']}, Value: {row['Value']}\n"

    formatted_data += "\nGoogle Trends Top Data:\n"
    for index, row in top_data.iterrows():
        formatted_data += f"- Query: {row['Query']}, Value: {row['Value']}\n"

    return formatted_data


def summarize_data(formatted_data):
    """
    Summarize data using the o1-mini model with a single 'user' role message.
    We combine system-like instructions and user instructions into a single prompt.
    """
    # >>> CHANGE: Use local time for the date in the summary <<<
    local_tz = pytz.timezone("Australia/Sydney")  # or any other desired timezone
    now_local = dt.datetime.now(local_tz)
    current_date = now_local.strftime("%Y-%m-%d")

    # System-like context for the prompt
    system_like_context = (
        "You are a seasoned financial news editor for an Australian financial news publisher. "
        "Your responsibilities include analyzing financial data and news sources to identify "
        "key trends, notable events, and opportunities for in-depth reporting. "
        "You provide insightful summaries and detailed briefs to help financial journalists "
        "craft stories that inform and engage retail investors and industry professionals. "
        "Your communication style is clear, concise, and analytical, with a focus on accuracy "
        "and relevance. You use industry-specific terminology appropriately and maintain an "
        "objective tone.\n\n"
    )

    # Instructions for the summarization
    instructions = (
        f"As a news editor for an Australian financial news publisher, your task is to analyze "
        f"and summarize the latest data from various sources related to the Australian stock market. "
        f"Your goal is to identify key trends, recurring themes, and interesting opportunities for "
        f"our financial journalists to cover.\n\n"
        f"Using the provided data, please perform the following tasks:\n"
        f"1. Analyze the \"Google Trends Rising\" data to identify the top 10 rising search queries, "
        f"paying special attention to high-volume queries and those marked as 'Breakout'.\n"
        f"2. Analyze the \"Google Trends Top\" data to identify the top search queries.\n"
        f"3. Review the articles from \"Google News\" to identify recurring themes and notable entities.\n"
        f"4. Review the articles from \"Top Stories\" for the query \"ASX 200\" to identify significant news stories.\n\n"
        f"Please include the following sections in your report using plain text with single asterisks (*) for bold text. "
        f"Use lines of hyphens (\"-\" repeated) to create horizontal lines as separators before and after major sections "
        f"and brief titles. Do not use Markdown headers or `###`.\n\n"
        f"Include the date of summarization ({current_date}) in your report.\n\n"
        f"The report should have the following structure:\n\n"
        f"--------------------------------------------------\n"
        f"*Summary of Findings [{current_date}]*\n"
        f"--------------------------------------------------\n"
        f"*Google Trends Insights*: List the top 10 trends from the \"Google Trends Rising\" data, along with their volumes.\n\n"
        f"*Key Trends & Recurring Themes*: Identify the top 5 trends with brief descriptions and their volumes.\n\n"
        f"*Notable Entities*: List key companies, institutions, and market insights discussed in the data.\n\n"
        f"--------------------------------------------------\n"
        f"*5 Detailed Briefs for Journalists*\n"
        f"--------------------------------------------------\n\n"
        f"For each brief, use the following structure, separated by horizontal lines:\n\n"
        f"--------------------------------------------------\n"
        f"*Brief Title*\n"
        f"--------------------------------------------------\n"
        f"1. *Synopsis*: Brief summary of the findings.\n"
        f"2. *Key Themes*: Main themes identified in the data.\n"
        f"3. *Entities*: Relevant companies, indexes, or key individuals.\n"
        f"4. *Source Insights*: Data sources these insights come from.\n"
        f"5. *Suggested Angles*: Recommended angles for journalists to pursue.\n\n"
        f"Include emojis at the beginning of important sections to visually highlight them. "
        f"Do not use Markdown headers or `###`.\n"
    )

    # Combine context, instructions, and data into one user prompt
    big_prompt = (
        f"{system_like_context}"
        f"{instructions}\n"
        f"Here is the data to analyze:\n\n"
        f"{formatted_data}"
    )

    messages = [
        {
            "role": "user",
            "content": big_prompt
        }
    ]

    # Call the OpenAI API
    response = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=messages
    )
    summary = response['choices'][0]['message']['content']
    return summary


def store_summary_in_google_sheets(sheet, summary):
    """Stores the summary data in a 'Summaries' worksheet, appending a row."""
    summary_sheet = sheet.worksheet("Summaries")
    summary_sheet.append_row([summary])
    time.sleep(1)  # Delay to prevent exceeding quota


def generate_summary():
    """
    Pulls data from Google Sheets, summarizes using the AI model,
    stores the summary in the 'Summaries' worksheet, and returns it.
    """
    # Read data from relevant worksheets
    news_data = read_data(sheet, "Google News")
    top_stories_data = read_data(sheet, "Top Stories")
    rising_data = read_data(sheet, "Google Trends Rising")
    top_data = read_data(sheet, "Google Trends Top")

    # Format all data into a single string
    formatted_data = format_data_for_prompt(news_data, top_stories_data, rising_data, top_data)

    # Generate summary via OpenAI
    summary = summarize_data(formatted_data)

    # Store the summary in "Summaries" worksheet
    store_summary_in_google_sheets(sheet, summary)

    # Return the summary text so we can display it in Streamlit
    return summary


def main():
    """
    Original main function to allow command-line usage.
    It just calls generate_summary() but doesn't return anything.
    """
    generate_summary()


if __name__ == "__main__":
    main()
