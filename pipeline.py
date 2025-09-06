from google_play_scraper import reviews
import requests
import pandas as pd
import uuid
import os
from datetime import datetime, timedelta
from supabase import create_client, Client


# -------------------------------
# Supabase client setup
# -------------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------------
# Google Play Reviews (Zomato)
# -------------------------------
def fetch_playstore_reviews(app_id="com.application.zomato", count=100):
    result, _ = reviews(
        app_id,
        lang="en",
        country="in",
        sort="newest",
        count=count
    )
    return pd.DataFrame([
        {
            "review_id": r.get("reviewId"),
            "rating": r.get("score"),
            "review_text": r.get("content"),
            "review_date": r.get("at")
        }
        for r in result
    ])


# -------------------------------
# iOS App Store Reviews (Zomato)
# -------------------------------
def fetch_appstore_reviews(app_id="434613896", pages=10):
    rows = []
    for page in range(1, pages + 1):
        url = f"https://itunes.apple.com/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
        res = requests.get(url)
        if res.status_code != 200:
            continue
        data = res.json()
        for entry in data.get("feed", {}).get("entry", []):
            if "im:rating" in entry:
                rows.append({
                    "review_id": str(uuid.uuid4()),  # iOS doesn’t always provide unique ID
                    "rating": int(entry["im:rating"]["label"]),
                    "review_text": entry["content"]["label"],
                    "review_date": datetime.fromisoformat(entry["updated"]["label"].replace("Z", "+00:00"))
                })
    return pd.DataFrame(rows)


# -------------------------------
# Insert into Supabase (with deduplication)
# -------------------------------
def upsert_reviews(df, table_name, conflict_column="review_id"):
    if df.empty:
        print(f"⚠️ No new reviews for {table_name}.")
        return
    df["review_date"] = df["review_date"].astype(str)
    for row in df.to_dict(orient="records"):
        supabase.table(table_name).upsert(row, on_conflict=conflict_column).execute()
    print(f"✅ Uploaded {len(df)} reviews into {table_name}")


# -------------------------------
# Filter reviews by "yesterday"
# -------------------------------
def filter_yesterday(df, date_col="review_date"):
    if df.empty:
        return df
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    df[date_col] = pd.to_datetime(df[date_col])
    return df[df[date_col].dt.date == yesterday]


# -------------------------------
# Main Pipeline
# -------------------------------
if __name__ == "__main__":
    print("🚀 Starting Zomato review pipeline...")

    # Google Play
    df_play = fetch_playstore_reviews()
    df_play_yesterday = filter_yesterday(df_play, "review_date")
    upsert_reviews(df_play_yesterday, "zomato_playstore_reviews")

    # iOS App Store
    df_ios = fetch_appstore_reviews(pages=15)
    df_ios_yesterday = filter_yesterday(df_ios, "review_date")
    upsert_reviews(df_ios_yesterday, "appstore_reviews")

    print("🎉 Pipeline finished.")
