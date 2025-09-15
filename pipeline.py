from google_play_scraper import reviews, Sort
import requests
import pandas as pd
import uuid
import os
from datetime import datetime
from supabase import create_client, Client


# -------------------------------
# Supabase client setup
# -------------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------------
# Google Play Reviews (with pagination)
# -------------------------------
def fetch_playstore_reviews(app_id="com.application.zomato", max_reviews=500):
    all_reviews = []
    continuation_token = None

    while len(all_reviews) < max_reviews:
        result, continuation_token = reviews(
            app_id,
            lang="en",
            country="in",
            sort=Sort.NEWEST,
            count=200,
            continuation_token=continuation_token
        )
        all_reviews.extend(result)
        if not continuation_token:  # no more reviews available
            break

    df = pd.DataFrame([
        {
            "review_id": r.get("reviewId"),
            "rating": r.get("score"),
            "review_text": r.get("content"),
            "review_date": r.get("at")
        }
        for r in all_reviews
    ])
    return df


# -------------------------------
# iOS App Store Reviews (Zomato)
# -------------------------------
def fetch_appstore_reviews(app_id="434613896", pages=15):
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
                    "review_date": datetime.fromisoformat(
                        entry["updated"]["label"].replace("Z", "+00:00")
                    )
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
    print(f"✅ Uploaded {len(df)} new reviews into {table_name}")


# -------------------------------
# Filter only new reviews (based on latest stored date)
# -------------------------------
def filter_new_reviews(df, table_name, date_col="review_date"):
    if df.empty:
        return df
    latest = supabase.table(table_name).select(date_col).order(date_col, desc=True).limit(1).execute()
    if latest.data:
        last_date = pd.to_datetime(latest.data[0][date_col])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df[df[date_col] > last_date]
    return df


# -------------------------------
# Main Pipeline
# -------------------------------
if __name__ == "__main__":
    print("🚀 Starting Zomato review pipeline...")

    # Google Play
    df_play = fetch_playstore_reviews(max_reviews=500)
    print(f"Fetched {len(df_play)} Play Store reviews")
    df_play_new = filter_new_reviews(df_play, "zomato_playstore_reviews")
    print(f"New Play Store reviews: {len(df_play_new)}")
    upsert_reviews(df_play_new, "zomato_playstore_reviews")

    # iOS App Store
    df_ios = fetch_appstore_reviews(pages=15)
    print(f"Fetched {len(df_ios)} iOS App Store reviews")
    df_ios_new = filter_new_reviews(df_ios, "appstore_reviews")
    print(f"New iOS reviews: {len(df_ios_new)}")
    upsert_reviews(df_ios_new, "appstore_reviews")

    print("🎉 Pipeline finished successfully.")
