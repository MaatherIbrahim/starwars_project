import pandas as pd
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler

from char_info import char_info
from new_char_info import new_char_info
from fetch_homeworld import fetch_homeworld, load_homeworld_cache
from load_to_db import load_data

LIMIT = 30
HOMEWORLD_CACHE_FILE = "homeworld_cache.json"
FINAL_CSV_FILE = "star_wars_characters_complete.csv"
FINAL_JSON_FILE = "star_wars_characters_complete.json"

def run_etl():
    print("ETL job started")

    df_old = pd.DataFrame(char_info(LIMIT))
    df_new = pd.DataFrame(new_char_info(LIMIT))

    merge_df = pd.merge(df_old, df_new, on="name", how="outer")

    planet_cache = load_homeworld_cache(HOMEWORLD_CACHE_FILE)

    planet_df = (
        merge_df["homeworld"]
        .apply(lambda url: fetch_homeworld(url, planet_cache, HOMEWORLD_CACHE_FILE))
        .apply(pd.Series)
    )

    final_df = pd.concat([merge_df, planet_df], axis=1)

    final_df.to_csv(FINAL_CSV_FILE, index=False)
    final_df.to_json(FINAL_JSON_FILE, orient="records", indent=2)

    load_data(final_df)
    print("ETL completed")

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_etl, "interval", minutes=1)
    scheduler.start()

if __name__ == "__main__":
    start_scheduler()
    uvicorn.run("api:app", host="0.0.0.0", port=8000)
