import os
import json
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine
from char_info import char_info
from new_char_info import new_char_info
from fetch_homeworld import (
    fetch_homeworld,
    load_homeworld_cache
)

from load_to_db import load_data
import subprocess
import sys
import uvicorn
LIMIT = 30

OLD_CHAR_FILE = "old_republic_characters.json"
NEW_CHAR_FILE = "new_republic_characters.json"
HOMEWORLD_CACHE_FILE = "homeworld_cache.json"
FINAL_CSV_FILE = "star_wars_characters_complete.csv"
FINAL_JSON_FILE = "star_wars_characters_complete.json"


def run_etl():
    print("ETL job started")

    # Load character data
    old_info = char_info(LIMIT)
    new_info = new_char_info(LIMIT)

    #  Convert to DataFrames
    df_old = pd.DataFrame(old_info)
    df_new = pd.DataFrame(new_info)

    #  Merge characters
    merge_df = pd.merge(df_old, df_new, on="name", how="outer")

    #  Load homeworld cache
    planet_cache = load_homeworld_cache(HOMEWORLD_CACHE_FILE)

    # Fetch & enrich homeworld data
    planet_df = (
        merge_df["homeworld"]
        .apply(lambda url: fetch_homeworld(url, planet_cache, HOMEWORLD_CACHE_FILE))
        .apply(pd.Series)
    )

    final_df = pd.concat([merge_df, planet_df], axis=1)

    #  Save outputs
    final_df.to_csv(FINAL_CSV_FILE, index=False)
    final_df.to_json(FINAL_JSON_FILE, orient="records", indent=2)

    print("Pipeline completed successfully ")
    load_data(final_df)

def start_scheduler():
    scheduler = BlockingScheduler()

    scheduler.add_job(
        run_etl,
        trigger="interval",
        minutes=1,              # run every minute
        id="starwars_etl_job",
        replace_existing=True
    )

    print("Scheduler started. ETL will run every 1 minute.")
    scheduler.start()

def run_api():
    print("Starting FastAPI server...")
    subprocess.Popen([
        sys.executable,
        "-m",
        "uvicorn",
        "api:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000"
    ])
    
if __name__ == "__main__":
    start_scheduler()  
    run_api()







