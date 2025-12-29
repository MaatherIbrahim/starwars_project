import requests
import pandas as pd
import json
import os

def load_homeworld_cache(cache_file):
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_homeworld_cache(cache, cache_file):
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def fetch_homeworld(homeworld_url, planet_cache, cache_file):
    if pd.isna(homeworld_url) or not homeworld_url:
        return {}

    if homeworld_url in planet_cache:
        return planet_cache[homeworld_url]

    response = requests.get(homeworld_url)
    response.raise_for_status()
    data = response.json()

    planet = data.get("result", {}).get("properties", data)

    planet_data = {
        "homeworld_name": planet.get("name"),
        "rotation_period": planet.get("rotation_period"),
        "orbital_period": planet.get("orbital_period"),
        "diameter": planet.get("diameter"),
        "climate": planet.get("climate"),
        "gravity": planet.get("gravity"),
        "terrain": planet.get("terrain"),
        "surface_water": planet.get("surface_water"),
        "population": planet.get("population"),
    }

    planet_cache[homeworld_url] = planet_data
    save_homeworld_cache(planet_cache, cache_file)

    return planet_data
