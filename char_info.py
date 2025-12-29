import pandas as pd
from config import Old_Republic_DB
import requests

def char_info(limit):
    characters = []
    url = Old_Republic_DB
    while url and len(characters) < limit:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = data["results"][:limit]

        for item in results:
            if len(characters) >= limit:
                break
            character_dict = {
                "uid": item["uid"],
                "name": item["name"],
                "url": item["url"],
                
            }

            characters.append(character_dict)
        url = data["next"]
        
    return characters 