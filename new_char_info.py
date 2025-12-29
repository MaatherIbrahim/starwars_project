import pandas as pd
from config import New_Republic_DB
import requests
def new_char_info(limit):
    characters = []
    url = New_Republic_DB
    while url and len(characters) < limit:
        response = requests.get(url)
        response.raise_for_status()
        data2 = response.json()
        for item in data2:
            if len(characters) >= limit:
                break
            character_dict2 = {
                "name": item["name"],
                "height": item["height"],
                "mass": item["mass"],
                "hair_color": item["hair_color"],
                "skin_color": item["skin_color"],
                "eye_color": item["eye_color"],
                "birth_year": item["birth_year"],
                "gender": item["gender"],
                "homeworld" : item["homeworld"]
            }

            characters.append(character_dict2)
        
        
    return characters 