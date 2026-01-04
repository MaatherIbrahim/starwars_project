from sqlalchemy import insert, text
from db import engine
from model import planets, characters
import pandas as pd

def load_data(final_df: pd.DataFrame):

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM characters"))
        conn.execute(text("DELETE FROM planets"))

        planet_map = {}

        # Insert planets
        for _, row in final_df.iterrows():
            planet_name = row.get("homeworld_name")

            if pd.isna(planet_name):
                continue

            if planet_name not in planet_map:
                result = conn.execute(
                    insert(planets).values(
                        name=planet_name,
                        climate=row.get("climate"),
                        terrain=row.get("terrain"),
                        population=row.get("population"),
                    )
                )
                planet_map[planet_name] = result.lastrowid

        # Insert characters
        for _, row in final_df.iterrows():
            conn.execute(
                insert(characters).values(
                    name=row.get("name"),
                    height=row.get("height"),
                    mass=row.get("mass"),
                    gender=row.get("gender"),
                    birth_year=row.get("birth_year"),
                    planet_id=planet_map.get(row.get("homeworld_name"))
                )
            )
