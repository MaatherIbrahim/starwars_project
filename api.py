from fastapi import FastAPI, HTTPException, Query, Request
from sqlalchemy import select, text
from db import engine
from model import characters, planets
from logger_config import setup_logger
import time
import pandas as pd

from main import (
    char_info,
    new_char_info,
    LIMIT,
    load_homeworld_cache,
    HOMEWORLD_CACHE_FILE,
    fetch_homeworld,
    FINAL_CSV_FILE,
    FINAL_JSON_FILE,
    load_data,
)

app = FastAPI(title="Star Wars Character Registry")

# =========================
# GET ALL CHARACTERS
# =========================
@app.get("/characters")
def get_characters(
    name: str | None = Query(default=None),
    planet: str | None = Query(default=None),
):
    with engine.connect() as conn:
        query = """
            SELECT
                c.id,
                c.name,
                c.height,
                c.mass,
                c.gender,
                c.birth_year,

                p.name AS planet_name,
                p.climate,
                p.terrain,
                p.population,
                p.rotation_period,
                p.orbital_period,
                p.diameter,
                p.gravity,
                p.surface_water

            FROM characters c
            LEFT JOIN planets p
                ON c.planet_id = p.id
            WHERE 1=1
        """

        params = {}

        if name:
            query += " AND c.name ILIKE :name"
            params["name"] = f"%{name}%"

        if planet:
            query += " AND p.name ILIKE :planet"
            params["planet"] = f"%{planet}%"

        query += " ORDER BY c.id"

        result = conn.execute(text(query), params)
        rows = result.fetchall()

        output = []
        for row in rows:
            d = dict(row._mapping)

            character = {
                "id": d["id"],
                "name": d["name"],
                "height": d["height"],
                "mass": d["mass"],
                "gender": d["gender"],
                "birth_year": d["birth_year"],
                "planet": {
                    "name": d["planet_name"],
                    "climate": d["climate"],
                    "terrain": d["terrain"],
                    "population": d["population"],
                    "rotation_period": d["rotation_period"],
                    "orbital_period": d["orbital_period"],
                    "diameter": d["diameter"],
                    "gravity": d["gravity"],
                    "surface_water": d["surface_water"],
                },
            }

            output.append(character)

        return output


# =========================
# GET CHARACTER BY NAME
# =========================
@app.get("/characters/name/{name}")
def get_character_by_name(name: str):
    with engine.connect() as conn:
        result = conn.execute(
            select(
                characters.c.id,
                characters.c.name,
                characters.c.height,
                characters.c.mass,
                characters.c.gender,
                characters.c.birth_year,

                planets.c.name.label("planet_name"),
                planets.c.climate,
                planets.c.terrain,
                planets.c.population,
                planets.c.rotation_period,
                planets.c.orbital_period,
                planets.c.diameter,
                planets.c.gravity,
                planets.c.surface_water,
            )
            .select_from(
                characters.join(
                    planets,
                    characters.c.planet_id == planets.c.id,
                    isouter=True,
                )
            )
            .where(characters.c.name.ilike(name))
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Character not found")

        d = dict(result._mapping)

        return {
            "id": d["id"],
            "name": d["name"],
            "height": d["height"],
            "mass": d["mass"],
            "gender": d["gender"],
            "birth_year": d["birth_year"],
            "planet": {
                "name": d["planet_name"],
                "climate": d["climate"],
                "terrain": d["terrain"],
                "population": d["population"],
                "rotation_period": d["rotation_period"],
                "orbital_period": d["orbital_period"],
                "diameter": d["diameter"],
                "gravity": d["gravity"],
                "surface_water": d["surface_water"],
            },
        }


# =========================
# GET CHARACTER BY ID
# =========================
@app.get("/characters/{character_id}")
def get_character_by_id(character_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            select(
                characters.c.id,
                characters.c.name,
                characters.c.height,
                characters.c.mass,
                characters.c.gender,
                characters.c.birth_year,

                planets.c.name.label("planet_name"),
                planets.c.climate,
                planets.c.terrain,
                planets.c.population,
                planets.c.rotation_period,
                planets.c.orbital_period,
                planets.c.diameter,
                planets.c.gravity,
                planets.c.surface_water,
            )
            .select_from(
                characters.join(
                    planets,
                    characters.c.planet_id == planets.c.id,
                    isouter=True,
                )
            )
            .where(characters.c.id == character_id)
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Character not found")

        d = dict(result._mapping)

        return {
            "id": d["id"],
            "name": d["name"],
            "height": d["height"],
            "mass": d["mass"],
            "gender": d["gender"],
            "birth_year": d["birth_year"],
            "planet": {
                "name": d["planet_name"],
                "climate": d["climate"],
                "terrain": d["terrain"],
                "population": d["population"],
                "rotation_period": d["rotation_period"],
                "orbital_period": d["orbital_period"],
                "diameter": d["diameter"],
                "gravity": d["gravity"],
                "surface_water": d["surface_water"],
            },
        }



# GET ALL PLANETS

@app.get("/planets")
def get_planets(name: str | None = Query(default=None)):
    with engine.connect() as conn:
        query = select(planets)
        if name:
            query = query.where(planets.c.name.ilike(f"%{name}%"))

        result = conn.execute(query.order_by(planets.c.id))
        return [dict(row._mapping) for row in result]


# GET PLANET BY NAME
@app.get("/planets/name/{name}")
def get_planet_by_name(name: str):
    with engine.connect() as conn:
        result = conn.execute(
            select(planets).where(planets.c.name.ilike(name))
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Planet not found")

        return dict(result._mapping)
    
# GET PLANET BY ID
@app.get("/planets/id/{planet_id}")
def get_planet_by_id(planet_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            select(planets).where(planets.c.id == planet_id)
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Planet not found")

        return dict(result._mapping)
    
 # GET PLANET WITH CHARACTERS   
@app.get("/planets/name/{planet_name}/characters")
def get_planet_with_characters(planet_name: str):
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                planets.c.id.label("planet_id"),
                planets.c.name.label("planet_name"),
                planets.c.climate,
                planets.c.terrain,
                planets.c.population,
                planets.c.rotation_period,
                planets.c.orbital_period,
                planets.c.diameter,
                planets.c.gravity,
                planets.c.surface_water,

                characters.c.id.label("character_id"),
                characters.c.name.label("character_name"),
                characters.c.height,
                characters.c.mass,
                characters.c.gender,
                characters.c.birth_year,
            )
            .select_from(
                planets.join(
                    characters,
                    planets.c.id == characters.c.planet_id,
                    isouter=True,
                )
            )
            .where(planets.c.name.ilike(planet_name))
        ).fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Planet not found")

        planet = {
            "id": rows[0].planet_id,
            "name": rows[0].planet_name,
            "climate": rows[0].climate,
            "terrain": rows[0].terrain,
            "population": rows[0].population,
            "rotation_period": rows[0].rotation_period,
            "orbital_period": rows[0].orbital_period,
            "diameter": rows[0].diameter,
            "gravity": rows[0].gravity,
            "surface_water": rows[0].surface_water,
            "characters": [],
        }

        for row in rows:
            if row.character_id:
                planet["characters"].append(
                    {
                        "id": row.character_id,
                        "name": row.character_name,
                        "height": row.height,
                        "mass": row.mass,
                        "gender": row.gender,
                        "birth_year": row.birth_year,
                    }
                )

        return planet

# LOGGING

logger = setup_logger()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000
        logger.info(
            f"API CALL: {request.method} {request.url.path} "
            f"status={response.status_code} duration_ms={duration_ms:.2f}"
        )
        return response
    except Exception as e:
        duration_ms = (time.time() - start) * 1000
        logger.exception(
            f"API ERROR: {request.method} {request.url.path} "
            f"duration_ms={duration_ms:.2f} error={str(e)}"
        )
        raise
