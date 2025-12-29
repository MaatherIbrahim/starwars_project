from fastapi import FastAPI, HTTPException, Query, Request
from sqlalchemy import select, text
from db import engine
from model import characters, planets
from logger_config import setup_logger
import time
from logger_config import setup_logger
from main import (char_info, new_char_info ,
                   LIMIT, load_homeworld_cache,
                   HOMEWORLD_CACHE_FILE,fetch_homeworld,
                   FINAL_CSV_FILE,FINAL_JSON_FILE,load_data)
import pandas as pd

app = FastAPI(title="Star Wars Character Registry")
## get all names
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
                p.name AS planet,
                p.climate,
                p.population
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
        return [dict(row._mapping) for row in result]

## get name by name    
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
                planets.c.name.label("planet"),
                planets.c.climate,
                planets.c.population
            )
            .select_from(
                characters.join(
                    planets,
                    characters.c.planet_id == planets.c.id,
                    isouter=True
                )
            )
            .where(characters.c.name.ilike(name))
        ).fetchone()

        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Character '{name}' not found"
            )

        return dict(result._mapping)

       
## get name by id
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
                planets.c.name.label("planet"),
                planets.c.climate,
                planets.c.population
            )
            .select_from(
                characters.join(
                    planets,
                    characters.c.planet_id == planets.c.id,
                    isouter=True
                )
            )
            .where(characters.c.id == character_id)
        ).fetchone()

        if result is None:
            raise HTTPException(status_code=404, detail="Character not found")

        return dict(result._mapping)

## get name by planet name
@app.get("/planets/{planet_name}/characters")
def get_characters_by_planet(planet_name: str):
    with engine.connect() as conn:
        result = conn.execute(
            select(
                characters.c.name,
                characters.c.height,
                characters.c.mass,
                characters.c.gender,
                characters.c.birth_year,
                planets.c.name.label("planet"),
                planets.c.climate,
                planets.c.population
            )
            .select_from(
                characters.join(planets, characters.c.planet_id == planets.c.id)
            )
            .where(planets.c.name.ilike(f"%{planet_name}%"))
        ).fetchall()

        return [dict(row._mapping) for row in result]

## get all planets name
@app.get("/planets")
def get_planets(name: str | None = Query(default=None)):
    with engine.connect() as conn:
        query = select(
            planets.c.id,
            planets.c.name,
            planets.c.climate,
            planets.c.population
        )

        if name:
            query = query.where(planets.c.name.ilike(f"%{name}%"))

        result = conn.execute(query.order_by(planets.c.id))
        return [dict(row._mapping) for row in result]

## get  planet name by id
@app.get("/planets/{planet_id}")
def get_planet_by_id(planet_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            select(
                planets.c.id,
                planets.c.name,
                planets.c.climate,
                planets.c.population
            ).where(planets.c.id == planet_id)
        ).fetchone()

        if result is None:
            raise HTTPException(status_code=404, detail="Planet not found")

        return dict(result._mapping)

## for getting logs
logger = setup_logger()

@app.on_event("startup")
def on_startup():
    logger.info("SYSTEM STATUS: API server started successfully")

@app.on_event("shutdown")
def on_shutdown():
    logger.info("SYSTEM STATUS: API server shut down")

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
## Log ETL
def run_etl():
    logger.info("ETL RUN STARTED")

    try:
        logger.info("STEP 1: Fetching Old Republic characters")
        old_info = char_info(LIMIT)

        logger.info("STEP 2: Fetching New Republic characters")
        new_info = new_char_info(LIMIT)

        logger.info("STEP 3: Converting data to DataFrames")
        df_old = pd.DataFrame(old_info)
        df_new = pd.DataFrame(new_info)

        logger.info("STEP 4: Merging character datasets")
        merge_df = pd.merge(df_old, df_new, on="name", how="outer")

        logger.info("STEP 5: Loading homeworld cache")
        planet_cache = load_homeworld_cache(HOMEWORLD_CACHE_FILE)

        logger.info("STEP 6: Fetching homeworld details")
        planet_df = (
            merge_df["homeworld"]
            .apply(lambda url: fetch_homeworld(url, planet_cache, HOMEWORLD_CACHE_FILE))
            .apply(pd.Series)
        )

        logger.info("STEP 7: Creating final dataset")
        final_df = pd.concat([merge_df, planet_df], axis=1)

        logger.info("STEP 8: Saving CSV and JSON outputs")
        final_df.to_csv(FINAL_CSV_FILE, index=False)
        final_df.to_json(FINAL_JSON_FILE, orient="records", indent=2)

        logger.info("STEP 9: Loading data into PostgreSQL")
        load_data(final_df)

        logger.info("ETL RUN COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.exception("ETL RUN FAILED")
        raise
