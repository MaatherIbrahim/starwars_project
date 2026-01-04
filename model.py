from sqlalchemy import Table, Column, Integer, Text, MetaData, ForeignKey
from db import engine

metadata = MetaData()

planets = Table(
    "planets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, unique=True),
    Column("climate", Text),
    Column("terrain", Text),
    Column("population", Text),
)

characters = Table(
    "characters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text),
    Column("height", Text),
    Column("mass", Text),
    Column("gender", Text),
    Column("birth_year", Text),
    Column("planet_id", Integer, ForeignKey("planets.id")),
)

metadata.create_all(engine)
