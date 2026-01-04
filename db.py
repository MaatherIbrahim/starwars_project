from sqlalchemy import create_engine
import os

DB_URL = os.getenv("DATABASE_URL", "sqlite:///starwars.db")

engine = create_engine(
    DB_URL,
    echo=False,
    connect_args={"check_same_thread": False}  #  for FastAPI
)
