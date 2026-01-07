import os
from sqlalchemy import create_engine

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", "5432")

IN_DOCKER = os.path.exists("/.dockerenv")
DB_HOST = "db" if IN_DOCKER else "localhost"

print(" Runtime environment:")
print(f"   IN_DOCKER = {IN_DOCKER}")
print(f"   DB_HOST   = {DB_HOST}")

def create_db_engine():
    if DB_USER and DB_PASSWORD and DB_NAME:
        try:
            url = (
                f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
                f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            )
            engine = create_engine(url, pool_pre_ping=True)

            with engine.connect():
                pass

            print(" Connected to PostgreSQL")
            return engine

        except Exception as e:
            print(" PostgreSQL not available, falling back to SQLite")
            print(f"   Reason: {e}")

    print(" Using SQLite (local fallback)")
    return create_engine("sqlite:///starwars.db", echo=False)

engine = create_db_engine()
