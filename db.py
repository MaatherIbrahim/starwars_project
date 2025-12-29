from sqlalchemy import create_engine
DB_URL = "postgresql+psycopg2://postgres:maather@localhost:5432/starwars"
engine = create_engine(DB_URL)