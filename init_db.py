from db import engine
from model import metadata

if __name__ == "__main__":
    metadata.create_all(engine)
    print("Database tables created successfully")
