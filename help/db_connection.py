from sqlalchemy import create_engine

def postgres_connection(db_name: str):
    user = "dwhuser"
    password = "dwh123"
    host = "localhost"
    port = "3000"

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    return engine
