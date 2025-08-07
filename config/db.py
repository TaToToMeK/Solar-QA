import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text, exc
from sqlalchemy.exc import OperationalError
import logging
logger = logging.getLogger(__name__)

_engine = None

def connect_db():
    """
    Tworzy i zapisuje uchwyt do bazy danych jako singleton.
    """
    global _engine
    if _engine is not None:
        return _engine  # już połączony

    load_dotenv()
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_NAME")
    url = f"mysql+pymysql://{user}:{password}@{host}/{dbname}"

    _engine = create_engine(url)
    my_check_connection(_engine)
    return _engine

def get_engine():
    """
    Zwraca singleton `engine`, po wcześniejszym wywołaniu `connect_db()`.
    """
    if _engine is None:
        raise RuntimeError("Engine has not been initialized. Call connect_db() first.")
    return _engine

def my_check_connection(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("✅ Połączenie z bazą działa!")
            print("Wynik testowego zapytania:", result.scalar())
            return True
    except Exception as e:
        print(f"❌ Błąd połączenia: {e}")
        return False