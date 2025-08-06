import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text, exc
from sqlalchemy.exc import OperationalError
import logging
logger = logging.getLogger(__name__)

def connect_db():
    load_dotenv()  # Załaduj zmienne z .env
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_NAME")
    url = f"mysql+pymysql://{user}:{password}@{host}/{dbname}"
    engine = create_engine(url)
    my_check_connection(engine)
    return engine

def my_check_connection(engine):
    """
    Sprawdza status połączenia z bazą danych.
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("✅ Połączenie z bazą działa!")
            print("Wynik testowego zapytania:", result.scalar())
            return True
    except Exception as e:
        print(f"❌ Błąd połączenia: {e}")
        return False