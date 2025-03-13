# db_connection.py
import psycopg2
from psycopg2 import OperationalError
import logging
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv



load_dotenv()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection():
    """Membuka koneksi ke database PostgreSQL menggunakan environment variables."""
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT")
        )
        logging.info("Berhasil terhubung ke database.")
        return conn
    except OperationalError as e:
        logging.error(f"Kesalahan koneksi database: {e}")
        raise



POSTGRES_CONFIG = {
            "database":os.environ.get("DB_NAME"),
            "user":os.environ.get("DB_USER"),
            "password":os.environ.get("DB_PASSWORD"),
            "host":os.environ.get("DB_HOST"),
            "port":os.environ.get("DB_PORT")
}


def get_postgres_engine():
    url = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    return create_engine(url)