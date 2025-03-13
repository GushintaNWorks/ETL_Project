from datetime import datetime, timedelta
import pandas as pd
import os
from helper.db_connection import get_postgres_engine

TABLES = ["attendees", "events", "registrations"]
DATA_DIR = "/opt/airflow/data/"  


os.makedirs(DATA_DIR, exist_ok=True)


def extract_postgres_data():
    yesterday = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')
    engine = get_postgres_engine()

    for table in TABLES:
        query = f"""
        SELECT * FROM project.{table}
        WHERE created_at >= '{yesterday} 00:00:00' AND created_at < '{yesterday} 23:59:59'
        """
        df = pd.read_sql_query(query, con=engine)

        # Simpan hasil ekstraksi ke JSON
        file_path = os.path.join(DATA_DIR, f"{table}.json")
        df.to_json(file_path, orient="records", date_format="iso")

        print(f"Extracted {len(df)} rows from table {table} and saved to {file_path}")