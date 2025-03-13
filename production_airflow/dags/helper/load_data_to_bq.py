from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import pandas as pd

BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")


GCP_SERVICE_ACCOUNT_KEY = '/opt/airflow/keys/gcp_keys.json'
DATA_DIR = "/opt/airflow/data/"  
BATCH_SIZE = 100000  


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY

def create_bigquery_dataset():
    client = bigquery.Client(project=BQ_PROJECT)
    dataset_ref = client.dataset(BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {BQ_DATASET} sudah ada.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"
        client.create_dataset(dataset)
        print(f"Dataset {BQ_DATASET} telah dibuat.")

def load_to_bigquery_in_batches(file_path, project_id, dataset_id, table_name, batch_size=100000):
    """Load data from JSON file to BigQuery in batches."""
    
    # **1. Pastikan file JSON ada**
    try:
        df = pd.read_json(file_path, orient="records")
    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        return
    
    # **2. Inisialisasi BigQuery Client**
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.staging_{table_name}"
    
    # **3. Load data ke BigQuery dalam batch**
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        batch_df = df.iloc[start:end]

        job = client.load_table_from_dataframe(batch_df, table_id)
        job.result()  # Tunggu hingga selesai
        
        print(f"Loaded {len(batch_df)} rows into {table_id}")

    print(f"Successfully loaded {len(df)} total rows into {table_id}")

def move_data_from_staging_to_production(table_name, project_id, dataset_id, primary_keys):
    """Memindahkan data dari staging ke production setelah menghapus duplikat."""
    client = bigquery.Client(project=project_id)
    staging_table_id = f"{project_id}.{dataset_id}.staging_{table_name}"
    production_table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Pastikan tabel staging ada
    query_check_staging = f"""
    SELECT COUNT(*) AS total 
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'staging_{table_name}'
    """
    result_staging = client.query(query_check_staging).result()
    if next(result_staging).total == 0:
        raise ValueError(f"❌ ERROR: Tabel staging {staging_table_id} tidak ditemukan!")
    
    # Pastikan tabel produksi ada
    schema_query = f"""
    SELECT column_name, data_type FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'staging_{table_name}'
    """
    schema_result = client.query(schema_query).result()
    schema = [bigquery.SchemaField(row.column_name, row.data_type) for row in schema_result]
    table = bigquery.Table(production_table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    
    # Hapus duplikat dan pindahkan data ke production
    primary_key_condition = " , ".join(primary_keys)
    query = f"""
    CREATE OR REPLACE TABLE `{production_table_id}` AS
    SELECT DISTINCT * FROM `{staging_table_id}`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {primary_key_condition} ORDER BY CURRENT_TIMESTAMP()) = 1
    """
    job = client.query(query)
    job.result()
    print(f"✅ Data berhasil dipindahkan ke {production_table_id}")
