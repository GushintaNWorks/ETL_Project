import os
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from helper.extract_table import extract_postgres_data  # Sesuai modul terbaru
from helper.discord_alert import send_discord_alert
from helper.load_data_to_bq import (
    create_bigquery_dataset,
    load_to_bigquery_in_batches,
    move_data_from_staging_to_production
)


TABLES = ["attendees", "events", "registrations"]
DATA_DIR = "/opt/airflow/data/"  
BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")


GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")



TABLE_CONFIG = {
    "attendees": {
        "primary_keys": ["attendee_id"],
        "columns": ['attendee_id', 'attendee_name', 'email', 'phone', 'date_of_birth', 'address', 'created_at']
    },
    "events": {
        "primary_keys": ["event_id"],
        "columns": ['event_id', 'event_name', 'location', 'duration', 'fee', 'event_date', 'created_at']
    },
    "registrations": {
        "primary_keys": ["registration_id"],
        "columns": ['registration_id', 'attendee_id', 'event_id', 'registration_date', 'payment_status', 'created_at']
    }
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id='automate_ingest_data_to_dwh',
    default_args={**default_args, 'on_failure_callback': send_discord_alert},
    description='Ingest data from PostgreSQL to BigQuery with batching',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 1, tzinfo=pytz.timezone("Asia/Jakarta")),
    catchup=False
) as dag:

    
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_bigquery_dataset,
        op_kwargs={'project_id': BQ_PROJECT, 'dataset_id': BQ_DATASET},
    )

    for table, config in TABLE_CONFIG.items():
        with TaskGroup(group_id=table) as table_group:
            
            
            extract_task = PythonOperator(
                task_id=f'extract_{table}',
                python_callable=extract_postgres_data,
            )

            
            load_to_staging_task = PythonOperator(
                task_id=f'load_to_staging_{table}',
                python_callable=load_to_bigquery_in_batches,
                op_kwargs={
                    'file_path': os.path.join(DATA_DIR, f"{table}.json"),
                    'project_id': BQ_PROJECT,
                    'dataset_id': BQ_DATASET,
                    'table_name': table,
                    'batch_size': 100000
                },
            )

            
            move_to_production_task = PythonOperator(
                task_id=f'move_to_production_{table}',
                python_callable=move_data_from_staging_to_production,
                op_kwargs={
                    'table_name': table,
                    'project_id': BQ_PROJECT,
                    'dataset_id': BQ_DATASET,
                    'primary_keys': config["primary_keys"],
                    'partition_column': 'created_at',
                    'columns': config["columns"]
                },
            )

            # Urutan Task dalam Group
            extract_task >> load_to_staging_task >> move_to_production_task

        create_dataset_task >> table_group