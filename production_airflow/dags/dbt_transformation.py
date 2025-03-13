from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from helper.discord_alert import send_discord_alert
import os

# Define the commands with full paths
DBT_CMD = os.environ.get("DBT_CMD")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR")

default_args = {
    'owner': 'airflow',

    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'dbt_transformations',
    default_args={**default_args, 'on_failure_callback': send_discord_alert},
    description='DBT transformations for event analytics',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2024, 2, 1),
    catchup=False,
) as dag:

    # Add environment setup
    setup_env = BashOperator(
        task_id='setup_environment',
        bash_command=f'cd {DBT_PROJECT_DIR} && ls -la',
    )

    # Update dbt run command with full path
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'''
            export PATH="/home/airflow/.local/bin:$PATH" &&
            cd {DBT_PROJECT_DIR} &&
            {DBT_CMD} run --profiles-dir {DBT_PROFILES_DIR}
        ''',
        env={
            'DBT_PROFILES_DIR': DBT_PROFILES_DIR,
            'BQ_PROJECT': '{{ var.value.BQ_PROJECT }}',
            'BQ_DATASET': '{{ var.value.BQ_DATASET }}'
        }
    )

    # Update dbt test command with full path
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'''
            export PATH="/home/airflow/.local/bin:$PATH" &&
            cd {DBT_PROJECT_DIR} &&
            {DBT_CMD} test --profiles-dir {DBT_PROFILES_DIR}
        ''',
        env={
            'DBT_PROFILES_DIR': DBT_PROFILES_DIR,
            'BQ_PROJECT': '{{ var.value.BQ_PROJECT }}',
            'BQ_DATASET': '{{ var.value.BQ_DATASET }}'
        }
    )

    setup_env >> dbt_run >> dbt_test