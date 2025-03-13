from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from helper.create_tables import create_tables
from helper.insert_data import insert_attendees,insert_events,insert_registrations
from helper.discord_alert import send_discord_alert


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


jakarta_tz = pytz.timezone("Asia/Jakarta")


with DAG(
    'automate_insert_data_to_db',
    default_args={**default_args, 'on_failure_callback': send_discord_alert},
    description='DAG untuk insert data ke PostgreSQL dengan tabel baru',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 12, 1, tzinfo=jakarta_tz),
    catchup=False
) as dag:

    
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )

   
    insert_attendees_task = PythonOperator(
        task_id='insert_attendees',
        python_callable=insert_attendees
    )

    
    insert_events_task = PythonOperator(
        task_id='insert_events',
        python_callable=insert_events
    )

    
    insert_registrations_task = PythonOperator(
        task_id='insert_registrations',
        python_callable=insert_registrations
    )

    
    create_tables_task >> insert_attendees_task >> insert_events_task >> insert_registrations_task
