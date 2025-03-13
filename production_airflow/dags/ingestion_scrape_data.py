from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import pandas as pd
import os
from selenium import webdriver
import time
import re
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Konfigurasi default args untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Variabel lingkungan
BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATA_DIR = "/opt/airflow/data/"
BATCH_SIZE = 100000

# Load kredensial GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY

def scrape_adapundi():
    """
    Scrape data from Adapundi website using combined approach
    """
    driver = None
    try:
        # Set up Chrome options
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        # Initialize driver
        driver = webdriver.Chrome(options=options)
        
        # Navigate to website
        url = 'https://www.adapundi.com/'
        driver.get(url)
        time.sleep(5)  # Initial wait for page load
        
        # Scroll down gradually to load all elements
        for _ in range(9):
            driver.execute_script("window.scrollBy(0, 300)")
            time.sleep(2)
        
        # Get page source and create BeautifulSoup object
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract data using the specific class names
        items = soup.find_all('div', class_='col-md-3 col-12')
        
        if not items:
            raise Exception("No statistics items found with class 'col-md-3 col-12'")
        
        stats_data = []
        for item in items:
            try:
                # Extract description
                keterangan_tag = item.find('p', class_='mb-0 ml-2 align-self-center')
                keterangan = keterangan_tag.text.strip() if keterangan_tag else None
                
                # Extract value
                jumlah_tag = item.find('h5', class_='text-center f-semiBlack mb-4')
                jumlah = re.sub(r'[^0-9]', '', jumlah_tag.text) if jumlah_tag else None
                
                if keterangan and jumlah:
                    stats_data.append({
                        'metric': keterangan,
                        'value': jumlah,
                        'scraped_date': datetime.now().strftime('%Y-%m-%d')
                    })
            except Exception as e:
                print(f"Error extracting item: {e}")
                continue
        
        if not stats_data:
            raise Exception("No valid statistics data could be extracted")
        
        # Create DataFrame
        df = pd.DataFrame(stats_data)
        
        # Ensure output directory exists
        output_dir = '/opt/airflow/data'
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to CSV with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'{output_dir}/adapundi_stats_{timestamp}.csv'
        df.to_csv(output_path, index=False)
        
        print(f"Successfully scraped {len(stats_data)} items")
        return output_path
        
    except Exception as e:
        error_message = f"Error scraping Adapundi: {str(e)}"
        print(error_message)
        raise Exception(error_message)
        
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"Error closing driver: {e}")

def load_to_bigquery_in_batches(project_id, dataset_id, table_name, batch_size=100000, **kwargs):
    """Load semua file CSV dari DATA_DIR ke BigQuery dalam batch."""
    
    DATA_DIR = "/opt/airflow/data/"  # Direktori penyimpanan file CSV

    # Cek apakah ada file CSV di folder
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        print("❌ Tidak ada file CSV untuk diunggah.")
        return

    # Inisialisasi BigQuery Client
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    total_rows = 0

    # Loop untuk membaca dan upload setiap file CSV
    for file_name in csv_files:
        file_path = os.path.join(DATA_DIR, file_name)

        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f"❌ ERROR: Gagal membaca CSV {file_path}: {e}")
            continue

        # Upload data dalam batch
        for start in range(0, len(df), batch_size):
            end = start + batch_size
            batch_df = df.iloc[start:end]

            job = client.load_table_from_dataframe(batch_df, table_id)
            job.result()  # Tunggu hingga job selesai
            
            print(f"✅ Loaded {len(batch_df)} rows from {file_name} into {table_id}")
            total_rows += len(batch_df)

        # Hapus file setelah sukses upload
        os.remove(file_path)
        print(f"🗑️ Deleted {file_path} after successful upload.")

    print(f"✅ Successfully loaded {total_rows} total rows into {table_id}")

# Buat DAG
dag = DAG(
    'adapundi_scraping_bq_batch',
    default_args=default_args,
    description='Scrape Adapundi dan simpan ke BigQuery secara batch',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 2, 6),
    catchup=False,
    max_active_runs=1,
)

# Task scraping
scrape_task = PythonOperator(
    task_id='scrape_adapundi_task',
    python_callable=scrape_adapundi,
    dag=dag,
)

table = "adapundi_stats"

    # Task Load Data ke Staging BigQuery
load_bq_task = PythonOperator(
        task_id=f'load_to_bq',
        python_callable=load_to_bigquery_in_batches,
        op_kwargs={
            'project_id': os.environ.get("BQ_PROJECT"),
            'dataset_id': os.environ.get("BQ_DATASET"),
            'table_name': table,
            'batch_size': 100000
        },
    )

# Set dependencies
scrape_task >> load_bq_task