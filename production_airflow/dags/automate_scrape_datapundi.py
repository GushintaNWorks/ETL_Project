from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import re
from helper.discord_alert import send_discord_alert

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': send_discord_alert,
}

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

# Create the DAG
dag = DAG(
    'adapundi_scraping',
    default_args=default_args,
    description='Scrape statistics from Adapundi website daily',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2024, 2, 6),
    catchup=False,
    max_active_runs=1,
)

# Define tasks
scrape_task = PythonOperator(
    task_id='scrape_adapundi_task',
    python_callable=scrape_adapundi,
    dag=dag,
)

# Set task dependencies
scrape_task