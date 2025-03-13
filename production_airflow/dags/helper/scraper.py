import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import re

def datapundi_statistik(url):
    """Scrape data statistik dari Adapundi."""

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Remote(
        command_executor="http://selenium-chrome:4444/wd/hub",
        options=options
    )

    url = 'https://www.adapundi.com/'
    driver.get(url)
    time.sleep(5) 

    for _ in range(9):  
        driver.execute_script("window.scrollBy(0, 300)")
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    data = []

    items = soup.find_all('div', class_='col-md-3 col-12')
    for item in items:
        keterangan_tag = item.find('p', class_='mb-0 ml-2 align-self-center')
        keterangan = keterangan_tag.text.strip() if keterangan_tag else "keterangan tidak ditemukan"
        
        jumlah_tag = item.find('h5', class_='text-center f-semiBlack mb-4')
        jumlah = re.sub(r'[^0-9]', '', jumlah_tag.text) if jumlah_tag else "0"

        data.append((keterangan, jumlah))

    driver.quit()
    
    df = pd.DataFrame(data, columns=["Keterangan", "Jumlah"])
    df.to_csv("/opt/airflow/data/datapundi_statistik.csv", index=False)
    
    return df