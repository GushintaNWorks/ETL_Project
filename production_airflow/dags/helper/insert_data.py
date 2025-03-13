import os
import random
import pytz
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker
from psycopg2 import OperationalError
from helper.db_connection import get_db_connection

# Load environment variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

fake = Faker()

# Fungsi untuk mengambil data dari API
API_ENDPOINT = "https://randomuser.me/api/"

def retrieve_user_data(api_url=API_ENDPOINT) -> dict:
    """Fetches random user data from API endpoint."""
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()['results'][0]
    else:
        logging.warning(f"Gagal mengambil data dari API. Status code: {response.status_code}")
        return {}

def insert_attendees():
    """Insert data ke tabel attendees"""
    try:
        local_tz = pytz.timezone("Asia/Jakarta")
        created_at = datetime.now().astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT attendee_id FROM project.attendees ORDER BY attendee_id DESC LIMIT 1")
                last_attendee_id = cur.fetchone()
                next_attendee_id = 1 if not last_attendee_id else int(last_attendee_id[0]) + 1

                for _ in range(10):
                    api_data = retrieve_user_data()
                    attendee_id = str(next_attendee_id).zfill(5)
                    attendee_name = f"{api_data['name']['first']} {api_data['name']['last']}"
                    email = api_data['email']
                    phone = api_data['phone']
                    date_of_birth = api_data['dob']['date'][:10]  
                    address = f"{api_data['location']['street']['number']} {api_data['location']['street']['name']}, {api_data['location']['city']}"

                    cur.execute(
                        """
                        INSERT INTO project.attendees (attendee_id, attendee_name, email, phone, date_of_birth, address, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (attendee_id, attendee_name, email, phone, date_of_birth, address, created_at)
                    )
                    next_attendee_id += 1  

                conn.commit()
                logging.info("Success insert data attendees into database.")

    except OperationalError as e:
        logging.error(f"Error inserting attendees: {e}")
        raise

# Fungsi untuk membuat nama event secara random
event_types = ["Conference", "Workshop", "Webinar", "Seminar", "Festival", "Expo", "Hackathon", "Theatre"]
prefixes = ["cyber", "tech", "digit", "info", "crypto", "nano", "meta", "bio"]
roots = ["tron", "byte", "chip", "logic", "data", "graph", "scope", "design"]

def generate_random_event_name():
    return f"{random.choice(event_types)} on {random.choice(prefixes)}{random.choice(roots)}"

def insert_events():
    """Insert event data ke tabel events."""
    try:
        local_tz = pytz.timezone("Asia/Jakarta")
        created_at = datetime.now().astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT event_id FROM project.events ORDER BY event_id DESC LIMIT 1")
                last_event_id = cur.fetchone()
                next_event_id = 1 if not last_event_id else int(last_event_id[0]) + 1

                for _ in range(5):
                    event_id = str(next_event_id).zfill(5)
                    event_name = generate_random_event_name()
                    location = fake.city()
                    duration = f"{random.randint(1, 6)} Hours"
                    fee = random.randint(500000, 5000000)
                    event_date = fake.date_between(start_date="-1y", end_date="today")

                    cur.execute(
                        """
                        INSERT INTO project.events (event_id, event_name, location, duration, fee, event_date, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (event_id, event_name, location, duration, fee, event_date, created_at)
                    )
                    next_event_id += 1

                conn.commit()
                logging.info("Success insert data events into database.")

    except OperationalError as e:
        logging.error(f"Error inserting events: {e}")
        raise

def insert_registrations():
    """Insert registration data into the PostgreSQL database."""
    try:
        local_tz = pytz.timezone("Asia/Jakarta")
        created_at = datetime.now().astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT registration_id FROM project.registrations ORDER BY registration_id DESC LIMIT 1")
        last_registration_id = cur.fetchone()
        next_registration_id = 1 if not last_registration_id else int(last_registration_id[0]) + 1

        cur.execute("SELECT attendee_id FROM project.attendees")
        attendee_ids = [row[0] for row in cur.fetchall()]

        # Get all event_ids and their event_dates
        cur.execute("SELECT event_id, event_date FROM project.events")
        events = cur.fetchall()
        event_dict = {event_id: event_date for event_id, event_date in events}

        for _ in range(15):
            registration_id = str(next_registration_id).zfill(5)
            attendee_id = random.choice(attendee_ids)
            event_id = random.choice(list(event_dict.keys()))
            event_date = event_dict[event_id]

            # Jika event_date belum dalam bentuk datetime, pastikan menggunakan parsing
            if isinstance(event_date, str):
                event_date_dt = datetime.strptime(event_date, "%Y-%m-%d")
            else:
                event_date_dt = event_date  # Jika sudah datetime, langsung gunakan

            # Calculate registration_date (7-14 days before event_date)
            registration_date_dt = event_date_dt - timedelta(days=random.randint(7, 14))
            registration_date = registration_date_dt.strftime("%Y-%m-%d %H:%M:%S")


            payment_status = random.choice(['Paid', 'Unpaid'])

            cur.execute(
                """
                INSERT INTO project.registrations (registration_id, attendee_id, event_id, registration_date, payment_status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (registration_id, attendee_id, event_id, registration_date, payment_status, created_at)
            )

            next_registration_id += 1

        conn.commit()
        cur.close()
        conn.close()

    except OperationalError as e:
        print(f"Error while inserting registrations: {e}")
        raise

