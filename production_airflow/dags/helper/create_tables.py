# create_table.py
from helper.db_connection import get_db_connection
from psycopg2 import OperationalError

def create_tables():
    """Create tables in the PostgreSQL database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        create_schema = """
        CREATE SCHEMA IF NOT EXISTS project;
        """

        create_attendees_table = """
        CREATE TABLE IF NOT EXISTS project.attendees (
            attendee_id VARCHAR(100) PRIMARY KEY,
            attendee_name VARCHAR(100),
            email VARCHAR(100),
            phone VARCHAR(20),
            date_of_birth DATE,
            address VARCHAR(255),
            created_at TIMESTAMP
        );
        """

        create_events_table = """
        CREATE TABLE IF NOT EXISTS project.events (
            event_id VARCHAR(100) PRIMARY KEY,
            event_name VARCHAR(150),
            location VARCHAR(100),
            duration VARCHAR(20),
            fee INTEGER,
            event_date TIMESTAMP,
            created_at TIMESTAMP
        );
        """

        create_registrations_table = """
        CREATE TABLE IF NOT EXISTS project.registrations (
            registration_id INTEGER PRIMARY KEY,
            attendee_id VARCHAR(100),
            event_id VARCHAR(100),
            registration_date TIMESTAMP,
            payment_status VARCHAR(20),
            created_at TIMESTAMP,
            FOREIGN KEY (attendee_id) REFERENCES project.attendees(attendee_id),
            FOREIGN KEY (event_id) REFERENCES project.events(event_id)
        );
        """

        cur.execute(create_schema)
        cur.execute(create_attendees_table)
        cur.execute(create_events_table)
        cur.execute(create_registrations_table)

        conn.commit()
        cur.close()
        conn.close()

    except OperationalError as e:
        print(f"Error while creating tables: {e}")
        raise
