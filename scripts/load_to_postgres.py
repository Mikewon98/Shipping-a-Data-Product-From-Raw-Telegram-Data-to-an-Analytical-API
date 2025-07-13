
import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# --- Database Configuration ---
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = "localhost"  # Assumes the script is run on the host or can access the db container
DB_PORT = "5432"

# --- Data Configuration ---
DATA_DIR = "data/raw/telegram_messages"

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

def create_raw_table(conn):
    """Creates the table to store raw JSON data if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_telegram_messages (
                id SERIAL PRIMARY KEY,
                channel_name VARCHAR(255),
                message_id INT,
                data JSONB,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def load_json_to_db(conn):
    """Loads JSON files from the data directory into the database."""
    with conn.cursor() as cur:
        for date_dir in os.listdir(DATA_DIR):
            date_path = os.path.join(DATA_DIR, date_dir)
            if not os.path.isdir(date_path): continue

            for channel_dir in os.listdir(date_path):
                channel_path = os.path.join(date_path, channel_dir)
                if not os.path.isdir(channel_path): continue

                for file_name in os.listdir(channel_path):
                    if file_name.endswith(".json"):
                        file_path = os.path.join(channel_path, file_name)
                        with open(file_path, 'r', encoding='utf-8') as f:
                            message_data = json.load(f)
                            message_id = message_data.get('id')

                            # Insert data into the table
                            cur.execute("""
                                INSERT INTO raw_telegram_messages (channel_name, message_id, data)
                                VALUES (%s, %s, %s)
                            """, (channel_dir, message_id, json.dumps(message_data)))
    conn.commit()

def main():
    """Main function to run the data loading process."""
    try:
        conn = get_db_connection()
        print("Database connection successful.")

        create_raw_table(conn)
        print("'raw_telegram_messages' table ensured.")

        load_json_to_db(conn)
        print("Data loading complete.")

        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
