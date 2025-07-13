import os
import json
import logging
import psycopg2
from ultralytics import YOLO
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
IMAGE_DIR = "data/raw/images"
DETECTION_OUTPUT_DIR = "data/processed/detections"
LOG_FILE = "object_detection.log"

# --- Database Configuration ---
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = "db"  # Changed to 'db' for Docker Compose service name
DB_PORT = "5432"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

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

def create_detections_table(conn):
    """Creates the table to store image detection data if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_image_detections (
                id SERIAL PRIMARY KEY,
                message_id INT NOT NULL,
                channel_name VARCHAR(255),
                detected_object_class VARCHAR(255) NOT NULL,
                confidence_score NUMERIC(5,4) NOT NULL,
                image_path VARCHAR(512),
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def get_message_id_from_filename(filename):
    """Extracts message ID from image filename (e.g., 12345.jpg -> 12345)."""
    try:
        return int(os.path.splitext(filename)[0])
    except ValueError:
        return None

def main():
    logging.info("Starting object detection process.")

    conn = None
    try:
        conn = get_db_connection()
        create_detections_table(conn)
        logging.info("Database connection successful and raw_image_detections table ensured.")
    except Exception as e:
        logging.error(f"Failed to connect to database or create table: {e}")
        return

    # Load a pre-trained YOLOv8 model
    try:
        model = YOLO('yolov8n.pt')  # Using nano model for demonstration
        logging.info("YOLOv8 model loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading YOLOv8 model: {e}")
        if conn: conn.close()
        return

    os.makedirs(DETECTION_OUTPUT_DIR, exist_ok=True)

    with conn.cursor() as cur:
        for date_dir in os.listdir(IMAGE_DIR):
            date_path = os.path.join(IMAGE_DIR, date_dir)
            if not os.path.isdir(date_path): continue

            for channel_dir in os.listdir(date_path):
                channel_path = os.path.join(date_path, channel_dir)
                if not os.path.isdir(channel_path): continue

                for image_file in os.listdir(channel_path):
                    if image_file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                        image_path = os.path.join(channel_path, image_file)
                        message_id = get_message_id_from_filename(image_file)

                        if message_id is None:
                            logging.warning(f"Skipping invalid image filename: {image_file}")
                            continue

                        logging.info(f"Processing image: {image_file} from channel: {channel_dir}")

                        try:
                            results = model(image_path)
                            for r in results:
                                for box in r.boxes:
                                    class_id = int(box.cls[0])
                                    confidence = float(box.conf[0])
                                    class_name = model.names[class_id]

                                    cur.execute("""
                                        INSERT INTO raw_image_detections (
                                            message_id, channel_name, detected_object_class, confidence_score, image_path
                                        )
                                        VALUES (%s, %s, %s, %s, %s)
                                    """, (
                                        message_id, channel_dir, class_name, confidence, image_path
                                    ))
                            conn.commit()
                            logging.info(f"Detected objects in {image_file} and saved to DB.")

                        except Exception as e:
                            logging.error(f"Error processing image {image_file}: {e}")

    if conn: conn.close()
    logging.info("Object detection process completed.")

if __name__ == "__main__":
    main()