
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto

# Load environment variables
load_dotenv()

# --- Configuration ---
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
CHANNELS = [
    "lobelia4cosmetics",
    "tikvahpharma",
    "Chemedhealth",
]
DATA_DIR = "data/raw/telegram_messages"
IMAGE_DIR = "data/raw/images"
LOG_FILE = "scraper.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_full_path(path):
    return os.path.join(get_script_dir(), path)

async def scrape_channel(client, channel_name):
    """Scrapes messages and images from a single Telegram channel."""
    logging.info(f"Scraping channel: {channel_name}")
    today = datetime.now().strftime("%Y-%m-%d")
    channel_data_dir = os.path.join(DATA_DIR, today, channel_name)
    channel_image_dir = os.path.join(IMAGE_DIR, today, channel_name)

    os.makedirs(channel_data_dir, exist_ok=True)
    os.makedirs(channel_image_dir, exist_ok=True)

    try:
        async for message in client.iter_messages(channel_name, limit=100):  # Limit for demonstration
            # 1. Save Message Data
            message_data = message.to_dict()
            file_path = os.path.join(channel_data_dir, f"{message.id}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(message_data, f, ensure_ascii=False, indent=4, default=str)

            # 2. Download Images
            if message.media and isinstance(message.media, MessageMediaPhoto):
                image_path = os.path.join(channel_image_dir, f"{message.id}.jpg")
                await client.download_media(message.media, file=image_path)
                logging.info(f"Downloaded image {message.id}.jpg from {channel_name}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_name}: {e}")

async def main():
    """Main function to connect to Telegram and start scraping."""
    if not API_ID or not API_HASH:
        logging.error("Telegram API_ID and API_HASH must be set in .env file.")
        return

    # Use a session file within the scraper directory
    session_file = get_full_path('telegram_session.session')

    async with TelegramClient(session_file, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            await scrape_channel(client, channel)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
