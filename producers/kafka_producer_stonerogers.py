#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import requests 

# Import external packages
from dotenv import load_dotenv
load_dotenv()

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

# Fetch NewsAPI Key from environment
API_KEY = os.getenv("NEWSAPI_KEY", "9310f341248d4ad28a3b0bc6391c9fa2")
BASE_URL = "https://newsapi.org/v2/top-headlines"

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "news_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 60))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def fetch_news_data():
    """Fetch top headlines from NewsAPI."""
    url = f"{BASE_URL}?country=us&apiKey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        articles = data.get("articles", [])
        
        if not articles:
            return None
        
        return [
            {
                "source": article["source"]["name"],
                "title": article["title"],
                "url": article["url"],
                "published_at": article["publishedAt"]
            }
            for article in articles if article.get("title") and article.get("url")
        ]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching news data: {e}")
        return None

#####################################
# Message Generator
#####################################

def generate_messages(producer, topic, interval_secs):
    """Fetch news headlines and send messages to Kafka."""
    try:
        while True:
            news_data = fetch_news_data()
            
            if news_data:
                for article in news_data:
                    message = f"Headline: {article['title']} | Source: {article['source']} | {article['url']}"
                    logger.info(f"Generated news: {message}")

                    # Ensure we are sending a string and encoding it
                    if isinstance(message, str):
                        producer.send(topic, value=message.encode('utf-8'))
                        logger.info(f"Sent message to topic '{topic}': {message}")
                    else:
                        logger.error(f"Message is not a string: {message}")

            else:
                logger.warning("No news articles retrieved.")

            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################

def main():
    logger.info("START producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting news production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
