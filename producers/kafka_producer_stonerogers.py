# Import Modules
#####################################
import os
import sys
import time
from dotenv import load_dotenv
from newsapi import NewsApiClient  # Import the NewsApiClient
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

# Fetch NewsAPI Key from environment
API_KEY = os.getenv("NEWSAPI_KEY")
logger.info(f"Fetched API Key: {API_KEY}")
newsapi = NewsApiClient(api_key=API_KEY)  # Initialize NewsApiClient

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "news_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    try:
        interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 10))
    except ValueError:
        logger.error("Invalid MESSAGE_INTERVAL_SECONDS; using default value of 10 seconds.")
        interval = 10

    logger.info(f"Message interval: {interval} seconds")
    return interval

def fetch_news_data():
    """Fetch top headlines from NewsAPI using the newsapi-python library."""
    try:
        logger.info("Fetching top headlines...")
        top_headlines = newsapi.get_top_headlines(country='us')

        logger.info(f"API response status: {top_headlines['status']}")  # Log the status
        articles = top_headlines.get("articles", [])
        logger.info(f"Number of articles retrieved: {len(articles)}")  # Log number of articles

        if not articles:
            logger.warning("No articles found in response.")
            return None

        # Process and return the articles
        return [
            {
                "source": article["source"]["name"],
                "title": article["title"],
                "url": article["url"],
                "published_at": article["publishedAt"]
            }
            for article in articles if article.get("title") and article.get("url")
        ]
    except Exception as e:
        logger.error(f"Error fetching news data: {e}")
        return None

#####################################
# Message Generator
#####################################
def generate_messages(producer, topic, interval_secs):
    """Fetch news headlines and send messages to Kafka."""
    try:
        while True:
            logger.info("Fetching news data...")  # Log fetching news data
            news_data = fetch_news_data()  # Fetch data here
            
            # Check the results of fetching news data
            if news_data is None:
                logger.warning("No news articles retrieved from API.")
            elif not news_data:
                logger.warning("Empty news articles list retrieved from API.")
            else:
                for article in news_data:
                    message = (
                        f"Title: {article['title']} | "
                        f"Source: {article['source']} | "
                        f"URL: {article['url']} | "
                        f"Published At: {article['published_at']}"
                    )
                    logger.info(f"Generated news: {message}")

                    # Ensure we are sending a string and encoding it
                    if isinstance(message, str):
                        producer.send(topic, value=message.encode('utf-8'))
                        logger.info(f"Sent message to topic '{topic}': {message}")
                    else:
                        logger.error(f"Message is not a string: {message}")

            logger.info(f"Waiting for {interval_secs} seconds before the next fetch...")
            time.sleep(interval_secs)  # Sleep after processing
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
