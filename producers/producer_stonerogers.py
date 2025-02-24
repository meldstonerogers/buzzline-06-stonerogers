# Import Modules
#####################################
import os
import sys
import time
from dotenv import load_dotenv
from newsapi import NewsApiClient  # Import the NewsApiClient
from utils.utils_logger import logger
import psycopg2

#####################################
# Load Environment Variables
#####################################
load_dotenv()

# Fetch NewsAPI Key from environment
API_KEY = os.getenv("NEWSAPI_KEY")
logger.info(f"Fetched API Key: {API_KEY}")
newsapi = NewsApiClient(api_key=API_KEY)  # Initialize NewsApiClient

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    try:
        interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 60))
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

def insert_message_to_db(message: dict, db_host: str, db_name: str, db_user: str, db_password: str):
    """Insert a processed news article into the PostgreSQL database."""
    logger.info(f"Inserting message into PostgreSQL: {message}")

    insert_query = """
    INSERT INTO news_articles (source, title, url, published_at)
    VALUES (%s, %s, %s, %s);
    """

    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute(insert_query, (
            message.get("source"),
            message.get("title"),
            message.get("url"),
            message.get("published_at")
        ))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Inserted news article into PostgreSQL successfully.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into PostgreSQL: {e}")

def main():
    logger.info("START producer.")

    # Database details
    db_host = os.getenv("POSTGRES_HOST")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    interval_secs = get_message_interval()

    try:
        while True:
            logger.info("Fetching news data...")
            news_data = fetch_news_data()

            if news_data is None:
                logger.warning("No news articles retrieved from API.")
            elif not news_data:
                logger.warning("Empty news articles list retrieved from API.")
            else:
                for article in news_data:
                    insert_message_to_db(article, db_host, db_name, db_user, db_password)

            logger.info(f"Waiting for {interval_secs} seconds before the next fetch...")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        logger.info("END producer.")

#####################################
# Conditional Execution
#####################################
if __name__ == "__main__":
    main()
