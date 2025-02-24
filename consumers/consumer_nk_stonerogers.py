# Import Modules
#####################################
import os
import sys
import time
import psycopg2
from utils.utils_logger import logger
from textblob import TextBlob  # For sentiment analysis
import utils.utils_config as config

#####################################
# PostgreSQL Functions
#####################################

def get_db_connection(db_host: str, db_name: str, db_user: str, db_password: str):
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        logger.error(f"ERROR: Could not connect to PostgreSQL: {e}")
        sys.exit(1)

def fetch_news_articles(conn):
    """Fetch news articles from the PostgreSQL database."""
    logger.info("Fetching news articles from PostgreSQL.")
    articles = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT source, title, url, published_at FROM news_articles;")
            articles = cursor.fetchall()
    except Exception as e:
        logger.error(f"ERROR: Failed to fetch news articles: {e}")
    return articles

def analyze_sentiment(title: str) -> str:
    """Analyze sentiment of the given title and return the sentiment polarity."""
    analysis = TextBlob(title)
    # Polarity ranges from -1 (negative) to 1 (positive)
    return analysis.sentiment.polarity

def main():
    logger.info("START consumer.")

    # Database details
    db_host = os.getenv("POSTGRES_HOST")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")

    conn = get_db_connection(db_host, db_name, db_user, db_password)

    try:
        while True:
            articles = fetch_news_articles(conn)
            if not articles:
                logger.info("No articles found in the database.")
            else:
                for article in articles:
                    source, title, url, published_at = article
                    sentiment_score = analyze_sentiment(title)
                    logger.info(f"Title: {title} | Source: {source} | URL: {url} | Published At: {published_at} | Sentiment Score: {sentiment_score}")
                    
                    # Update the sentiment score in the database
                    try:
                        with conn.cursor() as cursor:
                            update_query = """
                            UPDATE news_articles
                            SET sentiment_score = %s
                            WHERE url = %s;
                            """
                            cursor.execute(update_query, (sentiment_score, url))
                            conn.commit()
                            logger.info(f"Updated sentiment score for article: {title}")
                    except Exception as e:
                        logger.error(f"ERROR: Failed to update sentiment score for {title}: {e}")

            # Sleep for a defined interval before fetching new articles
            time.sleep(10)  # Adjust the interval as needed

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        conn.close()
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
