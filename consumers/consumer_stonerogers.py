#####################################
# Import Modules
#####################################

import json
import os
import sys
import time
import psycopg2
from kafka import KafkaConsumer
from utils.utils_logger import logger
import utils.utils_config as config

#####################################
# PostgreSQL Functions
#####################################

def init_db(db_host: str, db_name: str, db_user: str, db_password: str):
    """
    Initialize the PostgreSQL database by creating the required table.
    """
    logger.info(f"Initializing PostgreSQL database '{db_name}'.")
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS news_articles (
        id SERIAL PRIMARY KEY,
        source VARCHAR(255),
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        published_at TIMESTAMP
    );
    """

    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("SUCCESS: PostgreSQL database and table initialized.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize PostgreSQL: {e}")
        sys.exit(1)


def insert_message(message: dict, db_host: str, db_name: str, db_user: str, db_password: str):
    """
    Insert a processed news article into the PostgreSQL database.
    """
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


#####################################
# Message Processing
#####################################

def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message from Kafka.
    """
    logger.info(f"Processing message: {message}")
    try:
        processed_message = {
            "source": message.get("source"),
            "title": message.get("title"),
            "url": message.get("url"),
            "published_at": message.get("published_at")
        }
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Kafka Consumer
#####################################

def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    db_host: str,
    db_name: str,
    db_user: str,
    db_password: str,
    interval_secs: int,
):
    """
    Consume messages from Kafka and store them in PostgreSQL.
    """
    logger.info(f"Starting Kafka consumer for topic {topic}")

    try:
        consumer = KafkaConsumer(
            topic,
            group_id=group,
            bootstrap_servers=kafka_url,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, db_host, db_name, db_user, db_password)
            time.sleep(interval_secs)
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Main Function
#####################################

def main():
    """
    Main function to run the consumer process.
    """
    logger.info("Starting Consumer to run continuously.")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()

        # PostgreSQL details
        db_host = config.get_postgres_host()
        db_name = config.get_postgres_db()
        db_user = config.get_postgres_user()
        db_password = config.get_postgres_password()

        logger.info("Successfully read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("Initializing PostgreSQL database.")
    init_db(db_host, db_name, db_user, db_password)

    logger.info("Start consuming Kafka messages and inserting into PostgreSQL.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, db_host, db_name, db_user, db_password, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
