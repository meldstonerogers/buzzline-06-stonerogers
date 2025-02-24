"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 

It centralizes the configuration management 
by loading environment variables from .env in the root project folder
 and constructing file paths using pathlib. 

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

# import from Python Standard Library
import os
import pathlib

# import from external packages
from dotenv import load_dotenv

# import from local modules
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_message_interval_seconds_as_int() -> int:
    """Fetch MESSAGE_INTERVAL_SECONDS from environment or use default."""
    try:
        interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
        logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval}")
        return interval
    except ValueError:
        logger.error("Invalid value for MESSAGE_INTERVAL_SECONDS; using default (5).")
        return 5

def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir


def get_database_type() -> str:
    """Fetch DATABASE_TYPE from environment or use default."""
    db_type = os.getenv("DATABASE_TYPE", "sqlite")
    logger.info(f"DATABASE_TYPE: {db_type}")
    return db_type


def get_postgres_host() -> str:
    """Fetch POSTGRES_HOST from environment or use default."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    logger.info(f"POSTGRES_HOST: {host}")
    return host


def get_postgres_port() -> int:
    """Fetch POSTGRES_PORT from environment or use default."""
    try:
        port = int(os.getenv("POSTGRES_PORT", 5432))
        logger.info(f"POSTGRES_PORT: {port}")
        return port
    except ValueError:
        logger.error("Invalid value for POSTGRES_PORT; using default (5432).")
        return 5432


def get_postgres_db() -> str:
    """Fetch POSTGRES_DB from environment or use default."""
    db = os.getenv("POSTGRES_DB", "postgres_buzz_database")
    logger.info(f"POSTGRES_DB: {db}")
    return db


def get_postgres_user() -> str:
    """Fetch POSTGRES_USER from environment or use default."""
    user = os.getenv("POSTGRES_USER", "your_username")
    logger.info(f"POSTGRES_USER: {user}")
    return user


def get_postgres_password() -> str:
    """Fetch POSTGRES_PASSWORD from environment or use default."""
    password = os.getenv("POSTGRES_PASSWORD", "your_password")
    logger.info("POSTGRES_PASSWORD: [REDACTED]")
    return password


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Test the configuration functions
    logger.info("Testing configuration.")
    try:
        get_message_interval_seconds_as_int()
        get_base_data_path()
        get_database_type()
        get_postgres_host()
        get_postgres_port()
        get_postgres_db()
        get_postgres_user()
        get_postgres_password()
        logger.info("SUCCESS: Configuration function tests complete.")

    except Exception as e:
        logger.error(f"ERROR: Configuration function test failed: {e}")
