import os
import logging
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv

# import pycouchdb

# Load environment variables from .env file
load_dotenv()

# CouchDB Configurations
class CouchDb:
    db_name = os.getenv("COUCH_DB_DBNAME")
    username = os.getenv("COUCH_DB_USERNAME")
    password = os.getenv("COUCH_DB_PASSWORD")
    endpoint = os.getenv("COUCH_DB_ENDPOINT").replace("http://", "").replace("https://", "")

# Minio Configurations
class MinioDb:
    endpoint = os.getenv("MINIO_URL").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

# Folder paths
UPLOAD_FOLDER = "./uploads"
LOGS_FOLDER = "./logs"

def setup_logging():
    """
    Set up logging for the application.
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create a logger object
    logger = logging.getLogger("parsing_service")

    # Set the log level (DEBUG in this case)
    logger.setLevel(logging.DEBUG)

    # Create the handler with rotation at midnight and keep 1 backup
    handler = TimedRotatingFileHandler(
        f"{LOGS_FOLDER}/service.log", when="midnight", backupCount=1
    )

    # Formatter for log messages
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger


