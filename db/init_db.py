from db_manager import DBManager
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    """Initialize the database with required tables"""
    db = DBManager()
    try:
        logger.info("Connecting to database...")
        db.connect()
        db.init_database()
        logger.info("Database initialized successfully")

    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    load_dotenv()  # Load environment variables
    init_database()
