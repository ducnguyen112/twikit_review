import pandas as pd
import sys
import logging
from db.db_manager import DBManager
from typing import List, Dict
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 1000) -> List[pd.DataFrame]:
    """Split dataframe into chunks for batch processing"""
    return np.array_split(df, len(df) // chunk_size + 1)
def extract_id(data_raw) -> str:
    import json
    data = json.loads(data_raw)
    return data['author']['identifier']
def extract_url(data_raw) -> str:
    import json
    data = json.loads(data_raw)
    return data['author']['url']
def insert_persons_from_csv(csv_path: str):
    """Insert person records from a CSV file using pandas"""
    try:
        # Initialize database connection
        db = DBManager()
        db.connect()

        # Read CSV file into pandas DataFrame
        logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        df['id'] = df['data'].apply(extract_id)
        df['source_url'] = df['data'].apply(extract_url)      
        # Verify required column exists
        if 'id' not in df.columns:
            logger.error("CSV file must contain 'id' column")
            sys.exit(1)
        
        # Remove any rows with null IDs
        df = df.dropna(subset=['id'])
        total_rows = len(df)
        logger.info(f"Found {total_rows} valid records in CSV file")

        # Process in chunks for better memory management
        chunks = chunk_dataframe(df)
        processed = 0
        errors = 0

        for chunk_idx, chunk in enumerate(chunks):
            try:
                # Process each row in the chunk
                for _, row in chunk.iterrows():
                    try:
                        # Add person to database
                        db.add_person(
                            person_id=str(row['id']),  # Ensure ID is string
                            source_url=row['source_url'],
                            data=row['data'],
                            status_code=200,
                            slug=row['slug'],
                        )
                        processed += 1
                    except Exception as e:
                        logger.error(f"Error processing ID {row['id']}: {str(e)}")
                        errors += 1
                        continue

                # Log progress after each chunk
                logger.info(f"Processed chunk {chunk_idx + 1}/{len(chunks)} ({processed}/{total_rows} records)")

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1}: {str(e)}")
                continue

        # Log final statistics
        logger.info(f"Processing complete:")
        logger.info(f"- Total records: {total_rows}")
        logger.info(f"- Successfully processed: {processed}")
        logger.info(f"- Errors: {errors}")

    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":

    csv_path = "/Users/duc/VsCode/twikit_crawler/200_twitter_init.csv"
    insert_persons_from_csv(csv_path)