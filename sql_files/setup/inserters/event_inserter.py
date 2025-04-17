import os
import logging
import pandas as pd
from typing import List
from datetime import datetime

from models.event import Event
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db"  
TABLE_NAME = "events"


def load_events_from_csv(csv_path: str) -> List[Event]:
    """
    Loads event data from CSV and validates schema before parsing.

    Args:
        csv_path (str): Path to the CSV file containing event data.
        
    Raises:
        FileNotFoundError: If the CSV file does not exist.
        Exception: If there is an error during the loading or validation process.
    

    Returns:
        List[Event]: Parsed event objects if schema is valid.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        df = df.replace({pd.NA: None})

        # Validate schema
        db_metadata = get_table_metadata(DB_NAME, TABLE_NAME)
        validated_df = validate_dataframe(df, db_metadata, TABLE_NAME)

        if validated_df is None:
            logger.error("❌ Schema mismatch — Aborting event load.")
            return []

        events = []
        for _, row in validated_df.iterrows():
            try:
                raw_date = str(row["date"]).strip()
                raw_time = str(row["time"]).strip().replace("Z", "")
                dt = datetime.strptime(f"{raw_date} {raw_time}", "%Y-%m-%d %H:%M:%S")

                event = Event(
                    round=int(row["round"]),
                    raceName=row["raceName"],
                    date=datetime.strptime(row["date"], "%Y-%m-%d"),  # Only date part
                    time=row["time"].replace("Z", "")  # Keep as string
                )
                events.append(event)

            except Exception as e:
                logger.warning(f"⚠️ Skipping row due to error: {e}")

        return events

    except Exception as e:
        logger.error(f"❌ Failed to load events from CSV: {e}")
        raise


def insert_events(events: List[Event]):
    """
    Inserts a list of Event objects into the database.
    Args:
        events (List[Event]): List of Event objects to insert.
    Raises:
        Exception: If there is an error during the insertion process.

    """
    if not events:
        logger.info("No events to insert.")
        return

    try:
        sql_template = load_sql("insert_events.sql")
        values = [
            (
                event.round,
                event.raceName,
                event.date,
                event.time
            )
            for event in events
        ]

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql_template, values)
            conn.commit()
            logger.info(f"✅ Inserted {len(events)} events into database.")

    except Exception as e:
        logger.error(f"❌ Error during event insertion: {e}")
        raise
