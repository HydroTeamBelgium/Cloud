import csv
import os
import ast
import logging
from typing import List
from datetime import datetime

from models.event import Event
from db import connect_to_db, load_sql

logger = logging.getLogger(__name__)

from datetime import datetime

def load_events_from_csv(csv_path: str) -> List[Event]:
    """
    Loads event data from CSV and returns a list of Event objects.

    Args:
        csv_path (str): Path to the CSV file containing event data.
    Returns:
        List[Event]: A list of Event objects.
    Raises:
        FileNotFoundError: If the CSV file does not exist.
        Exception: If there is an error reading the CSV file or parsing data.

    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    events = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    raw_date = row["date"].strip()
                    raw_time = row["time"].strip().replace("Z", "")
                    dt = datetime.strptime(f"{raw_date} {raw_time}", "%Y-%m-%d %H:%M:%S")

                    
                    event = Event(
                        id=int(row["round"]),
                        name=row["raceName"],
                        start_date=dt,
                        end_date=dt,
                        location=row["raceName"]
                    )
                    events.append(event)

                except Exception as e:
                    logger.error(f"⚠️ Skipping event due to error: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to load events from CSV: {e}")
        raise

    return events



def insert_events(events: List[Event]):
    if not events:
        logger.info("No events to insert.")
        return

    try:
        sql_template = load_sql("insert_events.sql")

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                for event in events:
                    try:
                        #logger.info(f"Inserting event: {event.name} ({event.id})")
                        cursor.execute(sql_template, (
                            event.id,
                            event.name,
                            event.start_date,
                            event.end_date,
                            event.location
                        ))
                    except Exception as e:
                        logger.error(f"❌ Failed to insert event ID={event.id}: {e}")
                        raise
                conn.commit()
                logger.info(f"✅ Inserted {len(events)} events into database.")
    except Exception as e:
        logger.error(f"❌ Error during event insertion: {e}")
        raise
