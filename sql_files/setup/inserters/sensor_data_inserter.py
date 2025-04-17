import os
import csv
import logging
from datetime import datetime
from db import connect_to_db, load_sql
from typing import List

logger = logging.getLogger(__name__)

def insert_all_sensor_data(sensor_files: List[str],base_path: str):
    """
    Inserts data into each sensor_<id> table based on CSV files.
    The script assumes that the CSV files are named in the format `sensor_<id>.csv`.
    Each CSV file should contain the following columns:
    - sensor_id
    - value
    - timestamp
    - event_id
    The script loads `insert_sensor_data.sql` template and replaces `{table_name}`.

    """
   

    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                template = load_sql("insert_sensor_data.sql")

                for f in sensor_files:
                    table_name = f.replace(".csv", "")
                    file_path = os.path.join(base_path, f)
                    logger.info(f"Inserting data into {table_name}...")

                    with open(file_path, newline='') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            try:
                                timestamp_str = row["timestamp"].strip()
                                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

                                
                                query = template.replace("{table_name}", table_name)
                                cursor.execute(query, (
                                    int(row["sensor_id"]),
                                    float(row["value"]),
                                    timestamp,
                                    int(row["event_id"])
                                ))
                                # logger.info(f"Inserting row: sensor={row['sensor_id']}, value={row['value']}, timestamp={timestamp}, event_id={row['event_id']}")
                            except Exception as e:
                                logger.warning(f"⚠️ Skipping row in {table_name} due to error: {e}")
                
                conn.commit()
                logger.info("✅ All sensor data inserted successfully.")

    except Exception as e:
        logger.error(f"❌ Failed to insert sensor data: {e}")
        raise
