import csv
import os
import logging
from typing import List
from models.readingEndPoint import ReadingEndPoint
from db import connect_to_db, load_sql

logger = logging.getLogger(__name__)

def load_reading_endpoints_from_csv(csv_path: str) -> List[ReadingEndPoint]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    endpoints = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            logger.info(f"CSV Headers: {reader.fieldnames}")  # <--- Add this
            for row in reader:
                try:
                    endpoint = ReadingEndPoint(
                        id=int(row["id"]),
                        name=row["name"],
                        functional_group=row["functional_group"],
                        car_component=int(row["car_component"]) if row.get("car_component") else None
                    )

                    endpoints.append(endpoint)
                except Exception as e:
                    logger.warning(f"⚠️ Skipping row due to error: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to load reading_end_point data from CSV: {e}")
        raise

    return endpoints

def insert_reading_endpoints(endpoints: List[ReadingEndPoint]):
    if not endpoints:
        logger.info("No reading endpoints to insert.")
        return

    try:
        sql_template = load_sql("insert_reading_endpoints.sql")

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                for endpoint in endpoints:
                    try:
                        cursor.execute(sql_template, (
                            endpoint.id,
                            endpoint.name,
                            endpoint.functional_group,
                            endpoint.car_component
                        ))
                    except Exception as e:
                        logger.error(f"❌ Failed to insert reading endpoint ID={endpoint.id}: {e}")
                        raise
                conn.commit()
                logger.info(f"✅ Inserted {len(endpoints)} reading endpoints into database.")
    except Exception as e:
        logger.error(f"❌ Error during reading endpoint insertion: {e}")
        raise
