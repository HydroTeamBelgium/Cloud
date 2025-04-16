import csv
import os
import logging
from typing import List
from datetime import datetime

from models.driver import Driver
from db import connect_to_db, load_sql

logger = logging.getLogger(__name__)

def load_drivers_from_csv(csv_path: str) -> List[Driver]:
    """
    Loads driver data from a CSV into Driver objects.
    Raises Exception if anything fails.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    drivers = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                driver = Driver(
                    id=row["driverId"],
                    permanent_number=int(row["permanentNumber"]),
                    code=row["code"],
                    url=row["url"],
                    given_name=row["givenName"],
                    family_name=row["familyName"],
                    date_of_birth=datetime.strptime(row["dateOfBirth"], "%Y-%m-%d"),
                    nationality=row["nationality"]
                )
                drivers.append(driver)
    except Exception as e:
        logger.error(f"❌ Failed to load drivers from CSV: {e}")
        raise

    return drivers


def insert_drivers(drivers: List[Driver]):
    """
    Inserts a list of Driver objects into the database.
    """
    if not drivers:
        logger.info("No drivers to insert.")
        return

    try:
        sql_template = load_sql("insert_drivers.sql")

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                for driver in drivers:
                    try:
                       logger.info(f"Inserting driver: {driver.id}, {driver.given_name}, {driver.code}, {driver.date_of_birth}")

                       cursor.execute(sql_template, (
                            driver.given_name,
                            driver.code,
                            driver.date_of_birth
                        ))
                    except Exception as e:
                        logger.error(f"❌ Failed to insert driver ID={driver.id}: {e}")
                        raise
                conn.commit()
                logger.info(f"✅ Inserted {len(drivers)} drivers into database.")
    except Exception as e:
        logger.error(f"❌ Error during driver insertion: {e}")
        raise