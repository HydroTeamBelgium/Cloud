import csv
import os
import logging
import pandas as pd
import numpy as np
from typing import List
from datetime import datetime

from models.driver import Driver
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db" 
TABLE_NAME = "drivers"


def load_drivers_from_csv(csv_path: str) -> List[Driver]:
    """
    Loads and validates driver data from CSV and returns a list of Driver objects.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        df = df.replace({np.nan: None})

        # Validate schema
        db_metadata = get_table_metadata(DB_NAME, TABLE_NAME)
        validated_df = validate_dataframe(df, db_metadata, TABLE_NAME)

        if validated_df is None:
            logger.error("❌ Schema mismatch — Aborting driver loading.")
            return []

        # Convert each row into a Driver object
        drivers = [
            Driver(
                given_name=row["given_name"],
                code=row["code"],
                date_of_birth=datetime.strptime(row["date_of_birth"], "%Y-%m-%d")
            )
            for _, row in validated_df.iterrows()
        ]

        return drivers

    except Exception as e:
        logger.error(f"❌ Failed to load drivers from CSV: {e}")
        raise


def insert_drivers(drivers: List[Driver]):
    """
    Inserts a list of Driver objects into the database.
    """
    if not drivers:
        logger.info("No drivers to insert.")
        return

    try:
        sql_template = load_sql("insert_drivers.sql")

        values = [
            (
                driver.given_name,
                driver.code,
                driver.date_of_birth
            )
            for driver in drivers
        ]

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql_template, values)
            conn.commit()
            logger.info(f"✅ Inserted {len(drivers)} drivers into database.")

    except Exception as e:
        logger.error(f"❌ Error during driver insertion: {e}")
        raise
