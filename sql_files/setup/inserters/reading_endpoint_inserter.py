import os
import logging
import pandas as pd
from typing import List

from models.readingEndPoint import ReadingEndPoint
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db"  # Replace with your DB name
TABLE_NAME = "reading_end_point"


def load_reading_endpoints_from_csv(csv_path: str) -> List[ReadingEndPoint]:
    """
    Loads and validates reading_end_point data from CSV.

    Returns:
        List[ReadingEndPoint]: A list of validated endpoint objects.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        df = df.replace({pd.NA: None})
        df.rename(columns={
            "functional_group": "functionalGroup",
            "car_component": "carComponent"
        }, inplace=True)

        db_metadata = get_table_metadata(DB_NAME, TABLE_NAME)
        validated_df = validate_dataframe(df, db_metadata, TABLE_NAME)

        if validated_df is None:
            logger.error("❌ Schema mismatch — Aborting endpoint load.")
            return []

        endpoints = []
        for _, row in validated_df.iterrows():
            try:
                endpoint = ReadingEndPoint(
                    id=int(row["id"]),
                    name=row["name"],
                    functional_group=row["functionalGroup"], 
                    car_component=int(row["carComponent"]) if pd.notnull(row["carComponent"]) else None
                )

                endpoints.append(endpoint)
            except Exception as e:
                logger.warning(f"⚠️ Skipping row due to error: {e}")

        return endpoints

    except Exception as e:
        logger.error(f"❌ Failed to load reading_end_point data from CSV: {e}")
        raise


def insert_reading_endpoints(endpoints: List[ReadingEndPoint]):
    """
    Inserts validated reading_end_point objects into the database.
    """
    if not endpoints:
        logger.info("No reading endpoints to insert.")
        return

    try:
        sql_template = load_sql("insert_reading_endpoints.sql")
        values = [
            (
                ep.id,
                ep.name,
                ep.functional_group,
                ep.car_component
            )
            for ep in endpoints
        ]

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql_template, values)
            conn.commit()
            logger.info(f"✅ Inserted {len(endpoints)} reading endpoints into database.")

    except Exception as e:
        logger.error(f"❌ Error during reading endpoint insertion: {e}")
        raise
