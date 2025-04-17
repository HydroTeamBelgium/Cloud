import os
import logging
import pandas as pd
from typing import List

from models.carComponent import CarComponent
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db"
TABLE_NAME = "car_components"


def load_car_components_from_csv(csv_path: str) -> List[CarComponent]:
    """
    Loads and validates car component data from CSV and returns a list of CarComponent objects.
    Args:
        csv_path (str): Path to the CSV file containing car component data.
    Raises:
        FileNotFoundError: If the CSV file does not exist.
        Exception: If there is an error during the loading or validation process.
    Returns:
        List[CarComponent]: A list of validated car component objects.
    """
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        df = df.replace({pd.NA: None})

        # Rename to match DB column names
        df.columns = [
            "id", "semanticType", "manufacturer", "serialNumber", "parentComponent"
        ]

        db_metadata = get_table_metadata(DB_NAME, TABLE_NAME)
        validated_df = validate_dataframe(df, db_metadata, TABLE_NAME)

        if validated_df is None:
            logger.error("❌ Schema mismatch — Aborting component load.")
            return []

        components = [
            CarComponent(
                id=int(row["id"]),
                semantic_type=row["semanticType"],
                manufacturer=row["manufacturer"],
                serial_number=row["serialNumber"],
                parent_component=int(row["parentComponent"]) if not pd.isna(row["parentComponent"]) else None
            )
            for _, row in validated_df.iterrows()
        ]

        return components

    except Exception as e:
        logger.error(f"❌ Failed to load car components: {e}")
        raise



def insert_car_components(components: List[CarComponent]) -> None:
    """
    Inserts car components into the database.
    Args:
        components (List[CarComponent]): List of car component objects to insert.
    Raises:
        Exception: If there is an error during the insertion process.

    Returns:
        None
    """
    if not components:
        logger.info("No car components to insert.")
        return

    try:
        sql_template = load_sql("insert_car_components.sql")
        values = [
            (
                c.id,
                c.semantic_type,
                c.manufacturer,
                c.serial_number,
                c.parent_component
            ) for c in components
        ]

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql_template, values)
            conn.commit()
            logger.info(f"✅ Inserted {len(components)} car components into database.")
    except Exception as e:
        logger.error(f"❌ Error during car component insertion: {e}")
        raise
