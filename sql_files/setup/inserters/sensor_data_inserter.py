import os
import logging
import pandas as pd
from datetime import datetime
from typing import List
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db"  # ← replace with your actual DB name


def insert_all_sensor_data(sensor_files: List[str], base_path: str):
    """
    Inserts data into each sensor_<id> table based on CSV files.
    Args:
        sensor_files (List[str]): List of CSV file names containing sensor data.
        base_path (str): Base path where the CSV files are located.
    Raises:
        Exception: If there is an error during the database connection or insertion process.
    
    Each CSV should contain:
        - sensor_id
        - value
        - timestamp
        - event_id
    """
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                base_sql = load_sql("insert_sensor_data.sql")

                for file_name in sensor_files:
                    table_name = file_name.replace(".csv", "")
                    file_path = os.path.join(base_path, file_name)
                    logger.info(f"📥 Processing {file_path}...")

                    try:
                        df = pd.read_csv(file_path)
                        df.columns = [col.strip().lower() for col in df.columns]
                        df = df.replace({pd.NA: None, pd.NaT: None})

                        db_metadata = get_table_metadata(DB_NAME, table_name)
                        validated_df = validate_dataframe(df, db_metadata, table_name)

                        if validated_df is None:
                            logger.error(f"❌ Skipping {table_name}: schema mismatch.")
                            continue

                        # Convert timestamp column
                        validated_df["timestamp"] = pd.to_datetime(validated_df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

                        insert_query = base_sql.replace("{table_name}", table_name)
                        values = [
                            (
                                int(row["sensor_id"]),
                                float(row["value"]),
                                row["timestamp"].to_pydatetime(),
                                int(row["event_id"])
                            )
                            for _, row in validated_df.iterrows()
                            if pd.notnull(row["timestamp"]) and all(pd.notnull(row[col]) for col in ["sensor_id", "value", "event_id"])
                        ]

                        cursor.executemany(insert_query, values)
                        logger.info(f"✅ Inserted {len(values)} rows into {table_name}")

                    except Exception as e:
                        logger.error(f"❌ Error inserting data into {table_name}: {e}")

            conn.commit()
            logger.info("🎉 All sensor data inserted successfully.")
    except Exception as e:
        logger.error(f"❌ Sensor data insertion failed: {e}")
        raise
