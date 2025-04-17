import csv
import os
import logging
import pandas as pd
import numpy as np
from typing import List
from models.user import User
from db import connect_to_db, load_sql
from metadataCheck import get_table_metadata, validate_dataframe

logger = logging.getLogger(__name__)

DB_NAME = "hydro_db"  
TABLE_NAME = "users"


def load_users_from_csv(csv_path: str) -> List[User]:
    """
    Loads and validates user data from CSV, returning a list of User objects.

    Args:
        csv_path (str): Path to the users.csv file.

    Returns:
        List[User]: Parsed and validated users ready for insertion.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        df = df.replace({np.nan: None})

        db_metadata = get_table_metadata(DB_NAME, TABLE_NAME)
        validated_df = validate_dataframe(df, db_metadata, TABLE_NAME)

        if validated_df is None:
            logger.error(f"❌ Aborting user loading due to schema mismatch.")
            return []

        users = [
            User(
                id=int(row["id"]),
                username=row["username"],
                email=row["email"],
                admin=bool(int(row["admin"])),
                password=row["password"],
                active_session=bool(int(row["activeSession"]))
            )
            for _, row in validated_df.iterrows()
        ]

        return users

    except Exception as e:
        logger.error(f"❌ Failed to load users from CSV: {e}")
        raise


def insert_users(users: List[User]):
    """
    Inserts a list of User objects into the database.

    Args:
        users (List[User]): List of users to insert.
    """
    if not users:
        logger.info("No users to insert.")
        return

    try:
        sql_template = load_sql("insert_users.sql")

        values = [
            (
                user.id,
                user.username,
                user.email,
                int(user.admin),
                user.password,
                int(user.active_session),
            )
            for user in users
        ]

        with connect_to_db() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql_template, values)
            conn.commit()
            logger.info(f"✅ Inserted {len(users)} users into database.")

    except Exception as e:
        logger.error(f"❌ Error during user insertion: {e}")
        raise
