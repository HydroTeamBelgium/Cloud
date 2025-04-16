import csv
import os
import logging
from typing import List
from models.user import User
from db import connect_to_db, load_sql  # assumes load_sql reads from `sql/` directory

logger = logging.getLogger(__name__)

def load_users_from_csv(csv_path: str) -> List[User]:
    """
    Loads a list of User objects from a CSV file.

    Args:
        csv_path (str): Path to the users CSV file.

    Returns:
        List[User]: A list of parsed User objects.

    Raises:
        Exception: If the file cannot be opened or parsing fails.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"❌ CSV file not found: {csv_path}")

    users = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                user = User(
                    id=int(row["id"]),
                    username=row["username"],
                    email=row["email"],
                    admin=bool(int(row["admin"])),
                    password=row["password"],
                    active_session=bool(int(row["activeSession"]))
                )
                users.append(user)
    except Exception as e:
        logger.error(f"❌ Failed to load users from CSV: {e}")
        raise

    return users


def insert_users(users: List[User]):
    """
    Inserts a list of User objects into the database.

    Args:
        users (List[User]): List of users to insert.

    Raises:
        Exception: If any database operation fails.
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