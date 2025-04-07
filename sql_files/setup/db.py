import mysql.connector
import yaml
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


def load_sql(filename: str) -> str:
    """
    Loads an SQL file from the sql directory.
    Args:
        filename (str): The name of the SQL file to load.
    Returns:
        str: The SQL query as a string.
    Raises:
        FileNotFoundError: If the SQL file is not found.
    """
    if not filename.endswith(".sql"):
        raise ValueError("Filename must end with .sql")
    if not os.path.isfile(filename):
        raise FileNotFoundError(f"SQL file {filename} not found")
    sql_path = os.path.join(os.path.dirname(__file__), "sql", filename)
    with open(sql_path, "r") as f:
        return f.read().strip()


def load_db_config() -> dict:
    """
    Loads the database configuration from a YAML file.

    Returns:
        dict: A dictionary containing the database connection details.

    Raises:
        FileNotFoundError: If the YAML file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)["database"]

def fetch_sensors_from_db() -> List[Dict]:
    """
    Fetches sensor records using an external SQL file.
    
    SQL file used:
        sql/fetch_sensors.sql

    Returns:
        List[Dict]: Each with 'id' and 'sensor_table'.

    Raises:
        Exception: If DB connection or SQL execution fails.
    """
    try:
        config = load_db_config()
        sql_query = load_sql("fetch_sensors.sql")

        with mysql.connector.connect(**config) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(sql_query)
                return cursor.fetchall()

    except Exception as e:
        logger.error(f"❌ Failed to fetch sensors from DB: {e}")
        raise
