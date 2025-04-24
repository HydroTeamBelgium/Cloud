
import csv
import os
from typing import Callable, List
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.models import Model


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

    logger = LoggerFactory().get_logger(__name__)
    if not filename.endswith(".sql"):
        filename += ".sql"

    # Fix: build the full path first, then check
    config = ConfigFactory().load_config()
    sql_path = os.path.join(config["sql_files"]["location"], filename)

    if not os.path.isfile(sql_path):
        for root, _, files in os.walk(config["sql_files"]["location"]):
            if filename in files:
                sql_path = root + "/" + filename
                break
    if not os.path.isfile(sql_path):
        logger.error(f"Could not find any sql file called: {filename}")
        
    with open(sql_path, "r") as f:
        return f.read().strip()
    
def load_csv(filename: str, object: Callable) -> List[object]:
    
    logger = LoggerFactory().get_logger(__name__)
    if not filename.endswith(".csv"):
        filename += ".csv"

    # Fix: build the full path first, then check

    config = ConfigFactory().load_config()
    csv_path = os.path.join(config["csv_files"]["location"], filename)

    if not os.path.exists(csv_path):
        logger.error(f"❌ CSV file not found: {csv_path}")
        return

    data = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            logger.info(f"CSV Headers: {reader.fieldnames}")  # <--- Add this
            for row in reader:
                try:
                    component = object(
                        id=int(row["id"]),
                        semantic_type=row["semantic_type"],  # fixed key name
                        manufacturer=row["manufacturer"] or None,
                        serial_number=row["serial_number"] or None,
                        parent_component=int(float(row["parent_component"])) if row.get("parent_component") else None
                    )
                    data.append(component)
                except Exception as e:
                    logger.info(
                        f"Inserting row: id={row['id']}, semantic_type={row.get('semanticType')}, "
                        f"manufacturer={row.get('manufacturer')}, serial_number={row.get('serialNumber')}, "
                        f"parent_component={row.get('parentComponent')}"
                    )
                    logger.warning(f"⚠️ Skipping row due to error: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to load car components: {e}")
        raise

    return data


def get_sensor_sql_filenames():
    config = ConfigFactory().load_config()
    base_path = config["sql_files"]["location"]
    return  [
            f for f in os.listdir(base_path)
            if f.startswith(config["sql_files"]["sensor_data"]["identifier"]) and f.endswith(".csv") and f != config["sql_files"]["sensor_data"]["entity"]
        ]