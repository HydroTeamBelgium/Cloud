import csv
import os
from typing import List, Optional, Type, TypeVar
from dataclasses import fields
from common.config import ConfigFactory
from common.logger import LoggerFactory

logger = LoggerFactory().get_logger(__name__)
config = ConfigFactory().load_config()

T = TypeVar('T')

def load_csv(filename: str, dataclass_type: Type[T]) -> List[T]:
    """
    Loads a CSV file and returns a list of dataclass instances.
    
    Args:
        filename (str): The name of the CSV file to load.
        dataclass_type (Type[T]): The dataclass type to instantiate from CSV rows.
    
    Returns:
        List[T]: A list of dataclass instances populated from the CSV file.
    
    Raises:
        FileNotFoundError: If the CSV file is not found.
    """
    if not filename.endswith(".csv"):
        filename += ".csv"

    csv_path = os.path.join(config.get_config_param("csv_files")["location"], filename)

    if not os.path.exists(csv_path):
        logger.error(f"❌ CSV file not found: {csv_path}")
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    data = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            # Get dataclass field names and types
            dataclass_fields = {f.name: f.type for f in fields(dataclass_type)}
            
            for row_num, row in enumerate(reader, start=1):
                try:
                    # Build kwargs by matching CSV columns to dataclass fields
                    kwargs = {}
                    for field_name, field_type in dataclass_fields.items():
                        value = row.get(field_name)
                        # Handle empty values
                        if not value or value.strip() == '':
                            kwargs[field_name] = None

                        # Type conversion based on field type
                        elif field_type == int or field_type == Optional[int]:
                            kwargs[field_name] = int(value)
                        elif field_type == float or field_type == Optional[float]:
                            kwargs[field_name] = float(value)
                        elif field_type == bool or field_type == Optional[bool]:
                            kwargs[field_name] = value.lower() in ('true', '1', 'yes')
                        else:
                            kwargs[field_name] = value
                    
                    instance = dataclass_type(**kwargs)
                    data.append(instance)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Skipping row {row_num} due to error: {e}")
                    
    except Exception as e:
        logger.error(f"❌ Failed to load CSV file: {e}")
        raise

    logger.info(f"✅ Successfully loaded {len(data)} records from {filename}")
    return data


def get_sensor_sql_filenames():
    config = ConfigFactory().load_config()
    base_path = config["sql_files"]["location"]
    return  [
            f for f in os.listdir(base_path)
            if f.startswith(config["sql_files"]["sensor_data"]["identifier"]) and f.endswith(".csv") and f != config["sql_files"]["sensor_data"]["entity"]
        ]