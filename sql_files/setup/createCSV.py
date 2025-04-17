import logging
import os
from fetchers import generate_driver_data, generate_events_data
from generators import generate_project_specific_csv_files
from exceptions import CSVNotCreatedError
from constants import CSV_DIR
from sensorsList import create_and_seed_sensor_entity


if __name__ == "__main__":
    

    os.makedirs(CSV_DIR, exist_ok=True)

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        create_and_seed_sensor_entity()
        generate_driver_data(CSV_DIR)
        generate_events_data(CSV_DIR)
        generate_project_specific_csv_files(CSV_DIR)
        logger.info(f"🎉 All CSV files successfully created in: {CSV_DIR}")
    except CSVNotCreatedError as e:
        logger.error(f"❌ CSV file creation failed: {e}")
