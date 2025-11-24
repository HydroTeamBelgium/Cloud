
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.initialise_database.DataFactory import DataFactory
from common.exceptions import CSVNotCreatedError
import os

if __name__ == "__main__":
    
    config = ConfigFactory().load_config()
    data_factory = DataFactory()
    CSV_DIR = config["csv_files"]["location"]
    os.makedirs(CSV_DIR, exist_ok=True)

    logger = LoggerFactory().get_logger(__name__)
    try:
        data_factory.generate_driver_data(CSV_DIR)
        data_factory.generate_events_data(CSV_DIR)
        data_factory.generate_project_specific_csv_files(CSV_DIR)
        logger.info(f"🎉 All CSV files successfully created in: {CSV_DIR}")
    except CSVNotCreatedError as e:
        logger.error(f"❌ CSV file creation failed: {e}")
