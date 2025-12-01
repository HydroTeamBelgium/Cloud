from common.logger import LoggerFactory
from database.initialise_database.create_csv_files.DataFactory import DataFactory
from common.exceptions import CSVNotCreatedError

if __name__ == "__main__":
    data_factory = DataFactory()

    logger = LoggerFactory().get_logger(__name__)
    try:
        data_factory.generate_project_specific_csv_files()
        logger.info(f"🎉 All CSV files successfully created in: {data_factory._csv_dir}")
    except CSVNotCreatedError as e:
        logger.error(f"❌ CSV file creation failed: {e}")
