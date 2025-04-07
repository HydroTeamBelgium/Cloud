import logging, os
from external_data_fetch import fetch_and_write_csv_json_path
from exceptions import APINotAvailableError, CSVNotCreatedError

logger = logging.getLogger(__name__)

def generate_driver_data(csv_path) -> None:
    """
    Fetches driver data from the Ergast API and writes to CSV file.
    
    Uses the `fetch_and_write_csv` function to handle the API request and CSV writing.

    The CSV file is stored in the same directory as this script.

    Can raise `APINotAvailableError` if the API is unreachable or returns an error.
    Can raise `CSVNotCreatedError` if the CSV file is not created or is empty.

    """

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    drivers_url = "https://ergast.com/api/f1/2023/drivers.json"
    filename = "drivers.csv"
   

    try:
        
        fetch_and_write_csv_json_path(drivers_url, filename, json_path=["MRData", "DriverTable", "Drivers"])
    except APINotAvailableError as e:
        logger.error(f"❌ API not available or failed to fetch data: {e}")
        raise

    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        logger.error(f"❌ CSV file was not created or is empty: {filename}")
        raise CSVNotCreatedError(f"CSV not created or is empty: {filename}")

    logger.info(f"✅ Driver data successfully written to {filename}")



def generate_events_data(csv_path) -> None:
    """
   Fetches event (race) data from the Ergast API and writes to CSV file.
    
    Uses the `fetch_and_write_csv_json_path` function to handle the API request and CSV writing.

    The CSV file is stored in the same directory as this script.

    Can raise `APINotAvailableError` if the API is unreachable or returns an error.
    Can raise `CSVNotCreatedError` if the CSV file is not created or is empty.

    """

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    drivers_url = "https://ergast.com/api/f1/2023.json"
    filename = "events.csv"
    
    try:
        
        fetch_and_write_csv_json_path(drivers_url, filename, json_path=["MRData", "RaceTable", "Races"])
    except APINotAvailableError as e:
        logger.error(f"❌ API not available or failed to fetch data: {e}")
        raise

    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        logger.error(f"❌ CSV file was not created or is empty: {filename}")
        raise CSVNotCreatedError(f"CSV not created or is empty: {filename}")

    logger.info(f"✅ Driver data successfully written to {filename}")

