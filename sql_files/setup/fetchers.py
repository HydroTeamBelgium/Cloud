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
    output_path = os.path.join(csv_path, filename)

    try:
        df = fetch_and_write_csv_json_path(drivers_url, filename, json_path=["MRData", "DriverTable", "Drivers"])
        
        if df is None or df.empty:
            raise CSVNotCreatedError("Returned dataframe is empty or None")

        required_columns = ["givenName", "code", "dateOfBirth"]  # <-- These are the actual JSON keys!
        df_filtered = df[required_columns]
        df_filtered.columns = ["given_name", "code", "date_of_birth"]  # Normalize column names to match your DB
        df_filtered.to_csv(output_path, index=False)

    except APINotAvailableError as e:
        logger.error(f"❌ API not available or failed to fetch data: {e}")
        raise
    except KeyError as e:
        logger.error(f"❌ One or more required columns missing: {e}")
        raise

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
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

    events_url = "https://ergast.com/api/f1/2023.json"
    filename = "events.csv"
    output_path = os.path.join(csv_path, filename)

    try:
        df = fetch_and_write_csv_json_path(events_url, filename, json_path=["MRData", "RaceTable", "Races"])
        
        if df is None or df.empty:
            raise CSVNotCreatedError("Returned dataframe is empty or None")

        required_columns = ["round", "raceName", "date", "time"]
        df_filtered = df[required_columns]
        df_filtered.to_csv(output_path, index=False)

    except APINotAvailableError as e:
        logger.error(f"❌ API not available or failed to fetch data: {e}")
        raise
    except KeyError as e:
        logger.error(f"❌ One or more required columns missing: {e}")
        raise

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise CSVNotCreatedError(f"CSV not created or is empty: {filename}")

    logger.info(f"✅ Event data successfully written to {filename}")

