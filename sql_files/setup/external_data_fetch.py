# external_data_utils.py
import requests
import pandas as pd
import os
import logging
logger = logging.getLogger(__name__)

def fetch_and_write_csv(api_url: str, filename: str, limit: int = None):
    """
    Fetches data from a given API URL and writes it to a CSV file.
    Args:
        api_url (str): The API URL to fetch data from.
        filename (str): The name of the CSV file to write the data to.
        limit (int, optional): The maximum number of records to write. Defaults to None.

    Raises:
        Exception: If the API request fails or if the data is empty.

        
    To be used when the API returns a flat JSON structure.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"⚠️ No data received from {api_url}")
            return

        # Limit number of records (optional)
        if limit:
            data = data[:limit]

        df = pd.DataFrame(data)
        if df.empty:
            print(f"⚠️ No valid data to write for {filename}")
            return

        csv_dir = os.path.abspath(os.path.dirname(__file__))
        output_path = os.path.join(csv_dir, filename)
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(df)} rows to {filename}")

    except Exception as e:
        print(f"❌ Error fetching or writing {filename}: {e}")


from exceptions import APINotAvailableError


def fetch_and_write_csv_json_path(url, filename, json_path):
    """
    Fetches JSON data from a URL, extracts the list at the given JSON path,
    converts it to a DataFrame and returns it.

    Args:
        url (str): The API endpoint.
        filename (str): Name of CSV file (optional).
        json_path (list[str]): Path to the data inside the JSON.

    Returns:
        pd.DataFrame: Extracted DataFrame from API data.
    """
    try:
        logger.info(f"🌐 Fetching data from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Follow the JSON path to reach the array of records
        for key in json_path:
            data = data.get(key, {})

        if not isinstance(data, list):
            logger.error("❌ Final data is not a list")
            return None

        df = pd.DataFrame(data)
        logger.info(f"📄 Retrieved {len(df)} rows")

        return df

    except requests.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        return None
    except ValueError as e:
        logger.error(f"❌ Failed to parse JSON: {e}")
        return None