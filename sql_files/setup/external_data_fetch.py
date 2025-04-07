# external_data_utils.py
import requests
import pandas as pd
import os

def fetch_and_write_csv(api_url: str, filename: str, limit: int = None):
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

def fetch_and_write_csv_json_path(api_url: str, filename: str, json_path: list[str]):
    """
    Fetches data from a given API URL, navigates to a specific JSON path,
    and writes the data to a CSV file.
    Args:
        api_url (str): The API URL to fetch data from.
        filename (str): The name of the CSV file to write the data to.
        json_path (list[str]): A list of keys to navigate through the JSON response.
    Raises:
        APINotAvailableError: If the API request fails or returns an error.
    
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        # Navigate to nested data using json_path
        for key in json_path:
            data = data.get(key, {})

        if not isinstance(data, list) or len(data) == 0:
            raise APINotAvailableError("No data found at specified path")

        df = pd.DataFrame(data)
        csv_path = os.path.join(os.path.dirname(__file__), filename)
        df.to_csv(csv_path, index=False)

    except requests.RequestException as e:
        raise APINotAvailableError(f"API request failed: {e}")