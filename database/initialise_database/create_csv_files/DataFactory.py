from dataclasses import asdict
from datetime import datetime, timedelta
import logging, os
import numpy as np
import pandas as pd
from typing import Any, Dict, Optional

import requests
from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory
from common.exceptions import APINotAvailableError, CSVNotCreatedError
from database.Database import Database
from database.models.carComponent import CarComponent
from database.models.readingEndPoint import ReadingEndPoint
from database.models.sensorData import SensorData
from database.models.sensorEntity import SensorEntity
from database.models.user import User

class DataFactory(metaclass = SingletonMeta):

    _logger: logging.Logger
    _config= Dict[str, Any]

    def __init__(self):
        self._logger = LoggerFactory.get_logger(__name__)
        self._config = ConfigFactory.load_config()

    def generate_driver_data(self, csv_path) -> None:
        """
        Fetches driver data from the Ergast API and writes to CSV file.
        
        Uses the `fetch_and_write_csv` function to handle the API request and CSV writing.

        The CSV file is stored in the same directory as this script.

        Can raise `APINotAvailableError` if the API is unreachable or returns an error.
        Can raise `CSVNotCreatedError` if the CSV file is not created or is empty.

        """

        drivers_url = self._config["fetch_data"]["drivers"]["url"]
        filename = self._config["fetch_data"]["drivers"]["filename"]
        json_path = self._config["fetch_data"]["drivers"]["json_path"]

        try:
            
            self._fetch_and_write_csv_json_path(drivers_url, filename, json_path=json_path)
        except APINotAvailableError as e:
            self._logger.error(f"❌ API not available or failed to fetch data: {e}")
            raise

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            self._logger.error(f"❌ CSV file was not created or is empty: {filename}")
            raise CSVNotCreatedError(f"CSV not created or is empty: {filename}")

        self._logger.info(f"✅ Driver data successfully written to {filename}")



    def generate_events_data(self, csv_path) -> None:
        """
        Fetches event (race) data from the Ergast API and writes to CSV file.
        
        Uses the `fetch_and_write_csv_json_path` function to handle the API request and CSV writing.

        The CSV file is stored in the same directory as this script.

        Can raise `APINotAvailableError` if the API is unreachable or returns an error.
        Can raise `CSVNotCreatedError` if the CSV file is not created or is empty.

        """

        events_url = self._config["fetch_data"]["events"]["url"]
        filename = self._config["fetch_data"]["events"]["filename"]
        json_path = self._config["fetch_data"]["events"]["json_path"]
        
        try:
            
            self._fetch_and_write_csv_json_path(events_url, filename, json_path=json_path)
        except APINotAvailableError as e:
            self._logger.error(f"❌ API not available or failed to fetch data: {e}")
            raise

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            self._logger.error(f"❌ CSV file was not created or is empty: {filename}")
            raise CSVNotCreatedError(f"CSV not created or is empty: {filename}")

        self._logger.info(f"✅ Driver data successfully written to {filename}")

    
    def generate_project_specific_csv_files(self, csv_dir):
        """
        Generates CSV files for project-specific tables using model classes.

        These tables include users, car components, reading end points, and sensor entities.
        Also generates synthetic sensor_<id>.csv files using randomized data.

        Raises:
            CSVNotCreatedError: if any expected CSV file is not created or is empty.
        """
        try:
            logger = LoggerFactory.get_logger()
            database = Database()

            logger.info("Generating users.csv")
            users = [
                User(i, f"driver{i}", f"driver{i}@example.com", 0, "hashed_pw", np.random.choice([0, 1]))
                for i in range(1, 11)
            ]
            df_users = pd.DataFrame([asdict(user) for user in users])
            users_csv = os.path.join(csv_dir, "users.csv")
            df_users.to_csv(users_csv, index=False)
            if not os.path.exists(users_csv) or os.path.getsize(users_csv) == 0:
                raise CSVNotCreatedError("users.csv not created or is empty")

            logger.info("Generating car_components.csv")
            components = [
                CarComponent(i, f"Component {i}", f"Manufacturer {i}", f"SN-{1000+i}",
                            np.random.choice(range(1, i)) if i > 1 and np.random.random() > 0.3 else None)
                for i in range(1, 11)
            ]
            df_components = pd.DataFrame([asdict(c) for c in components])
            components_csv = os.path.join(csv_dir, "car_components.csv")
            df_components.to_csv(components_csv, index=False)
            if not os.path.exists(components_csv) or os.path.getsize(components_csv) == 0:
                raise CSVNotCreatedError("car_components.csv not created or is empty")

            logger.info("Generating reading_end_point.csv")
            endpoints = [
                ReadingEndPoint(i, f"Endpoint {i}", f"Group {i}", np.random.randint(1, 10))
                for i in range(1, 6)
            ]
            df_endpoints = pd.DataFrame([asdict(ep) for ep in endpoints])
            endpoints_csv = os.path.join(csv_dir, "reading_end_point.csv")
            df_endpoints.to_csv(endpoints_csv, index=False)
            if not os.path.exists(endpoints_csv) or os.path.getsize(endpoints_csv) == 0:
                raise CSVNotCreatedError("reading_end_point.csv not created or is empty")

            logger.info("Generating sensor_entity.csv")
            sensors = [
                SensorEntity(i, f"SNR-{2000+i}",
                            f"{np.random.randint(2015, 2023)}-{np.random.randint(1, 12):02d}-{np.random.randint(1, 28):02d}",
                            np.random.randint(1, 5), np.random.randint(1, 5), f"sensor_{i}")
                for i in range(1, 6)
            ]
            df_sensors = pd.DataFrame([asdict(s) for s in sensors])
            sensors_csv = os.path.join(csv_dir, "sensor_entity.csv")
            df_sensors.to_csv(sensors_csv, index=False)
            if not os.path.exists(sensors_csv) or os.path.getsize(sensors_csv) == 0:
                raise CSVNotCreatedError("sensor_entity.csv not created or is empty")

            logger.info("Generating sensor_<id>.csv files")
            sensors_from_db = database.execute_query("fetch_sensors")

            events_csv = os.path.join(csv_dir, "events.csv")
            if not os.path.exists(events_csv):
                raise CSVNotCreatedError("events.csv not found. Generate it first with generate_events_data().")
            df_events = pd.read_csv(events_csv)


            """
            Generates synthetic sensor data for testing purposes and saves it to CSV.
            
            Each sensor gets a unique CSV file with random values, timestamps, and event links.

            """


            for sensor in sensors_from_db:
                sensor_data = []
                for event in asdict(df_events):
                    num_samples = np.random.randint(10, 20)
                    for _ in range(num_samples):
                        timestamp = datetime.strptime(event["date"], "%Y-%m-%d") + timedelta(minutes=np.random.randint(1, 120))
                        data = SensorData(sensor["id"], round(np.random.uniform(0.5, 100.0), 2),
                                        timestamp.strftime("%Y-%m-%d %H:%M:%S"), event["round"])
                        sensor_data.append(asdict(data))

                df_sensor_data = pd.DataFrame(sensor_data)
                sensor_csv_path = os.path.join(csv_dir, f"{sensor['sensor_table']}.csv")
                df_sensor_data.to_csv(sensor_csv_path, index=False)
                if not os.path.exists(sensor_csv_path) or os.path.getsize(sensor_csv_path) == 0:
                    raise CSVNotCreatedError(f"{sensor['sensor_table']}.csv not created or is empty")
                logger.info(f"✅ {sensor['sensor_table']}.csv created with {len(sensor_data)} entries")

            logger.info("✅ All project-specific CSV files successfully created.")

        except CSVNotCreatedError as e:
            logger.error(f"❌ Error generating project-specific CSV files: {e}")
            raise
        
    
    def _fetch_and_write_csv(self, api_url: str, filename: str, limit: Optional[int] = None):
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
                self._logger.error(f"⚠️ No data received from {api_url}")
                return

            # Limit number of records (optional)
            if limit:
                data = data[:limit]

            df = pd.DataFrame(data)
            if df.empty:
                self._logger.error(f"⚠️ No valid data to write for {filename} and limit = {limit}")
                return
            
            config = ConfigFactory().load_config()
            csv_dir = config["csv_files"]["location"]
            output_path = os.path.join(csv_dir, filename)
            df.to_csv(output_path, index=False)
            print(f"✅ Saved {len(df)} rows to {filename}")

        except Exception as e:
            print(f"❌ Error fetching or writing {filename}: {e}")




    def _fetch_and_write_csv_json_path(self, api_url: str, filename: str, json_path: list[str]):
        """
        Fetches data from a given API URL, navigates to a specific JSON path,
        and writes the data to a CSV file.
        Args:
            api_url (str): The API URL to fetch data from.
            filename (str): The name of the CSV file to write the data to.
            json_path (list[str]): A list of keys to navigate through the JSON response.
        Raises:
            APINotAvailableError: If the API request fails or returns an error.

            To be used when the API returns a nested JSON structure.
            The json_path should be a list of keys to navigate through the JSON response.
            For example, if the JSON response is:
            {
                "MRData": {
                    "DriverTable": {
                        "Drivers": [
                            {"id": "hamilton", "givenName": "Lewis", "familyName": "Hamilton"},
                            ...
                        ]
                    }
                }
            }
            The json_path would be ["MRData", "DriverTable", "Drivers"].
            If the data at the specified path is not a list or is empty, an error is raised.
            If the API request fails, an APINotAvailableError is raised.
            If the CSV file is not created or is empty, a CSVNotCreatedError is raised.
        
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