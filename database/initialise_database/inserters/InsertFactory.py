import csv
from dataclasses import astuple
import os
import logging
from typing import Any, Dict, List
from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.Database import Database
from database.tools.utils import load_csv

from database.models.car_component import CarComponent
from database.models.car_version import CarVersion
from database.models.reading_end_point import ReadingEndPoint
from database.models.sensor_data import SensorData
from database.models.sensor_entity import SensorEntity
from database.models.sensor_type import SensorType
from database.models.measurement_type import MeasurementType
from database.models.sensor_type_measurement_type import SensorTypeMeasurementType
from database.models.users import Users
from database.models.roles import Roles
from database.models.drivers import Driver
from database.models.events import Event
from database.models.event_type import EventType
from database.models.weather_sensor_data import WeatherSensorData
class InsertFactory(metaclass = SingletonMeta):

    _logger: logging.Logger
    _config= Dict[str, Any]
    _database: Database

    def __init__(self):
        self._logger = LoggerFactory().get_logger(__name__)
        self._config = ConfigFactory().load_config()
        self._database = Database()

    def _resolve_insert_sql(self, entity: str) -> str:
        """Resolve the insert SQL filename based on the config prefix and entity."""
        # ALL INSERT FILES MUST BE PREFIXED WITH insert_ AND LOCATED IN THE insert/ SUBFOLDER
        sql_file = f"insert_{entity}.sql"
        sql_file_path = os.path.join(self._config.get_config_param("sql_files")["location"], "insert", sql_file)
        if not os.path.isfile(sql_file_path):
            self._logger.error(f"Insert SQL file not found: {sql_file_path}")
            raise FileNotFoundError(f"Insert SQL file not found: {sql_file_path}")
        return sql_file_path
    
    def _insert_from_csv(self, entity_filename: str, model: object) -> None:
        """
        Loads data from a CSV file and inserts it into the database using the specified SQL script.
        The CSV and SQL filenames are derived from the entity_filename parameter.
        Args:
            entity_filename (str): The base name of the CSV and SQL files (without .csv and .sql extension).
            model (object): The dataclass model to map CSV rows to.
        """
        try:
            all_rows = load_csv(entity_filename, model)
            # check here if file exists! If you check in Database.execute_query
            # and there is an error, it will give an error for every row!
            sql_name = self._resolve_insert_sql(entity_filename)
        except FileNotFoundError as e:
            self._logger.error(f"Skipping insert for {entity_filename}: {e}")
            return
        
        for row in all_rows:
            try:
                row_tuple = astuple(row)
                self._database.execute_query(sql_name, row_tuple)
            except Exception as e:
                self._logger.error(f"Insert error ({entity_filename}): data={row} error={e}")
        self._logger.info(f"✅ Inserted {len(all_rows)} {entity_filename}.")

    def insert_all_project_data(self) -> None:
        """Insert all project data from CSV files in correct foreign key order."""
        # 1. Base tables with no dependencies
        self._insert_from_csv("users", Users)
        self._insert_from_csv("roles", Roles)
        self._insert_from_csv("car_version", CarVersion)
        self._insert_from_csv("event_type", EventType)
        self._insert_from_csv("measurement_type", MeasurementType)
        self._insert_from_csv("sensor_type", SensorType)
        
        # 2. Tables depending on roles
        self._insert_from_csv("drivers", Driver)
        
        # 3. Tables depending on car_version
        self._insert_from_csv("car_components", CarComponent)
        
        # 4. Tables depending on car_components
        self._insert_from_csv("reading_end_point", ReadingEndPoint)
        
        # 5. Tables depending on drivers and event_type
        self._insert_from_csv("events", Event)
        
        # 6. Tables depending on sensor_type and reading_end_point
        self._insert_from_csv("sensor_entity", SensorEntity)
        
        # 7. Junction table depending on sensor_type and measurement_type
        self._insert_from_csv("sensor_type_measurement_type", SensorTypeMeasurementType)
        
        # 8. Data tables depending on events, sensor_entity, and measurement_type
        self._insert_from_csv("sensor_data", SensorData)
        self._insert_from_csv("weather_sensor_data", WeatherSensorData)