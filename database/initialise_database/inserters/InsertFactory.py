import csv
from dataclasses import astuple
import os
import logging
from typing import Any, Dict, List
from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.Database import Database
from database.models.ReadingEndPoint import ReadingEndPoint
from database.models.Event import Event
from database.models.carComponent import CarComponent
from database.models.driver import Driver
from database.models.sensorData import SensorData
from database.tools.utils import load_csv
from models.user import User


class InsertFactory(metaclass = SingletonMeta):

    _logger: logging.Logger
    _config= Dict[str, Any]

    def __init__(self):
        self._logger = LoggerFactory.get_logger(__name__)
        self._config = ConfigFactory.load_config()
        self._database = Database()



    def insert_users(self, filename: str):
        """
        Inserts a list of User objects into the database.

        Args:
            users (List[User]): List of users to insert.

        Raises:
            Exception: If any database operation fails.
        """

        users = load_csv(filename, User)

        for user in users:
            try:
                self._database(self._config["sql_files"]["insert_sripts"]["users"], astuple(user))
            except Exception as e:
                self._logger.error(f"An error has occurerd trying to insert a user:\n data: {user} \n error: {e}")

        self._logger.info("✅ All sensor data inserted successfully.")


    def insert_all_sensor_data(self, sensor_files: List[str]):

        for filename in sensor_files:
            
            self._logger.info(f"Inserting data into {filename}...")
            sensor_data = load_csv(filename, SensorData)

            for data in sensor_data:
                try:
                    self._database(self._config["sql_files"]["insert_sripts"]["sensors"], astuple(data))
                except Exception as e:
                    self._logger.error(f"An error has occurerd trying to insert a sensor Data:\n data: {data} \n filename: {filename} \n error: {e}")
        
        self._logger.info("✅ All sensor data inserted successfully.")

    def insert_reading_endpoints(self, filename: str):

        endpoints = load_csv(filename, ReadingEndPoint)

        for endpoint in endpoints:
            try:
                self._database(self._config["sql_files"]["insert_sripts"]["endpoints"], astuple(endpoint))
            except Exception as e:
                self._logger.error(f"An error has occurerd trying to insert a endpoint:\n data: {endpoint} \n error: {e}")

        self._logger.info("✅ All sensor data inserted successfully.")

    def insert_events(self, filename: str):

        events = load_csv(filename, Event)

        for event in events:
            try:
                self._database(self._config["sql_files"]["insert_sripts"]["events"], astuple(event))
            except Exception as e:
                self._logger.error(f"An error has occurerd trying to insert a event:\n data: {event} \n error: {e}")

        self._logger.info("✅ All sensor data inserted successfully.")

    def insert_drivers(self, filename: str):

        drivers = load_csv(filename, Driver)

        for driver in drivers:
            try:
                self._database(self._config["sql_files"]["insert_sripts"]["drivers"], astuple(driver))
            except Exception as e:
                self._logger.error(f"An error has occurerd trying to insert a driver:\n data: {driver} \n error: {e}")

        self._logger.info("✅ All sensor data inserted successfully.")

    def insert_car_components(self, filename: str):

        components = load_csv(filename, CarComponent)

        for component in components:
            try:
                self._database(self._config["sql_files"]["insert_sripts"]["components"], astuple(component))
            except Exception as e:
                self._logger.error(f"An error has occurerd trying to insert a component:\n data: {component} \n error: {e}")

        self._logger.info("✅ All sensor data inserted successfully.")

           
