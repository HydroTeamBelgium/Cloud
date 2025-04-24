import os

import logging
from typing import Any, Dict
from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory
from database.Database import Database
from database.tools.utils import get_sensor_sql_filenames



class TableFactory(metaclass = SingletonMeta):

    _logger: logging.Logger
    _config= Dict[str, Any]

    def __init__(self):
        self._logger = LoggerFactory.get_logger(__name__)
        self._config = ConfigFactory.load_config()
        self._database = Database()

    def create_tables(self):
        self._create_general_tables()
        self._create_sensor_tables()

    def _create_general_tables(self):
        """
        Creates all tables using prewritten SQL files (including foreign keys).

        Executes in a strict order to preserve FK dependency integrity.

        The order of table creation is defined in the TABLE_ORDER list.

        Throws an exception if any table creation fails.
        """

        for filename in self._config["sql_files"]["table_creation"].values:
            try:
                self._database.execute_query(filename)
            except Exception as e:
                self._logger.error(f"Error on creating table:\n name: {filename}\n error: {e}")

    def _create_sensor_tables(self):
        """
        Creates all sensor_<id> tables based on detected CSV files,

        excluding 'sensor_entity.csv'.

        Loads `create_sensor_table.sql` template and replaces `{table_name}`.

        The script assumes that the CSV files are named in the format `sensor_<id>.csv`.
        
        """
    
        sensor_files = get_sensor_sql_filenames()

        for filename in sensor_files:
            try:
                self._database.execute_query(filename)
            except Exception as e:
                self._logger.error(f"Error on creating table:\n name: {filename}\n error: {e}")
        
