import mysql.connector
from typing import List, str, Dict, Any, Optional, Tuple, Bool
import logging


from SQL_files.sql_helper_functions import read_sql_file
from common.LoggerSingleton import SingletonMeta
from common.ConfigWrapper import ConfigWrapper

logger = logging.getLogger(__name__)

class Database (metaclass=SingletonMeta):
    """
    API information on Notion:
    https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4
    """
    
    def __init__(self, config:ConfigWrapper, database_engine:str) -> None:
        """
        Initializes the database object and creates a connection.
        The SingletonMeta metaclass ensures there's only one instance of a Database object
        
        Args:
            config (ConfigWrapper): A ConfigWrapper instance, which represents a database configuration, read from a yaml file.
                                    A configuration should first be instanciated by the ConfigFactory, after which it can be used as a ConfigWrapper instance.
            database_enginge (str): the 
        """
        self._config = config
        self._db_engine = database_engine        
    
    def _connect(self) -> None:
        "Establishes a connection to the database"
        try:
            self._connection = mysql.connector.connect(
                self._config.get_SQL_instance_connection_name(),  # Cloud SQL Instance Connection Name
                "pymysql",
                user=self._config.get_database_role_user_name(),
                password=self._config.get_database_role_password(),
                db=self._config.get_database_name(),
                ip_type=self._IPTypes.PUBLIC  # IPTypes.PRIVATE for private IP
            )
            logger.info("✅ Database connection established.")
            return self._connection
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def disconnect(self) -> None:
        "Disconnects the connection with the database"
        try:
            if self._connection and self.connection.is_connected():
                self._connection.close()
                logger.info("✅ Database connection closed successfully.")
            else:
                logger.info("There's no active database connection to close.")
        except Exception as e:
            logger.error(f"❌ Disconnection was not succesful.")
            raise

    
    def is_connected(self) -> bool:
        """
        Checks wether there's a connection to the database
        returns true if connected, else false
        """  
        pass
        
    def execute_query_path(self, query_file_path: str, params: tuple) -> List[Dict[str, Any]]:
        """
        Executes a raw SQL query on the connected database
        
        query (str): the raw SQL query. This query will be extracted from an SQL file.
                     Use of ‘typed’ queries (i.e. not extracted from an SQL file) are strongly not advised.
        params (tuple): Prevents SQL injection (see API on Notion - https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4#1b9ed9807d5880a9881ff95f720e5f4c)
        """
        pass
