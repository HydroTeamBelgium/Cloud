import mysql.connector
from typing import List, Dict, Any, Optional
import os

from common.Singleton import SingletonMeta
from common.config import ConfigFactory
from common.logger import LoggerFactory

class Database(metaclass=SingletonMeta):
    """
    API information on Notion:
    https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4
    """
    
    _config: Dict[str,Any]
    _connection: Optional[mysql.connector.MySQLConnection]

    def __init__(self) -> None:
        """
        Initializes the database object and creates a connection.
        """
        self._config = ConfigFactory().load_config()
        self._logger = LoggerFactory().get_logger(__name__)
        self._connection = None
        self._connect()
    
    def _connect(self) -> None:
        """Establishes a connection to the database"""
        try:
            if self._connection is None or not self._connection.is_connected():
                credentials = self._config.get_config_param("credentials")
                self._connection = mysql.connector.connect(**credentials)
                self._logger.info("✅ Database connection established.")
        except Exception as e:
            self._logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def disconnect(self) -> None:
        """Disconnects the connection with the database"""
        try:
            if self._connection and self._connection.is_connected():
                self._connection.close()
                self._logger.info("✅ Database connection closed successfully.")
        except Exception as e:
            self._logger.error(f"❌ Database connection was not closed: {e}")
            raise
    
    def is_connected(self) -> bool:
        """
        Checks whether there's a connection to the database
        returns true if connected, else false
        """  
        return self._connection is not None and self._connection.is_connected() # this is a mysql python method, it pings the underlying server to check activity
        
    def execute_query(self, query_file_name: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Executes a raw SQL query on the connected database
        
        args:
            query_file_name (str): the filename of the sql file. Works both with including .sql and without
        
            params (tuple): Prevents SQL injection (see API on Notion - https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4#1b9ed9807d5880a9881ff95f720e5f4c)

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the results of the query or a list of a Dict with key "affected rows" and value the number of affected rows
        """
        self._connect()  # Ensure connection is active
        
        # Add .sql extension if not present
        if not query_file_name.endswith('.sql'):
            query_file_name += '.sql'
        
        # Read SQL file
        if not os.path.exists(query_file_name):
            raise FileNotFoundError(f"SQL file not found: {query_file_name}")
        
        # Only issue is that it reads the file for every row in the csv
        with open(query_file_name, 'r') as f:
            sql_query = f.read().strip()
        
        try:
            with self._connection.cursor(dictionary=True) as cursor:
                cursor.execute(sql_query, params)
                
                # For SELECT queries
                if cursor.description:
                    result = cursor.fetchall()
                else:
                    # For INSERT/UPDATE/DELETE queries
                    self._connection.commit()
                    result = [{"affected_rows": cursor.rowcount}]
                
                self._logger.debug(f"✅ Query executed successfully: {query_file_name}")
                self._logger.debug(f"🔍 Results: {result}")
                return result
                
        except mysql.connector.Error as e:
            self._logger.error(f"❌ Query execution failed: {e}")
            self._connection.rollback()
            raise
        except Exception as e:
            self._logger.error(f"❌ Unexpected error during query execution: {e}")
            self._connection.rollback()
            raise






"""
USEFULL CODE SNIPPETS
_______________________________________________________________________
with mysql.connector.connect(**config["credentials"]) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(sql_query)
                return cursor.fetchall()
_______________________________________________________________________
def connect_to_db():

    Establishes a connection to the database using config.yaml.
    
    Returns:
        A MySQL connection object.

    Raises:
        Error: If the connection fails.
    

    try:
        config = ConfigFactory().load_config()
        connection = mysql.connector.connect(**config["credentials"])
        logger.info("✅ Database connection established.")
        return connection
    except Error as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise
_______________________________________________________________________



"""
        
        