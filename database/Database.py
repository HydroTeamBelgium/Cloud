import mysql.connector
from typing import List, str, Dict, Any, Optional, Tuple, Bool

from common.Singleton import SingletonMeta
from common.config import ConfigFactory

class Database(metaclass=SingletonMeta):
    """
    API information on Notion:
    https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4
    """
    
    _config: Dict[str,Any]

    def __init__(self) -> None:
        """
        Initializes the database object and creates a connection.
        """
        self._config = ConfigFactory().load_config()
    
    def _connect(self) -> None:
        "Establishes a connection to the database"
        pass
    
    def disconnect(self) -> None:
        "Disconnects the connection with the database"
        pass
    
    def is_connected(self) -> bool:
        """
        Checks wether there's a connection to the database
        returns true if connected, else false
        """  
        pass
        
    def execute_query(self, query_file_name: str, params: tuple) -> List[Dict[str, Any]]:
        """
        Executes a raw SQL query on the connected database
        
        args:
            query_file_name (str): the filename of the sql file. Works both with including .sql and without
        
            params (tuple): Prevents SQL injection (see API on Notion - https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4#1b9ed9807d5880a9881ff95f720e5f4c)
        """
        pass






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
        
        