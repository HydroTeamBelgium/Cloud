import mysql.connector
from typing import List, str, Dict, Any, Optional, Tuple, Bool
import logging
from flask import g #TODO: is the flask logging feature, used below in _get_query() really needed, or can we just use our logging feature?


from database.sql_helper_functions import read_sql_file
from common.Singleton import SingletonMeta
from common.ConfigWrapper import ConfigWrapper
from custom_exceptions import QueryConstructionError

logger = logging.getLogger(__name__)
DEBUG = False #TODO: this debug boolean should probably be somewhere else in the codebase, replace it and import it in this file

class Database (metaclass=SingletonMeta):
    """
    Represents a database object that provides methods for connecting to and interacting with a database.
    
    API information on Notion:
    https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4
    """
    
    def __init__(self, config:ConfigWrapper) -> None:
        """
        Initializes the database object and creates a connection.
        The SingletonMeta metaclass ensures there's only one instance of a Database object
        
        Args:
            config (ConfigWrapper): A ConfigWrapper instance, which represents a database configuration, read from a yaml file.
                                    A configuration should first be instantiated by the ConfigFactory, after which it can be used as a ConfigWrapper instance.
        """
        self._config = config        
    
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
            logger.error(f"❌ Disconnection was not successful.")
            raise

    
    def is_connected(self) -> bool:
        """
        Checks whether there's a connection to the database.
        
        Returns:
            bool: True if connected, False otherwise.
        """  
        return hasattr(self._connection) and self._connection.is_connected() # this is a mysql python method, it pings the underlying server to check activity
        
    def execute_query_path(self, query_file_path: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Executes a raw SQL query on the connected database.
        
        Args:
            query_file_path (str): The file path of the SQL query to be executed.
            **kwargs: Used for dynamic replacements of SQL placeholders (aka SQL 'variables', denoted with '{}').
                      Prevents SQL injection (see API on Notion - https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4#1b9ed9807d5880a9881ff95f720e5f4c).
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the results of the query or a list of a Dict with key "affected rows" and value the number of affected rows
        """
        try:
            plain_text_query = self._get_query(query_file_path, **kwargs)

            if not hasattr(self, '_connection') or not self._connection.is_connected():
                self._connect()
            cursor = self._connection.cursor(dictionary=True)
            cursor.execute(plain_text_query)
            if cursor.description:
                results = cursor.fetchall()
            else:
                self._connection.commit()
                results = [{"affected_rows": cursor.rowcount}]

            if DEBUG:
                logger.debug(f"✅ Query executed successfully: {query_file_path}")
                logger.debug(f"🔍 Results: {results}")

            cursor.close()
            return results
        except mysql.connector.Error as e:
            logger.error(f"❌ MySQL error while executing query from {query_file_path}: {e}")
            raise
        except QueryConstructionError as e:
            logger.error(f"❌ Query construction error: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error during query execution: {e}")
            raise
        
    
    def _get_query(self, query_file_path: str, **kwargs) -> str:
        """
        Reads an SQL query from a file and converts it to plain text (str). After completion, the method checks whether all SQL variables
        ('{}') are replaced with parameters. 
            
        Args:
            query_file_path (str): The file path of the SQL query to be executed (cf. query_file_path in execute_query()).
            **kwargs: Keyword arguments to replace SQL placeholders with.
                
        Returns:
            str: The string of the SQL query to be executed, with SQL variables ('{}') replaced with the parameters.
                
        Raises:
            QueryConstructionError: When a variable substitution is missing in the query.
        """
        plain_text_query = read_sql_file(query_file_path)
        # for key, value in self._sub_query_mapping.items():
        #     plain_text_query = plain_text_query.replace(key,value)
        #TODO: find out how sub-queries are used and implement the placeholder replacements when there are sub-querries
        for key, value in kwargs.items():
            plain_text_query = plain_text_query.replace(key,value)
        try:
            assert('{' not in plain_text_query)
            assert('}' not in plain_text_query)
        except AssertionError:
            raise QueryConstructionError(f"missed substitution in query:\n{plain_text_query}")
        if DEBUG:
            #TODO: replace flask logging with our own
            logger.info(f"id:{g.execution_id}\n"
                        f"{query_file_path}:")
            logger.info(f"id:{g.execution_id}\n {plain_text_query}")
        return plain_text_query
