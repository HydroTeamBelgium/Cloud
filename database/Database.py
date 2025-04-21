import mysql.connector
from typing import List, str, Dict, Any, Optional, Tuple, Bool

class Database:
    """
    API information on Notion:
    https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4
    """
    
    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        """
        Initializes the database object and creates a connection.
        
        host (str): Host adres of the database, found on the GCP console (go to the desired database instance)
        user (str): the user that tries to connect to the database (in testing probably a name, 
                    could be other instances from the car as well —> see GCP console/Database/Users)
        password (str): user password (set in GCP, see console). Password will be hashed and hash will be used
        database (str): the name of the MySQL database that you’re trying to connect to.
                        See GCP console and consult your team lead for which database you should connect to
        """
    
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
        
    def execute_query_path(self, query_file_path: str, params: tuple) -> List[Dict[str, Any]]:
        """
        Executes a raw SQL query on the connected database
        
        query (str): the raw SQL query. This query will be extracted from an SQL file.
                     Use of ‘typed’ queries (i.e. not extracted from an SQL file) are strongly not advised.
        params (tuple): Prevents SQL injection (see API on Notion - https://www.notion.so/Database-API-1a0ed9807d5880819ea3db2ee69cb93d?pvs=4#1b9ed9807d5880a9881ff95f720e5f4c)
        """
        pass
        
        