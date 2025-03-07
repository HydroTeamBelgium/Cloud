import mysql.connector
from mysql.connector import Error, MySQLConnection, CMySQLCursor
from typing import List, str, Dict, Any, Optional, Tuple, Bool

class Database:
    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        pass
    
    def connect(self) -> None:
        "Establishes a connection to the database"
        pass
    
    def disconnect(self) -> None:
        "Closes the connection to the database"
        pass
    
    def is_connected(sef) -> bool:
        """
        Checks wether there's a connection to the database
        returns true if connected, else false
        """  
        pass
    
    #CRUD instructions:
    
    def insert(self, table: str, data: Dict[str, Any]) -> int | None:
        """
        @table: str
        @data: Dict[str, Any] -- a key and value pair of a dictionnary, corresponding to the column name of the table, and the value to be inserted in that column
        
        outputs the id of the last inserted record in the database, or None if the insertion failed
        """
        pass
    
    def select(self, table: str, columns: List[str] = ["*"], where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        selects items from the database
        arguments:
        @table: select the required table where data should be retrieved
        @columns: list of strings, defaults to ["*"], which means "all the records"
        @where: where clause, defaults to None, and can be set to None as well (Optional), takes a dictionnary with a string key value (ie. the column name)
                and a 'Any' type as the value property of the dictionnary value
                
        returns a list of dictionnaries with string and 'Any' key-value pair, i.e. it returns the selected values out of the database
        """
        pass
    
    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> bool:
        """
        Updates records in the database and returns True if successful
        @table: table to be updated
        @data: the new data to overwrite the old data, column-value pair in the form of a dictionnary
        @where: where clause, column-value pair where the new data should be updated
        
        outputs a boolean if at least one record in the database is updated
        """
        pass
    
    def delete(self, table: str, where: Dict[str, Any]) -> bool:
        """
        @table: table where a record should be deleted
        @where: where clause where the record should be deleted (for example; {"id", 5})
        
        returns True if succesful
        """
        pass
    
    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
        """
        
        """
        pass
        
        