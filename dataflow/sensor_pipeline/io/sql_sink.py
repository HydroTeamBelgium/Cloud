import apache_beam as beam
from apache_beam.io.jdbc import WriteToJdbc
from .base_sink import BaseSink
from typing import Dict, List, Optional, Any
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict

class SqlSink(BaseSink):
    """Implementation of BaseSink for writing data to SQL databases.
    
    This sink writes PCollection elements to a SQL database using JDBC.
    It handles Protocol Buffer messages by converting them to dictionaries.
    """
    
    def __init__(self, 
                 connection_url: str,
                 driver_class_name: str,
                 table_name: str,
                 username: str = None,
                 password: str = None,
                 statement: str = None,
                 batch_size: int = 1000):
        """Initialize the SQL sink.
        
        Args:
            connection_url: JDBC connection URL.
            driver_class_name: JDBC driver class name.
            table_name: Target table name.
            username: Database username.
            password: Database password.
            statement: Custom SQL statement (if not using default insert).
            batch_size: Number of records to insert in a batch.
        """
        self.connection_url = connection_url
        self.driver_class_name = driver_class_name
        self.table_name = table_name
        self.username = username
        self.password = password
        self.statement = statement
        self.batch_size = batch_size
    
    def configure(self, **kwargs):
        """Configure the SQL sink with specific parameters.
        
        Args:
            **kwargs: Configuration parameters which may include:
                - connection_url: JDBC connection URL
                - driver_class_name: JDBC driver class name
                - table_name: Target table name
                - username: Database username
                - password: Database password
                - statement: Custom SQL statement
                - batch_size: Number of records in a batch
                
        Returns:
            self: Returns the sink instance for method chaining.
        """
        # Update configuration with provided values
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
                
        return self
    
    def write(self, pcollection):
        """Write the PCollection to the SQL database.
        
        Args:
            pcollection: The PCollection to write to the database.
            
        Returns:
            PDone: Indicator that the write operation has been initiated.
            
        Raises:
            ValueError: If required configuration is missing.
        """
        if not self.connection_url or not self.driver_class_name or not self.table_name:
            raise ValueError("connection_url, driver_class_name, and table_name are required")
        
        # First convert Protocol Buffer messages to dictionaries
        pcoll_dict = pcollection | "ConvertProtoToDict" >> beam.Map(self._proto_to_dict)
        
        # Write to JDBC
        return pcoll_dict | "WriteToJdbc" >> WriteToJdbc(
            driver_class_name=self.driver_class_name,
            jdbc_url=self.connection_url,
            statement=self.statement or self._create_insert_statement(),
            table_name=self.table_name,
            username=self.username,
            password=self.password,
            batch_size=self.batch_size
        )
    
    def _create_insert_statement(self):
        """Create a default SQL insert statement.
        
        This method should be implemented based on the expected schema.
        For now, it returns a placeholder that will need to be replaced
        with an actual implementation.
        
        Returns:
            str: SQL insert statement.
        """
        return f"INSERT INTO {self.table_name} VALUES(?, ?, ?, ?, ?)"
    
    @staticmethod
    def _proto_to_dict(element):
        """Convert a Protocol Buffer message to a dictionary.
        
        Args:
            element: Protocol Buffer message object.
            
        Returns:
            dict: Dictionary representation of the message.
        """
        # If element is already a dict, return it
        if isinstance(element, dict):
            return element
            
        # Convert Protocol Buffer to dictionary
        try:
            # Use the protobuf library's MessageToDict for proper conversion
            # This handles special types like Timestamp correctly
            return MessageToDict(
                element, 
                preserving_proto_field_name=True,
                including_default_value_fields=True
            )
        except Exception as e:
            # If conversion fails, try a basic approach
            import logging
            logging.warning(f"Error converting protobuf to dict using MessageToDict: {e}")
            
            # Fall back to a basic dictionary conversion
            result = {}
            for field in element.DESCRIPTOR.fields:
                field_name = field.name
                if hasattr(element, field_name):
                    value = getattr(element, field_name)
                    
                    # Handle special types like Timestamp
                    if isinstance(value, Timestamp):
                        result[field_name] = value.ToDatetime().isoformat()
                    else:
                        result[field_name] = value
                        
            return result