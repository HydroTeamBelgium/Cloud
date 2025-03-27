import logging
import os
import sys
from typing import Optional, Any, Dict, Type


class SingletonMeta(type):
    """
    A metaclass that implements the Singleton pattern to ensure only one instance of a class exists.
    
    This is used to guarantee that only one logging configuration exists across the application,
    preventing duplicate or conflicting logger setups.
    """
    _instances: Dict[Type, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Override the class instantiation to return an existing instance if one exists,
        otherwise create and store a new instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class LoggerFactory(metaclass=SingletonMeta):
    """
    A factory class that creates and configures loggers with consistent settings across the application.
    
    This class uses the Singleton pattern (via SingletonMeta) to ensure all loggers share the same
    configuration. It supports both console and file logging, with configurable log levels via
    environment variables.

    Key features:
    - Console logging always enabled, writing to stdout
    - Optional file logging when LOG_FILE environment variable is set
    - Configurable log levels via CONSOLE_LOG_LEVEL and FILE_LOG_LEVEL environment variables
    - Consistent log formatting across all handlers
    """
    
    def __init__(self) -> None:
        """
        Initialize the logger factory with root logger and handlers.
        
        Sets up the root logger and prepares handler placeholders. The actual logging
        configuration happens in _setup_logging().
        """
        self.root_logger: logging.Logger = logging.getLogger()
        self.console_handler: Optional[logging.StreamHandler] = None
        self.file_handler: Optional[logging.FileHandler] = None
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """
        Configure the root logger with console and optional file handlers.
        
        This method:
        1. Sets root logger to DEBUG to allow handler-level filtering
        2. Creates console handler with level from CONSOLE_LOG_LEVEL (defaults to INFO)
        3. Creates file handler if LOG_FILE is set, with level from FILE_LOG_LEVEL (defaults to DEBUG)
        4. Uses consistent format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
        Only runs configuration if handlers haven't been set up yet, preventing duplicate handlers.
        """
        # Remove existing handlers to prevent duplicates
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)
            
        self.root_logger.setLevel(logging.DEBUG)
        
        # Read environment variables for log levels
        console_log_level = os.environ.get('CONSOLE_LOG_LEVEL', 'INFO').upper()
        file_log_level = os.environ.get('FILE_LOG_LEVEL', 'DEBUG').upper()
        
        # Set up console handler
        self.console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.console_handler.setFormatter(formatter)
        self._set_handler_level(self.console_handler, console_log_level)
        self.root_logger.addHandler(self.console_handler)
        
        # Set up file handler if LOG_FILE is set
        log_file = os.environ.get('LOG_FILE')
        if log_file:
            self.file_handler = logging.FileHandler(log_file)
            self.file_handler.setFormatter(formatter)
            self._set_handler_level(self.file_handler, file_log_level)
            self.root_logger.addHandler(self.file_handler)
    
    def _set_handler_level(self, handler: logging.Handler, level: str) -> None:
        """
        Set a handler's log level, with validation and fallback to INFO.
        
        Args:
            handler: The logging handler to configure
            level: String representing desired log level ('DEBUG', 'INFO', etc.)
        
        If the provided level is invalid, defaults to INFO and prints a warning.
        """
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if level in valid_levels:
            handler.setLevel(getattr(logging, level))
        else:
            handler.setLevel(logging.INFO)
            print(f"Invalid log level: {level}, defaulting to INFO", file=sys.stderr)
    
    def get_logger(self, name: str, log_file: Optional[str] = None) -> logging.Logger:
        """
        Create or retrieve a logger with optional file-specific logging.
        
        Args:
            name: Logger name, typically __name__ for module-specific logging
            log_file: Optional path to a dedicated log file for this logger
        
        Returns:
            A configured logging.Logger instance
        
        If log_file is provided:
        - Creates a separate file handler for this specific logger
        - Disables propagation to prevent duplicate logging
        - Maintains console output by adding a console handler directly
        """
        logger = logging.getLogger(name)
        
        if log_file:
            # Clear any existing handlers to avoid duplication
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                
            # Add file handler
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            file_log_level = os.environ.get('FILE_LOG_LEVEL', 'DEBUG').upper()
            self._set_handler_level(file_handler, file_log_level)
            logger.addHandler(file_handler)
            
            # Prevent propagation to root logger to avoid duplicate logging
            logger.propagate = False
            
            # Add console handler to ensure output still goes to console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            console_log_level = os.environ.get('CONSOLE_LOG_LEVEL', 'INFO').upper()
            self._set_handler_level(console_handler, console_log_level)
            logger.addHandler(console_handler)
        
        return logger
    
    def set_console_log_level(self, level: str) -> None:
        """
        Dynamically change the console handler's log level.
        
        Args:
            level: String representing the new log level (e.g., 'DEBUG', 'INFO')
        
        Allows runtime adjustment of console logging verbosity without restart.
        """
        if self.console_handler:
            self._set_handler_level(self.console_handler, level.upper())
            
        # Also update any console handlers attached to individual loggers
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                    self._set_handler_level(handler, level.upper())
    
    def set_file_log_level(self, level: str) -> None:
        """
        Dynamically change the file handler's log level.
        
        Args:
            level: String representing the new log level (e.g., 'DEBUG', 'INFO')
        
        Allows runtime adjustment of file logging verbosity without restart.
        """
        if self.file_handler:
            self._set_handler_level(self.file_handler, level.upper())
            
        # Also update any file handlers attached to individual loggers
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    self._set_handler_level(handler, level.upper())


LoggerFactory = LoggerFactory()

def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    External function to get a configured logger instance.
    
    Args:
        name: Logger name, typically __name__ for module-specific logging
        log_file: Optional path to a dedicated log file for this logger
    
    Returns:
        A configured logging.Logger instance from the LoggerFactory
    
    This is the main entry point for getting loggers in the application.
    """
    return LoggerFactory.get_logger(name, log_file)