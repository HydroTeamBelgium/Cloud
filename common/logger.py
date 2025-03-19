import logging
import os
import sys

def setup_logging():
    """
    Configure the root logger with console and optional file handlers.
    This function is called automatically when the module is imported.
    """
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        root_logger.setLevel(logging.DEBUG)

        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if log_level in valid_levels:
            console_handler.setLevel(getattr(logging, log_level))
        else:
            console_handler.setLevel(logging.INFO)

        log_file = os.environ.get('LOG_FILE')
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG)  
            root_logger.addHandler(file_handler)

setup_logging()

def get_logger(name):
    """
    Retrieve a logger instance with the specified name.

    :param name: The name of the logger, typically __name__ for module-specific logging.
    :return: A logging.Logger instance configured with the shared setup.
    """
    return logging.getLogger(name)

def set_console_log_level(level):
    """
    Adjust the logging level for the console handler at runtime.

    :param level: A string representing the desired log level (e.g., 'DEBUG', 'INFO').
    """
    level = level.upper()
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if level in valid_levels:
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(getattr(logging, level))
    else:
        print(f"Invalid log level: {level}")