# Logging Module Guide

This guide explains how to use the custom logging module implemented in our cloud codebase. This module provides a pre-configured logging system that simplifies logging across the codebase. It outputs logs to the console by default and can also log to a file if configured. Below, you'll find instructions on how to import and use it in your code, along with practical use cases and examples.

---

## What This Logging Module Does

The logging module sets up a shared logging configuration for the entire project. Here's what it offers:

- **Console Logging**: Logs are sent to the console (stdout) with a default level of `INFO`. You can adjust this level using the `CONSOLE_LOG_LEVEL` environment variable.
- **File Logging**: Optionally logs to a file if you specify a file path with the `LOG_FILE` environment variable. Default level is `DEBUG`, configurable with `FILE_LOG_LEVEL`.
- **Log Levels**: Supports standard log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`.
- **Dynamic Control**: Allows changing log levels at runtime with the `set_console_log_level` and `set_file_log_level` functions.
- **Consistent Formatting**: Uses a standard format for all log messages: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`.
- **Module-Specific File Logging**: Option to create separate log files for specific modules.

This setup ensures we all use consistent logging, making it easier to monitor and debug the project.

---

## Importing and Using the Logger

To start using the logging module in your code, follow these steps:

1. **Import the `LoggerFactory` Class**

   Import `LoggerFactory` from the module to create a logger instance:

   ```python
   from common.logger import LoggerFactory
   ```

2. **Create a LoggerFactory Instance and Get a Logger**

   Initialize the factory and use it to get a logger named after your module:

   ```python
   logger_factory = LoggerFactory()
   logger = logger_factory.get_logger(__name__)
   ```

   Optionally, you can specify a dedicated log file for this logger:

   ```python
   logger_factory = LoggerFactory()
   logger = logger_factory.get_logger(__name__, log_file="my_module.log")
   ```

3. **Log Messages**

   Use the logger's methods to log messages at different levels:

   ```python
   logger.debug("This is a debug message")
   logger.info("Processing started")
   logger.warning("Something unexpected happened")
   logger.error("An error occurred")
   logger.critical("Critical failure, shutting down")
   ```

The module automatically configures the root logger when a LoggerFactory instance is created, so you don't need to call any additional setup methods.

---

## Log Levels

The module supports these standard log levels:

- **`DEBUG`**: Detailed information for debugging (e.g., variable values or step-by-step execution).
- **`INFO`**: General updates to confirm things are working (e.g., "Server started").
- **`WARNING`**: Minor issues that don't stop the program (e.g., "Configuration file not found, using defaults").
- **`ERROR`**: Serious problems that affect functionality (e.g., "Database connection failed").
- **`CRITICAL`**: Severe errors that may crash the program (e.g., "Out of memory, terminating").

Choose the right level based on the message's purpose and severity.

## Message Levels vs. Environment Variables

1. **Message Levels**: When you write code like `logger.debug()` or `logger.info()`, you're assigning a severity level to that specific message.

2. **Environment Variables**: Two separate environment variables control logging thresholds:
   - `CONSOLE_LOG_LEVEL`: Determines which messages appear in the console
   - `FILE_LOG_LEVEL`: Determines which messages are written to log files

Think of these as filters:
- Your code contains messages at various levels (DEBUG, INFO, WARNING, etc.)
- The environment variables determine which of these messages are actually shown or saved

### Why Change Log Levels?

- **Development**: Set CONSOLE_LOG_LEVEL=DEBUG to see all messages during development
- **Production**: Set CONSOLE_LOG_LEVEL=INFO or WARNING to reduce console output in production
- **Troubleshooting**: Set FILE_LOG_LEVEL=DEBUG to capture detailed logs for later analysis

---

## Controlling Log Output

### Console Logging

- **Default Behavior**: Logs `INFO` and above to the console.
- **Adjusting the Level**: Set the `CONSOLE_LOG_LEVEL` environment variable when running your application:

  ```bash
  CONSOLE_LOG_LEVEL=DEBUG python your_app.py
  ```

  Valid values are: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. If `CONSOLE_LOG_LEVEL` isn't set or is invalid, it defaults to `INFO`.

### File Logging

- **Enable File Logging**: Set the `LOG_FILE` environment variable to a file path:

  ```bash
  LOG_FILE=/var/log/myapp.log python your_app.py
  ```

- **Adjust File Log Level**: Set the `FILE_LOG_LEVEL` environment variable:

  ```bash
  FILE_LOG_LEVEL=INFO LOG_FILE=/var/log/myapp.log python your_app.py
  ```

- **Default Behavior**: By default, file logging captures `DEBUG` and above levels, regardless of the console log level setting.

### Module-Specific File Logging

You can create a dedicated log file for a specific module by passing the `log_file` parameter:

```python
from common.logger import LoggerFactory
logger_factory = LoggerFactory()
logger = logger_factory.get_logger(__name__, log_file="my_module.log")
```

This creates a separate log file that only contains logs from this specific logger, while still maintaining console output.

### Runtime Changes

- **Change Console Level Dynamically**: Use `set_console_log_level` to adjust the console log level during execution:

  ```python
  from common.logger import LoggerFactory
  logger_factory = LoggerFactory()
  
  # Later in code:
  logger_factory.set_console_log_level('DEBUG')  # Now console shows DEBUG and above
  ```

- **Change File Level Dynamically**: Similarly, use `set_file_log_level` to adjust file logging verbosity:

  ```python
  from common.logger import LoggerFactory
  logger_factory = LoggerFactory()
  
  # Later in code:
  logger_factory.set_file_log_level('INFO')  # Now log files only record INFO and above
  ```

  Valid levels are the same as above. If an invalid level is provided, it defaults to INFO and prints a warning.

---

## Use Cases and Examples

### Example 1: Basic Logging in a Module

Imagine you're writing a module `data_processor.py`:

```python
from common.logger import LoggerFactory

logger_factory = LoggerFactory()
logger = logger_factory.get_logger(__name__)

def process_data(data):
    logger.info("Starting data processing")
    if not data:
        logger.warning("No data provided")
        return
    try:
        result = data["value"] / 2
        logger.debug(f"Processed result: {result}")
    except KeyError:
        logger.error("Missing 'value' key in data")
    logger.info("Data processing complete")
```

Run it normally:

```bash
python data_processor.py
```

Output (default `INFO` level):
```
2023-10-10 10:00:00,000 - data_processor - INFO - Starting data processing
2023-10-10 10:00:00,001 - data_processor - INFO - Data processing complete
```

Run with debug logs:

```bash
CONSOLE_LOG_LEVEL=DEBUG python data_processor.py
```

Output:
```
2023-10-10 10:00:00,000 - data_processor - INFO - Starting data processing
2023-10-10 10:00:00,001 - data_processor - DEBUG - Processed result: 5.0
2023-10-10 10:00:00,002 - data_processor - INFO - Data processing complete
```

### Example 2: Logging to a File

Run the same script with file logging:

```bash
LOG_FILE=app.log python data_processor.py
```

Console output remains `INFO` and above, but `app.log` contains all messages by default:
```
2023-10-10 10:00:00,000 - data_processor - INFO - Starting data processing
2023-10-10 10:00:00,001 - data_processor - DEBUG - Processed result: 5.0
2023-10-10 10:00:00,002 - data_processor - INFO - Data processing complete
```

### Example 3: Module-Specific File Logging

```python
from common.logger import LoggerFactory

# Create a logger with its own log file
logger_factory = LoggerFactory()
logger = logger_factory.get_logger(__name__, log_file="my_module.log")

logger.debug("This debug message goes to my_module.log")
logger.info("This info message goes to both console and my_module.log")
```

### Example 4: Dynamic Log Level Change

Adjust log levels mid-execution:

```python
from common.logger import LoggerFactory

logger_factory = LoggerFactory()
logger = logger_factory.get_logger(__name__)

logger.debug("This won't show in console by default")
logger.info("App starting")

# Change console log level
logger_factory.set_console_log_level('DEBUG')
logger.debug("Now this will show in console")

# If file logging is enabled, adjust its level too
logger_factory.set_file_log_level('WARNING')
logger.info("This won't be logged to file anymore")
logger.warning("But this warning will be logged to file")
```

Output:
```
2023-10-10 10:00:00,000 - __main__ - INFO - App starting
2023-10-10 10:00:00,001 - __main__ - DEBUG - Now this will show in console
2023-10-10 10:00:00,002 - __main__ - WARNING - But this warning will be logged to file
```

---