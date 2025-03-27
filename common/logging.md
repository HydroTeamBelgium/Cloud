# Logging Module Guide

This guide explains how to use the custom logging module implemented in our cloud codebase. This module provides a pre-configured logging system that simplifies logging across the codebase. It outputs logs to the console by default and can also log to a file if configured. Below, you'll find instructions on how to import and use it in your code, along with practical use cases and examples.

---

## What This Logging Module Does

The logging module sets up a shared logging configuration for the entire project. Here's what it offers:

- **Console Logging**: Logs are sent to the console (stdout) with a default level of `INFO`. You can adjust this level using the `LOG_LEVEL` environment variable.
- **File Logging**: Optionally logs to a file if you specify a file path with the `LOG_FILE` environment variable.
- **Log Levels**: Supports standard log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`.
- **Dynamic Control**: Allows changing the console log level at runtime with the `set_console_log_level` function.
- **Consistent Formatting**: Uses a standard format for all log messages: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`.

This setup ensures we all use consistent logging, making it easier to monitor and debug the project.

---

## Importing and Using the Logger

To start using the logging module in your code, follow these steps:

1. **Import the `get_logger` Function**

   Import `get_logger` from the module to create a logger instance:

   ```python
   from common.logger import get_logger
   ```

2. **Create a Logger Instance**

   Use `get_logger` with `__name__` to get a logger named after your module. This helps identify where log messages come from:

   ```python
   logger = get_logger(__name__)
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

The module automatically configures the root logger when imported, so you don't need to call `setup_logging()` yourself—it's handled for you.

---

## Log Levels

The module supports these standard log levels:

- **`DEBUG`**: Detailed information for debugging (e.g., variable values or step-by-step execution).
- **`INFO`**: General updates to confirm things are working (e.g., "Server started").
- **`WARNING`**: Minor issues that don't stop the program (e.g., "Configuration file not found, using defaults").
- **`ERROR`**: Serious problems that affect functionality (e.g., "Database connection failed").
- **`CRITICAL`**: Severe errors that may crash the program (e.g., "Out of memory, terminating").

Choose the right level based on the message's purpose and severity.

## Message Levels vs. LOG_LEVEL Environment Variable

1. **Message Levels**: When you write code like `logger.debug()` or `logger.info()`, you're assigning a severity level to that specific message.

2. **LOG_LEVEL Environment Variable**: This sets a threshold that determines which messages actually appear in the console. Only messages with a level equal to or higher than this threshold will be displayed.

Think of it like a filter:
- Your code contains messages at various levels (DEBUG, INFO, WARNING, etc.)
- The LOG_LEVEL environment variable determines which of these messages are actually shown

### Why Change LOG_LEVEL?

- **Development**: Set LOG_LEVEL=DEBUG to see all messages, including detailed debugging information
- **Production**: Set LOG_LEVEL=INFO or WARNING to reduce log volume and focus on important events
- **Troubleshooting**: Temporarily change LOG_LEVEL to DEBUG when investigating an issue, then switch back

---

## Controlling Log Output

### Console Logging

- **Default Behavior**: Logs `INFO` and above to the console.
- **Adjusting the Level**: Set the `LOG_LEVEL` environment variable when running your application:

  ```bash
  LOG_LEVEL=DEBUG python your_app.py
  ```

  Valid values are: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. If `LOG_LEVEL` isn't set or is invalid, it defaults to `INFO`.

### File Logging

- **Enable File Logging**: Set the `LOG_FILE` environment variable to a file path:

  ```bash
  LOG_FILE=/var/log/myapp.log python your_app.py
  ```

- **Behavior**: All messages (`DEBUG` and above) are written to the file, regardless of the LOG_LEVEL environment variable setting for console output. This means your file will contain the complete log record while your console shows only the filtered view based on LOG_LEVEL.

### Runtime Changes

- **Change Console Level Dynamically**: Use `set_console_log_level` to adjust the console log level during execution:

  ```python
  from common.logger import set_console_log_level

  set_console_log_level('DEBUG')  # Now console shows DEBUG and above
  ```

  Valid levels are the same as above. If an invalid level is provided, it prints an error message and leaves the level unchanged.

---

## Use Cases and Examples

### Example 1: Basic Logging in a Module

Imagine you're writing a module `data_processor.py`:

```python
from common.logger import get_logger

logger = get_logger(__name__)

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
LOG_LEVEL=DEBUG python data_processor.py
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

Console output remains `INFO` and above, but `app.log` contains:
```
2023-10-10 10:00:00,000 - data_processor - INFO - Starting data processing
2023-10-10 10:00:00,001 - data_processor - DEBUG - Processed result: 5.0
2023-10-10 10:00:00,002 - data_processor - INFO - Data processing complete
```

### Example 3: Dynamic Log Level Change

Adjust the console level mid-execution:

```python
from common.logger import get_logger, set_console_log_level

logger = get_logger(__name__)

logger.debug("This won't show yet")
logger.info("App starting")
set_console_log_level('DEBUG')
logger.debug("Now this will show")
```

Output:
```
2023-10-10 10:00:00,000 - __main__ - INFO - App starting
2023-10-10 10:00:00,001 - __main__ - DEBUG - Now this will show
```

---