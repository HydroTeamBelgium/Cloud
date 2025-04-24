class APINotAvailableError(Exception):
    """Raised when the API URL is not available or returns an error."""
    pass

class CSVNotCreatedError(Exception):
    """Raised when the CSV file is not created or is empty."""
    pass