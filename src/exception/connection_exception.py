from typing import Optional, Dict
from . import ETLException

class ConnectionException(ETLException):
    """Exception for database connection issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_CONNECTION_ERROR",
            error_category="CONNECTION",
            context=context
        )