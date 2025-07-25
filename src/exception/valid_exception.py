from typing import Optional, Dict
from . import ETLException

class ValidationException(ETLException):
    """Exception for data validation issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_VALIDATION_ERROR",
            error_category="VALIDATION",
            context=context
        )