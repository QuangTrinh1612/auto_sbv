from typing import Optional, Dict
from . import ETLException

class ExtractionException(ETLException):
    """Exception for data extraction issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_EXTRACTION_ERROR",
            error_category="EXTRACTION",
            context=context
        )