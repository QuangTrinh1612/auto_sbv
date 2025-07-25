from typing import Optional, Dict
from . import ETLException

class TransformationException(ETLException):
    """Exception for data transformation issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_TRANSFORMATION_ERROR",
            error_category="TRANSFORMATION",
            context=context
        )