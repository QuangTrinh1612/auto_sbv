from typing import Optional, Dict
from . import ETLException

class LoadingException(ETLException):
    """Exception for data loading issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_LOADING_ERROR",
            error_category="LOADING",
            context=context
        )