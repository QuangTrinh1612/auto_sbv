from typing import Optional, Dict
from . import ETLException

class ConfigurationException(ETLException):
    """Exception for configuration issues."""
    
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="ETL_CONFIG_ERROR",
            error_category="CONFIGURATION",
            context=context
        )