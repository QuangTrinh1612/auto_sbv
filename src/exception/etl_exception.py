from typing import Dict, Any, Optional
from datetime import datetime

class ETLException(Exception):
    """
    Custom exception class for ETL operations.
    Provides additional context and categorization for ETL-specific errors.
    """
    
    def __init__(self, message: str, error_code: Optional[str] = None, 
                 error_category: Optional[str] = None, context: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "ETL_GENERIC_ERROR"
        self.error_category = error_category or "UNKNOWN"
        self.context = context or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/serialization."""
        return {
            'message': self.message,
            'error_code': self.error_code,
            'error_category': self.error_category,
            'context': self.context,
            'timestamp': self.timestamp.isoformat()
        }