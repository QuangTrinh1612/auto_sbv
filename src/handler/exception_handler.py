# TO-DO LIST
# - Implement the NotificationService class
# - Implement method NotificationService.send_error_notification

from typing import Optional, Dict, Any, Callable
from functools import wraps
import traceback

from src.exception import ETLException
from src.util.logger import ETLLogger

class ETLExceptionHandler:
    """
    Centralized exception handler for ETL operations.
    Provides logging, notification, and recovery mechanisms.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.logger = ETLLogger.get_logger(__name__)
        self.config = config or {}
        self.notification_service = None
        
        # Error tracking
        self.error_counts = {}
        self.error_history = []
        
        # Configuration
        self.max_retry_attempts = self.config.get('max_retry_attempts', 3)
        self.send_notifications = self.config.get('send_notifications', False)
        self.log_stack_trace = self.config.get('log_stack_trace', True)
        self.error_threshold = self.config.get('error_threshold', 10)
    
    def handle_exception(self, exception: Exception, context: Optional[Dict] = None, 
                        operation: Optional[str] = None, 
                        notify: bool = True) -> Dict[str, Any]:
        """
        Handle an exception with logging, tracking, and optional notification.
        
        Args:
            exception: The exception to handle
            context: Additional context about the error
            operation: Name of the operation that failed
            notify: Whether to send notifications
            
        Returns:
            Dict: Error details and handling results
        """
        # Convert to ETLException if not already
        if not isinstance(exception, ETLException):
            etl_exception = ETLException(
                message=str(exception),
                error_category="UNKNOWN",
                context=context
            )
        else:
            etl_exception = exception
            if context:
                etl_exception.context.update(context)
        
        # Add operation to context
        if operation:
            etl_exception.context['operation'] = operation
        
        # Log the exception
        self._log_exception(etl_exception)
        
        # Track the error
        self._track_error(etl_exception)
        
        # Send notification if enabled
        if notify and self.send_notifications and self.notification_service:
            self._send_notification(etl_exception)
        
        # Return error details
        return {
            'handled': True,
            'error_details': etl_exception.to_dict(),
            'error_id': self._generate_error_id(etl_exception),
            'timestamp': etl_exception.timestamp
        }
    
    def _log_exception(self, exception: ETLException):
        """Log the exception with appropriate level and details."""
        error_msg = f"[{exception.error_category}] {exception.message}"
        
        # Add context to log message
        if exception.context:
            context_str = ", ".join([f"{k}={v}" for k, v in exception.context.items()])
            error_msg += f" | Context: {context_str}"
        
        # Log based on error category
        if exception.error_category in ['CONNECTION', 'CONFIGURATION']:
            self.logger.error(error_msg)
        elif exception.error_category in ['VALIDATION', 'TRANSFORMATION']:
            self.logger.warning(error_msg)
        else:
            self.logger.error(error_msg)
        
        # Log stack trace if enabled
        if self.log_stack_trace:
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    def _track_error(self, exception: ETLException):
        """Track error for monitoring and analysis."""
        error_key = f"{exception.error_category}:{exception.error_code}"
        
        # Increment error count
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        # Add to error history
        self.error_history.append({
            'timestamp': exception.timestamp,
            'category': exception.error_category,
            'code': exception.error_code,
            'message': exception.message,
            'context': exception.context
        })
        
        # Keep only recent errors (last 1000)
        if len(self.error_history) > 1000:
            self.error_history = self.error_history[-1000:]
        
        # Check if error threshold exceeded
        total_errors = sum(self.error_counts.values())
        if total_errors >= self.error_threshold:
            self.logger.critical(f"Error threshold exceeded: {total_errors} errors")
    
    def _send_notification(self, exception: ETLException):
        """Send notification about the error."""
        try:
            if self.notification_service:
                self.notification_service.send_error_notification(
                    subject=f"ETL Error: {exception.error_category}",
                    message=exception.message,
                    error_details=exception.to_dict()
                )
        except Exception as e:
            self.logger.warning(f"Failed to send error notification: {str(e)}")
    
    def _generate_error_id(self, exception: ETLException) -> str:
        """Generate unique error ID for tracking."""
        import hashlib
        
        error_string = f"{exception.timestamp}{exception.error_code}{exception.message}"
        return hashlib.md5(error_string.encode()).hexdigest()[:8]
    
    def retry_on_exception(self, exceptions: tuple = (Exception,), 
                          max_attempts: Optional[int] = None,
                          backoff_factor: float = 2.0):
        """
        Decorator for retrying operations on specific exceptions.
        
        Args:
            exceptions: Tuple of exception types to retry on
            max_attempts: Maximum retry attempts (uses config default if None)
            backoff_factor: Exponential backoff factor
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempts = max_attempts or self.max_retry_attempts
                
                for attempt in range(attempts):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        if attempt == attempts - 1:  # Last attempt
                            self.handle_exception(
                                e, 
                                context={'function': func.__name__, 'attempt': attempt + 1},
                                operation=f"Retry {func.__name__}"
                            )
                            raise
                        else:
                            wait_time = backoff_factor ** attempt
                            self.logger.warning(
                                f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                                f"Retrying in {wait_time} seconds..."
                            )
                            import time
                            time.sleep(wait_time)
                
            return wrapper
        return decorator
    
    def safe_execute(self, func: Callable, *args, 
                    default_return=None, context: Optional[Dict] = None, **kwargs):
        """
        Safely execute a function with exception handling.
        
        Args:
            func: Function to execute
            *args: Function arguments
            default_return: Default return value if function fails
            context: Additional context for error handling
            **kwargs: Function keyword arguments
            
        Returns:
            Function result or default_return if exception occurs
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.handle_exception(
                e,
                context=context or {'function': func.__name__},
                operation=f"Safe execution of {func.__name__}"
            )
            return default_return
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of errors encountered.
        
        Returns:
            Dict: Error summary statistics
        """
        total_errors = sum(self.error_counts.values())
        
        # Get error breakdown by category
        category_counts = {}
        for error_key, count in self.error_counts.items():
            category = error_key.split(':')[0]
            category_counts[category] = category_counts.get(category, 0) + count
        
        # Get recent errors (last 10)
        recent_errors = self.error_history[-10:] if self.error_history else []
        
        return {
            'total_errors': total_errors,
            'error_categories': category_counts,
            'error_counts': self.error_counts,
            'recent_errors': recent_errors,
            'error_threshold': self.error_threshold,
            'threshold_exceeded': total_errors >= self.error_threshold
        }
    
    def reset_error_tracking(self):
        """Reset error tracking counters and history."""
        self.error_counts.clear()
        self.error_history.clear()
        self.logger.info("Error tracking reset")
    
    def create_context_manager(self, operation: str, context: Optional[Dict] = None):
        """
        Create a context manager for exception handling.
        
        Args:
            operation: Name of the operation
            context: Additional context
            
        Returns:
            Context manager
        """
        class ExceptionContextManager:
            def __init__(self, handler, operation, context):
                self.handler = handler
                self.operation = operation
                self.context = context or {}
            
            def __enter__(self):
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type:
                    self.handler.handle_exception(
                        exc_val,
                        context=self.context,
                        operation=self.operation
                    )
                return False  # Don't suppress the exception
        
        return ExceptionContextManager(self, operation, context)

# Global exception handler instance
_exception_handler = None


def get_exception_handler(config: Optional[Dict[str, Any]] = None) -> ETLExceptionHandler:
    """
    Get the global exception handler instance.
    
    Args:
        config: Configuration for exception handler
        
    Returns:
        ETLExceptionHandler: Exception handler instance
    """
    global _exception_handler
    
    if _exception_handler is None or config:
        _exception_handler = ETLExceptionHandler(config)
    
    return _exception_handler


def handle_etl_exception(exception: Exception, context: Optional[Dict] = None, 
                        operation: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function for handling exceptions.
    
    Args:
        exception: Exception to handle
        context: Additional context
        operation: Operation name
        
    Returns:
        Dict: Error handling results
    """
    handler = get_exception_handler()
    return handler.handle_exception(exception, context, operation)