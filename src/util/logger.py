"""
Centralized logging utility for ETL Framework
Provides structured logging with different levels, formatters, and handlers
"""

import logging
import logging.handlers
import os
import sys
import json
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path
import threading
from functools import wraps


class ETLFormatter(logging.Formatter):
    """Custom formatter for ETL logging with structured output"""
    
    def __init__(self, include_extra: bool = True, json_format: bool = False):
        self.include_extra = include_extra
        self.json_format = json_format
        
        if json_format:
            super().__init__()
        else:
            fmt = (
                '%(asctime)s | %(levelname)-8s | %(name)s | '
                '%(funcName)s:%(lineno)d | %(message)s'
            )
            super().__init__(fmt, datefmt='%Y-%m-%d %H:%M:%S')
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with ETL-specific information"""
        
        # Add ETL-specific context
        if not hasattr(record, 'job_id'):
            record.job_id = getattr(threading.current_thread(), 'job_id', 'unknown')
        if not hasattr(record, 'pipeline_id'):
            record.pipeline_id = getattr(threading.current_thread(), 'pipeline_id', 'unknown')
        if not hasattr(record, 'table_name'):
            record.table_name = getattr(threading.current_thread(), 'table_name', '')
        
        if self.json_format:
            return self._format_json(record)
        else:
            # Add extra context to message if available
            if self.include_extra and hasattr(record, 'extra_data'):
                record.message = f"{record.getMessage()} | Extra: {record.extra_data}"
            
            return super().format(record)
    
    def _format_json(self, record: logging.LogRecord) -> str:
        """Format record as JSON"""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'job_id': getattr(record, 'job_id', 'unknown'),
            'pipeline_id': getattr(record, 'pipeline_id', 'unknown'),
            'table_name': getattr(record, 'table_name', ''),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra data if present
        if hasattr(record, 'extra_data'):
            log_data['extra'] = record.extra_data
        
        return json.dumps(log_data, default=str)


class ContextFilter(logging.Filter):
    """Filter to add context information to log records"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context information to the record"""
        # Add thread-local context
        thread = threading.current_thread()
        record.job_id = getattr(thread, 'job_id', 'unknown')
        record.pipeline_id = getattr(thread, 'pipeline_id', 'unknown') 
        record.table_name = getattr(thread, 'table_name', '')
        
        # Add process information
        record.process_id = os.getpid()
        record.thread_id = threading.get_ident()
        
        return True


class ETLLogger:
    """Centralized logger for ETL operations"""
    
    _loggers: Dict[str, logging.Logger] = {}
    _configured: bool = False
    _lock = threading.Lock()
    
    @classmethod
    def configure(cls, config: Dict[str, Any]) -> None:
        """Configure logging system"""
        with cls._lock:
            if cls._configured:
                return
            
            # Create log directories
            cls._create_log_directories(config)
            
            # Configure root logger
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.DEBUG)
            
            # Remove existing handlers
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # Add configured handlers
            cls._add_handlers(root_logger, config)
            
            cls._configured = True
    
    @classmethod
    def _create_log_directories(cls, config: Dict[str, Any]) -> None:
        """Create necessary log directories"""
        log_dirs = [
            config.get('log_dir', './logs'),
            config.get('job_log_dir', './logs/etl_jobs'),
            config.get('error_log_dir', './logs/errors'),
            config.get('performance_log_dir', './logs/performance'),
            config.get('audit_log_dir', './logs/audit')
        ]
        
        for log_dir in log_dirs:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def _add_handlers(cls, logger: logging.Logger, config: Dict[str, Any]) -> None:
        """Add configured handlers to logger"""
        
        # Console handler
        if config.get('console_logging', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, config.get('console_level', 'INFO')))
            console_formatter = ETLFormatter(
                include_extra=config.get('console_include_extra', False),
                json_format=config.get('console_json', False)
            )
            console_handler.setFormatter(console_formatter)
            console_handler.addFilter(ContextFilter())
            logger.addHandler(console_handler)
        
        # File handler for general logs
        if config.get('file_logging', True):
            log_file = os.path.join(config.get('log_dir', './logs'), 'etl_framework.log')
            file_handler = logging.handlers.TimedRotatingFileHandler(
                log_file,
                when=config.get('rotation_when', 'midnight'),
                interval=config.get('rotation_interval', 1),
                backupCount=config.get('backup_count', 30),
                encoding='utf-8'
            )
            file_handler.setLevel(getattr(logging, config.get('file_level', 'DEBUG')))
            file_formatter = ETLFormatter(
                include_extra=config.get('file_include_extra', True),
                json_format=config.get('file_json', False)
            )
            file_handler.setFormatter(file_formatter)
            file_handler.addFilter(ContextFilter())
            logger.addHandler(file_handler)
        
        # Error file handler
        if config.get('error_logging', True):
            error_file = os.path.join(config.get('error_log_dir', './logs/errors'), 'errors.log')
            error_handler = logging.handlers.TimedRotatingFileHandler(
                error_file,
                when='midnight',
                interval=1,
                backupCount=90,
                encoding='utf-8'
            )
            error_handler.setLevel(logging.ERROR)
            error_formatter = ETLFormatter(
                include_extra=True,
                json_format=config.get('error_json', True)
            )
            error_handler.setFormatter(error_formatter)
            error_handler.addFilter(ContextFilter())
            logger.addHandler(error_handler)
        
        # Performance log handler
        if config.get('performance_logging', True):
            perf_file = os.path.join(config.get('performance_log_dir', './logs/performance'), 'performance.log')
            perf_handler = logging.handlers.TimedRotatingFileHandler(
                perf_file,
                when='midnight',
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
            perf_handler.setLevel(logging.INFO)
            perf_formatter = ETLFormatter(json_format=True)
            perf_handler.setFormatter(perf_formatter)
            perf_handler.addFilter(ContextFilter())
            
            # Create performance logger
            perf_logger = logging.getLogger('performance')
            perf_logger.addHandler(perf_handler)
            perf_logger.setLevel(logging.INFO)
            perf_logger.propagate = False
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get or create a logger with the given name"""
        if name not in cls._loggers:
            logger = logging.getLogger(name)
            cls._loggers[name] = logger
        
        return cls._loggers[name]
    
    @classmethod
    def set_context(cls, **kwargs) -> None:
        """Set context for current thread"""
        thread = threading.current_thread()
        for key, value in kwargs.items():
            setattr(thread, key, value)
    
    @classmethod
    def clear_context(cls) -> None:
        """Clear context for current thread"""
        thread = threading.current_thread()
        for attr in ['job_id', 'pipeline_id', 'table_name']:
            if hasattr(thread, attr):
                delattr(thread, attr)


def log_execution_time(logger: Optional[logging.Logger] = None):
    """Decorator to log function execution time"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            func_logger = logger or ETLLogger.get_logger(f"{func.__module__}.{func.__name__}")
            
            try:
                func_logger.info(f"Starting execution of {func.__name__}")
                result = func(*args, **kwargs)
                execution_time = (datetime.now() - start_time).total_seconds()
                
                func_logger.info(
                    f"Completed {func.__name__}",
                    extra={'extra_data': {'execution_time_seconds': execution_time}}
                )
                
                # Log to performance logger
                perf_logger = logging.getLogger('performance')
                perf_logger.info(
                    f"Function execution",
                    extra={
                        'extra_data': {
                            'function': func.__name__,
                            'module': func.__module__,
                            'execution_time_seconds': execution_time,
                            'status': 'success'
                        }
                    }
                )
                
                return result
                
            except Exception as e:
                execution_time = (datetime.now() - start_time).total_seconds()
                func_logger.error(
                    f"Error in {func.__name__}: {str(e)}",
                    extra={'extra_data': {'execution_time_seconds': execution_time}},
                    exc_info=True
                )
                
                # Log to performance logger
                perf_logger = logging.getLogger('performance')
                perf_logger.error(
                    f"Function execution failed",
                    extra={
                        'extra_data': {
                            'function': func.__name__,
                            'module': func.__module__,
                            'execution_time_seconds': execution_time,
                            'status': 'failed',
                            'error': str(e)
                        }
                    }
                )
                
                raise
        
        return wrapper
    return decorator


def log_method(logger: Optional[logging.Logger] = None, level: str = 'INFO'):
    """Decorator to log method entry and exit"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger or ETLLogger.get_logger(f"{func.__module__}.{func.__qualname__}")
            log_level = getattr(logging, level.upper())
            
            # Log entry
            func_logger.log(log_level, f"Entering {func.__qualname__}")
            
            try:
                result = func(*args, **kwargs)
                func_logger.log(log_level, f"Exiting {func.__qualname__}")
                return result
            except Exception as e:
                func_logger.error(f"Exception in {func.__qualname__}: {str(e)}", exc_info=True)
                raise
        
        return wrapper
    return decorator

class LoggerMixin:
    """Mixin class to add logging capabilities to any class"""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class"""
        if not hasattr(self, '_logger'):
            self._logger = ETLLogger.get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        return self._logger
    
    def log_info(self, message: str, **kwargs) -> None:
        """Log info message with optional extra data"""
        extra = {'extra_data': kwargs} if kwargs else None
        self.logger.info(message, extra=extra)
    
    def log_error(self, message: str, exception: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with optional exception and extra data"""
        extra = {'extra_data': kwargs} if kwargs else None
        self.logger.error(message, extra=extra, exc_info=exception)
    
    def log_warning(self, message: str, **kwargs) -> None:
        """Log warning message with optional extra data"""
        extra = {'extra_data': kwargs} if kwargs else None
        self.logger.warning(message, extra=extra)
    
    def log_debug(self, message: str, **kwargs) -> None:
        """Log debug message with optional extra data"""
        extra = {'extra_data': kwargs} if kwargs else None
        self.logger.debug(message, extra=extra)


# Example usage and configuration
def get_default_config() -> Dict[str, Any]:
    """Get default logging configuration"""
    return {
        'log_dir': './logs',
        'job_log_dir': './logs/etl_jobs',
        'error_log_dir': './logs/errors',
        'performance_log_dir': './logs/performance',
        'audit_log_dir': './logs/audit',
        'console_logging': True,
        'console_level': 'INFO',
        'console_include_extra': False,
        'console_json': False,
        'file_logging': True,
        'file_level': 'DEBUG',
        'file_include_extra': True,
        'file_json': False,
        'error_logging': True,
        'error_json': True,
        'performance_logging': True,
        'rotation_when': 'midnight',
        'rotation_interval': 1,
        'backup_count': 30
    }

if __name__ == "__main__":
    # Example usage
    config = get_default_config()
    ETLLogger.configure(config)
    
    # Set context for current thread
    ETLLogger.set_context(job_id='test_job_001', pipeline_id='test_pipeline')
    
    # Get logger and test
    logger = ETLLogger.get_logger('test.example')
    
    logger.info("This is an info message")
    logger.warning("This is a warning", extra={'extra_data': {'table': 'customers', 'rows': 1000}})
    logger.error("This is an error", extra={'extra_data': {'error_code': 'DB001'}})
    
    # Test decorator
    @log_execution_time(logger)
    def test_function():
        import time
        time.sleep(1)
        return "success"
    
    result = test_function()
    print(f"Function result: {result}")