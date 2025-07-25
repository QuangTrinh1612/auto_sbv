from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors in the ETL framework.
    Defines the interface that all extractors must implement.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor with configuration.
        
        Args:
            config: Configuration dictionary specific to the extractor
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.extraction_stats = {
            'start_time': None,
            'end_time': None,
            'rows_extracted': 0,
            'tables_processed': 0,
            'errors': []
        }
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def extract_table(self, table_name: str, schema: Optional[str] = None, 
                     where_clause: Optional[str] = None,
                     columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Extract data from a specific table.
        
        Args:
            table_name: Name of the table to extract
            schema: Schema name (optional)
            where_clause: WHERE clause for filtering (optional)
            columns: List of columns to extract (optional)
            
        Returns:
            pd.DataFrame: Extracted data
        """
        pass
    
    @abstractmethod
    def extract_query(self, query: str, parameters: Optional[Dict] = None) -> pd.DataFrame:
        """
        Extract data using a custom SQL query.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters (optional)
            
        Returns:
            pd.DataFrame: Query results
        """
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            Dict: Table metadata
        """
        pass
    
    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        List all tables in the specified schema.
        
        Args:
            schema: Schema name (optional)
            
        Returns:
            List[str]: List of table names
        """
        pass
    
    def extract_incremental(self, table_name: str, 
                          timestamp_column: str,
                          last_extract_time: datetime,
                          schema: Optional[str] = None,
                          columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Extract incremental data based on timestamp.
        Default implementation using WHERE clause.
        
        Args:
            table_name: Name of the table to extract
            timestamp_column: Column to use for incremental extraction
            last_extract_time: Last extraction timestamp
            schema: Schema name (optional)
            columns: List of columns to extract (optional)
            
        Returns:
            pd.DataFrame: Incremental data
        """
        formatted_time = last_extract_time.strftime('%Y-%m-%d %H:%M:%S')
        where_clause = f"{timestamp_column} > '{formatted_time}'"
        
        return self.extract_table(
            table_name=table_name,
            schema=schema,
            where_clause=where_clause,
            columns=columns
        )
    
    def start_extraction(self):
        """Mark the start of extraction process."""
        self.extraction_stats['start_time'] = datetime.now()
        self.extraction_stats['rows_extracted'] = 0
        self.extraction_stats['tables_processed'] = 0
        self.extraction_stats['errors'] = []
        
        self.logger.info(f"Starting extraction process at {self.extraction_stats['start_time']}")
    
    def end_extraction(self):
        """Mark the end of extraction process."""
        self.extraction_stats['end_time'] = datetime.now()
        
        duration = self.extraction_stats['end_time'] - self.extraction_stats['start_time']
        
        self.logger.info(f"Extraction completed at {self.extraction_stats['end_time']}")
        self.logger.info(f"Total duration: {duration}")
        self.logger.info(f"Tables processed: {self.extraction_stats['tables_processed']}")
        self.logger.info(f"Rows extracted: {self.extraction_stats['rows_extracted']}")
        
        if self.extraction_stats['errors']:
            self.logger.warning(f"Errors encountered: {len(self.extraction_stats['errors'])}")
    
    def add_extraction_stats(self, rows: int, tables: int = 1):
        """
        Add to extraction statistics.
        
        Args:
            rows: Number of rows extracted
            tables: Number of tables processed (default: 1)
        """
        self.extraction_stats['rows_extracted'] += rows
        self.extraction_stats['tables_processed'] += tables
    
    def add_error(self, error: str):
        """
        Add an error to the extraction statistics.
        
        Args:
            error: Error message
        """
        self.extraction_stats['errors'].append({
            'timestamp': datetime.now(),
            'error': error
        })
    
    def get_extraction_stats(self) -> Dict[str, Any]:
        """
        Get extraction statistics.
        
        Returns:
            Dict: Extraction statistics
        """
        return self.extraction_stats.copy()
    
    def validate_config(self) -> bool:
        """
        Validate the extractor configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        if not self.config:
            self.logger.error("Configuration is empty")
            return False
        
        # Basic validation - can be overridden in subclasses
        required_fields = ['host', 'username', 'password']
        
        for field in required_fields:
            if field not in self.config:
                self.logger.error(f"Missing required configuration field: {field}")
                return False
        
        return True
    
    @abstractmethod
    def close(self):
        """Close connections and cleanup resources."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()