# src/core/connection_manager.py

import oracledb
import sqlalchemy
from sqlalchemy import create_engine, pool
import pandas as pd
from typing import Dict, Any, Optional, Union
import logging
from pathlib import Path
import sys
from contextlib import contextmanager
import threading
import time

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.exception import ETLException
from src.util.logger import get_logger
from src.util.encryption_util import decrypt_password


class ConnectionManager:
    """
    Centralized connection manager for database connections.
    Supports connection pooling, retry logic, and multiple database types.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self._connections = {}
        self._pools = {}
        self._lock = threading.Lock()
    
    def create_oracle_connection(self, config: Dict[str, Any]) -> oracledb.Connection:
        """
        Create Oracle database connection.
        
        Args:
            config: Oracle configuration dictionary
            
        Returns:
            oracledb.Connection: Oracle database connection
        """
        try:
            host = config.get('host')
            port = config.get('port', 1521)
            service_name = config.get('service_name')
            sid = config.get('sid')
            username = config.get('username')
            password = config.get('password')
            
            # Decrypt password if encrypted
            if config.get('password_encrypted', False):
                password = decrypt_password(password)
            
            # Create DSN
            if service_name:
                dsn = oracledb.makedsn(host, port, service_name=service_name)
            elif sid:
                dsn = oracledb.makedsn(host, port, sid=sid)
            else:
                raise ETLException("Either service_name or sid must be provided")
            
            # Set Oracle client configuration
            if config.get('thick_mode', False):
                try:
                    oracledb.init_oracle_client()
                except Exception as e:
                    self.logger.warning(f"Failed to initialize Oracle thick client: {e}")
            
            # Create connection
            connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn,
                encoding='UTF-8'
            )
            
            # Set session parameters
            cursor = connection.cursor()
            
            # Set session timezone if specified
            if config.get('timezone'):
                cursor.execute(f"ALTER SESSION SET TIME_ZONE = '{config['timezone']}'")
            
            # Set NLS parameters
            nls_params = config.get('nls_parameters', {})
            for param, value in nls_params.items():
                cursor.execute(f"ALTER SESSION SET {param} = '{value}'")
            
            cursor.close()
            
            self.logger.info(f"Oracle connection established successfully to {host}:{port}")
            return connection
            
        except oracledb.DatabaseError as e:
            self.logger.error(f"Failed to create Oracle connection: {str(e)}")
            raise ETLException(f"Oracle connection failed: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error creating Oracle connection: {str(e)}")
            raise ETLException(f"Connection creation failed: {str(e)}")
    
    def create_oracle_pool(self, config: Dict[str, Any]) -> oracledb.SessionPool:
        """
        Create Oracle connection pool.
        
        Args:
            config: Oracle configuration dictionary
            
        Returns:
            oracledb.SessionPool: Oracle connection pool
        """
        try:
            host = config.get('host')
            port = config.get('port', 1521)
            service_name = config.get('service_name')
            sid = config.get('sid')
            username = config.get('username')
            password = config.get('password')
            
            # Pool configuration
            min_connections = config.get('pool_min', 1)
            max_connections = config.get('pool_max', 10)
            increment = config.get('pool_increment', 1)
            
            # Decrypt password if encrypted
            if config.get('password_encrypted', False):
                password = decrypt_password(password)
            
            # Create DSN
            if service_name:
                dsn = oracledb.makedsn(host, port, service_name=service_name)
            elif sid:
                dsn = oracledb.makedsn(host, port, sid=sid)
            else:
                raise ETLException("Either service_name or sid must be provided")
            
            # Create connection pool
            pool = oracledb.SessionPool(
                user=username,
                password=password,
                dsn=dsn,
                min=min_connections,
                max=max_connections,
                increment=increment,
                threaded=True,
                getmode=oracledb.SPOOL_ATTRVAL_WAIT,
                timeout=config.get('pool_timeout', 3600),
                encoding='UTF-8'
            )
            
            self.logger.info(f"Oracle connection pool created: {min_connections}-{max_connections} connections")
            return pool
            
        except oracledb.DatabaseError as e:
            self.logger.error(f"Failed to create Oracle connection pool: {str(e)}")
            raise ETLException(f"Oracle pool creation failed: {str(e)}")
    
    def create_sqlalchemy_engine(self, config: Dict[str, Any]) -> sqlalchemy.Engine:
        """
        Create SQLAlchemy engine for Oracle.
        
        Args:
            config: Oracle configuration dictionary
            
        Returns:
            sqlalchemy.Engine: SQLAlchemy engine
        """
        try:
            host = config.get('host')
            port = config.get('port', 1521)
            service_name = config.get('service_name')
            sid = config.get('sid')
            username = config.get('username')
            password = config.get('password')
            
            # Decrypt password if encrypted
            if config.get('password_encrypted', False):
                password = decrypt_password(password)
            
            # Build connection URL
            if service_name:
                url = f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}"
            elif sid:
                url = f"oracle+oracledb://{username}:{password}@{host}:{port}/{sid}"
            else:
                raise ETLException("Either service_name or sid must be provided")
            
            # Engine configuration
            engine_config = {
                'pool_size': config.get('pool_size', 10),
                'max_overflow': config.get('max_overflow', 20),
                'pool_timeout': config.get('pool_timeout', 30),
                'pool_recycle': config.get('pool_recycle', 3600),
                'echo': config.get('echo', False)
            }
            
            engine = create_engine(url, **engine_config)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1 FROM DUAL"))
            
            self.logger.info("SQLAlchemy Oracle engine created successfully")
            return engine
            
        except Exception as e:
            self.logger.error(f"Failed to create SQLAlchemy engine: {str(e)}")
            raise ETLException(f"SQLAlchemy engine creation failed: {str(e)}")
    
    def get_connection(self, connection_name: str, config: Dict[str, Any]) -> oracledb.Connection:
        """
        Get a named connection, creating it if it doesn't exist.
        
        Args:
            connection_name: Name to identify the connection
            config: Connection configuration
            
        Returns:
            oracledb.Connection: Database connection
        """
        with self._lock:
            if connection_name not in self._connections:
                self._connections[connection_name] = self.create_oracle_connection(config)
            
            return self._connections[connection_name]
    
    def get_pool(self, pool_name: str, config: Dict[str, Any]) -> oracledb.SessionPool:
        """
        Get a named connection pool, creating it if it doesn't exist.
        
        Args:
            pool_name: Name to identify the pool
            config: Pool configuration
            
        Returns:
            oracledb.SessionPool: Connection pool
        """
        with self._lock:
            if pool_name not in self._pools:
                self._pools[pool_name] = self.create_oracle_pool(config)
            
            return self._pools[pool_name]
    
    @contextmanager
    def get_pooled_connection(self, pool_name: str, config: Dict[str, Any]):
        """
        Context manager for getting a connection from a pool.
        
        Args:
            pool_name: Name of the connection pool
            config: Pool configuration
            
        Yields:
            oracledb.Connection: Database connection from pool
        """
        pool = self.get_pool(pool_name, config)
        connection = None
        
        try:
            connection = pool.acquire()
            yield connection
        except Exception as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            self.logger.error(f"Error in pooled connection: {str(e)}")
            raise
        finally:
            if connection:
                try:
                    pool.release(connection)
                except Exception as e:
                    self.logger.warning(f"Error releasing connection to pool: {str(e)}")
    
    def test_connection(self, config: Dict[str, Any], retry_count: int = 3) -> bool:
        """
        Test database connection with retry logic.
        
        Args:
            config: Connection configuration
            retry_count: Number of retry attempts
            
        Returns:
            bool: True if connection successful
        """
        for attempt in range(retry_count):
            try:
                connection = self.create_oracle_connection(config)
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                cursor.close()
                connection.close()
                
                self.logger.info("Connection test successful")
                return True
                
            except Exception as e:
                self.logger.warning(f"Connection test attempt {attempt + 1} failed: {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error("All connection test attempts failed")
                    return False
        
        return False
    
    def execute_query(self, connection: oracledb.Connection, query: str, 
                     parameters: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.
        
        Args:
            connection: Database connection
            query: SQL query to execute
            parameters: Query parameters (optional)
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            if parameters:
                df = pd.read_sql_query(query, connection, params=parameters)
            else:
                df = pd.read_sql_query(query, connection)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise ETLException(f"Query execution failed: {str(e)}")
    
    def execute_ddl(self, connection: oracledb.Connection, ddl: str) -> bool:
        """
        Execute DDL statement.
        
        Args:
            connection: Database connection
            ddl: DDL statement to execute
            
        Returns:
            bool: True if successful
        """
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(ddl)
            connection.commit()
            
            self.logger.info("DDL executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"DDL execution failed: {str(e)}")
            if connection:
                connection.rollback()
            raise ETLException(f"DDL execution failed: {str(e)}")
        finally:
            if cursor:
                cursor.close()
    
    def get_connection_info(self, connection: oracledb.Connection) -> Dict[str, Any]:
        """
        Get connection information.
        
        Args:
            connection: Database connection
            
        Returns:
            Dict: Connection information
        """
        try:
            cursor = connection.cursor()
            
            # Get database version
            cursor.execute("SELECT * FROM V$VERSION WHERE ROWNUM = 1")
            version_info = cursor.fetchone()
            
            # Get instance info
            cursor.execute("SELECT INSTANCE_NAME, HOST_NAME FROM V$INSTANCE")
            instance_info = cursor.fetchone()
            
            # Get session info
            cursor.execute("""
                SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') as username,
                       SYS_CONTEXT('USERENV', 'SERVER_HOST') as server_host,
                       SYS_CONTEXT('USERENV', 'DB_NAME') as db_name
                FROM DUAL
            """)
            session_info = cursor.fetchone()
            
            cursor.close()
            
            return {
                'database_version': version_info[0] if version_info else 'Unknown',
                'instance_name': instance_info[0] if instance_info else 'Unknown',
                'host_name': instance_info[1] if instance_info else 'Unknown',
                'username': session_info[0] if session_info else 'Unknown',
                'server_host': session_info[1] if session_info else 'Unknown',
                'database_name': session_info[2] if session_info else 'Unknown'
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to get connection info: {str(e)}")
            return {}
    
    def close_connection(self, connection_name: str):
        """
        Close a named connection.
        
        Args:
            connection_name: Name of the connection to close
        """
        with self._lock:
            if connection_name in self._connections:
                try:
                    self._connections[connection_name].close()
                    del self._connections[connection_name]
                    self.logger.info(f"Connection '{connection_name}' closed")
                except Exception as e:
                    self.logger.warning(f"Error closing connection '{connection_name}': {str(e)}")
    
    def close_pool(self, pool_name: str):
        """
        Close a named connection pool.
        
        Args:
            pool_name: Name of the pool to close
        """
        with self._lock:
            if pool_name in self._pools:
                try:
                    self._pools[pool_name].close()
                    del self._pools[pool_name]
                    self.logger.info(f"Connection pool '{pool_name}' closed")
                except Exception as e:
                    self.logger.warning(f"Error closing pool '{pool_name}': {str(e)}")
    
    def close_all(self):
        """Close all connections and pools."""
        with self._lock:
            # Close all connections
            for name, connection in list(self._connections.items()):
                try:
                    connection.close()
                    self.logger.info(f"Connection '{name}' closed")
                except Exception as e:
                    self.logger.warning(f"Error closing connection '{name}': {str(e)}")
            
            # Close all pools
            for name, pool in list(self._pools.items()):
                try:
                    pool.close()
                    self.logger.info(f"Pool '{name}' closed")
                except Exception as e:
                    self.logger.warning(f"Error closing pool '{name}': {str(e)}")
            
            self._connections.clear()
            self._pools.clear()
            
            self.logger.info("All connections and pools closed")