import oracledb
import os
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

class OracleDBManager:
    """A class to manage Oracle database connections and queries."""
    
    def __init__(self, username: str, password: str, dsn: str, 
                 config_dir: Optional[str] = None, wallet_location: Optional[str] = None):
        """
        Initialize Oracle DB connection parameters.
        
        Args:
            username: Database username
            password: Database password  
            dsn: Data Source Name (host:port/service_name or TNS alias)
            config_dir: Directory containing tnsnames.ora and sqlnet.ora
            wallet_location: Path to Oracle Wallet for secure connections
        """
        self.username = username
        self.password = password
        self.dsn = dsn
        self.config_dir = config_dir
        self.wallet_location = wallet_location
        
        # Instant Client for Thick Mode --- Thin mode not available
        lib_dir = r"\\localhost\c$\Users\trinh.quoc-quang\my-projects\auto_sbv\driver\instantclient_23_8"

        # Initialize Oracle client if needed
        if config_dir:
            oracledb.init_oracle_client(config_dir=config_dir, lib_dir=lib_dir)
        elif wallet_location:
            oracledb.init_oracle_client(config_dir=wallet_location, lib_dir=lib_dir)
        else:
            oracledb.init_oracle_client(lib_dir=lib_dir)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            if self.wallet_location:
                # For Oracle Cloud or wallet-based connections
                connection = oracledb.connect(
                    user=self.username,
                    password=self.password,
                    dsn=self.dsn,
                    config_dir=self.wallet_location,
                    wallet_location=self.wallet_location,
                    wallet_password=""  # Usually empty for auto-login wallets
                )
            else:
                # Standard connection
                connection = oracledb.connect(
                    user=self.username,
                    password=self.password,
                    dsn=self.dsn
                )
            yield connection
        except oracledb.Error as e:
            print(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as a list of dictionaries.
        
        Args:
            query: SQL SELECT statement
            params: Query parameters (optional)
            
        Returns:
            List of dictionaries representing query results
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all rows and convert to dictionaries
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
                
                return results
                
            except oracledb.Error as e:
                print(f"Query execution error: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_dml(self, statement: str, params: tuple = None) -> int:
        """
        Execute DML statements (INSERT, UPDATE, DELETE).
        
        Args:
            statement: SQL DML statement
            params: Statement parameters (optional)
            
        Returns:
            Number of affected rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(statement, params)
                else:
                    cursor.execute(statement)
                
                affected_rows = cursor.rowcount
                conn.commit()
                return affected_rows
                
            except oracledb.Error as e:
                conn.rollback()
                print(f"DML execution error: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_batch(self, statement: str, params_list: List[tuple]) -> int:
        """
        Execute batch DML operations for better performance.
        
        Args:
            statement: SQL DML statement
            params_list: List of parameter tuples
            
        Returns:
            Total number of affected rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(statement, params_list)
                affected_rows = cursor.rowcount
                conn.commit()
                return affected_rows
                
            except oracledb.Error as e:
                conn.rollback()
                print(f"Batch execution error: {e}")
                raise
            finally:
                cursor.close()
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                cursor.close()
                return result is not None
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

def main():
    # Database connection parameters
    username = "trinhquocquang"
    password = "uatlos123456"
    host = "10.8.0.137"
    port = "1625"
    service_name = "dfsp02"
    
    # Construct DSN (Data Source Name)
    dsn = f"{host}:{port}/{service_name}"
    
    # Initialize database manager
    db_manager = OracleDBManager(username, password, dsn)
    
    # Test connection
    if not db_manager.test_connection():
        print("Failed to connect to Oracle database")
        return
    
    print("Successfully connected to Oracle database!")
    
    try:
        print("\n=== Example: Parameterized Query ===")
        query = """
            SELECT owner, object_name, object_type 
            FROM all_objects 
            WHERE object_type = :obj_type 
            AND object_name LIKE '%LOAN%'
            AND ROWNUM <= :max_rows
        """
        params = ('TABLE', 50)
        results = db_manager.execute_query(query, params)
        
        print("Sample tables from all schemas:")
        for row in results:
            print(f"- {row['OWNER']}.{row['OBJECT_NAME']}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()