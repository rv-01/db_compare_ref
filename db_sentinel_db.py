#!/usr/bin/env python3
"""
Database Manager for DB Sentinel Utility

This module handles Oracle database connections using the modern oracledb library.
It provides connection pooling, transaction management, and query execution utilities.

Author: Solutions Architect
Version: 1.0
"""

import logging
import threading
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
import time

import oracledb
import pandas as pd


class DatabaseManager:
    """
    Manages Oracle database connections and operations for DB Sentinel utility.
    
    This class provides connection pooling, thread-safe database operations,
    and utilities for executing queries and managing transactions.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the database manager.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Connection pools
        self._source_pool = None
        self._target_pool = None
        
        # Thread-local storage for connections
        self._thread_local = threading.local()
        
        # Lock for pool creation
        self._pool_lock = threading.Lock()
        
        # Initialize Oracle client (thick mode for better performance)
        try:
            oracledb.init_oracle_client()
            self.logger.info("Oracle client initialized in thick mode")
        except Exception as e:
            self.logger.warning(f"Could not initialize thick mode: {str(e)}, using thin mode")
    
    def _create_connection_pool(self, db_config: Dict[str, Any], pool_name: str) -> oracledb.ConnectionPool:
        """
        Create a connection pool for the specified database.
        
        Args:
            db_config (Dict[str, Any]): Database configuration
            pool_name (str): Name for the connection pool
            
        Returns:
            oracledb.ConnectionPool: Created connection pool
        """
        try:
            # Build connection parameters
            dsn = (
                f"{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
            )
            
            pool_size = self.config['performance']['connection_pool_size']
            
            # Create connection pool
            pool = oracledb.create_pool(
                user=db_config['username'],
                password=db_config['password'],
                dsn=dsn,
                min=1,
                max=pool_size,
                increment=1,
                threaded=True,
                getmode=oracledb.POOL_GETMODE_WAIT,
                timeout=self.config['performance']['timeout_seconds']
            )
            
            self.logger.info(f"Created connection pool '{pool_name}' with {pool_size} connections")
            return pool
            
        except Exception as e:
            self.logger.error(f"Error creating connection pool '{pool_name}': {str(e)}")
            raise
    
    def get_source_connection(self) -> oracledb.Connection:
        """
        Get a connection to the source database.
        
        Returns:
            oracledb.Connection: Source database connection
        """
        if not hasattr(self._thread_local, 'source_conn') or self._thread_local.source_conn is None:
            if self._source_pool is None:
                with self._pool_lock:
                    if self._source_pool is None:
                        self._source_pool = self._create_connection_pool(
                            self.config['source_db'], 
                            'source_pool'
                        )
            
            self._thread_local.source_conn = self._source_pool.acquire()
            self.logger.debug("Acquired source database connection")
        
        return self._thread_local.source_conn
    
    def get_target_connection(self) -> oracledb.Connection:
        """
        Get a connection to the target database.
        
        Returns:
            oracledb.Connection: Target database connection
        """
        if not hasattr(self._thread_local, 'target_conn') or self._thread_local.target_conn is None:
            if self._target_pool is None:
                with self._pool_lock:
                    if self._target_pool is None:
                        self._target_pool = self._create_connection_pool(
                            self.config['target_db'], 
                            'target_pool'
                        )
            
            self._thread_local.target_conn = self._target_pool.acquire()
            self.logger.debug("Acquired target database connection")
        
        return self._thread_local.target_conn
    
    @contextmanager
    def get_connection(self, database: str):
        """
        Context manager for database connections.
        
        Args:
            database (str): Either 'source' or 'target'
            
        Yields:
            oracledb.Connection: Database connection
        """
        if database == 'source':
            conn = self.get_source_connection()
        elif database == 'target':
            conn = self.get_target_connection()
        else:
            raise ValueError(f"Invalid database identifier: {database}")
        
        try:
            yield conn
        except Exception as e:
            if hasattr(conn, 'rollback'):
                conn.rollback()
            raise
    
    def execute_query(self, connection: oracledb.Connection, query: str, 
                     params: Optional[Dict] = None, fetch: bool = True) -> Optional[List[Tuple]]:
        """
        Execute a SQL query on the specified connection.
        
        Args:
            connection (oracledb.Connection): Database connection
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            fetch (bool): Whether to fetch results
            
        Returns:
            Optional[List[Tuple]]: Query results if fetch=True, None otherwise
        """
        cursor = None
        try:
            cursor = connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                return cursor.fetchall()
            else:
                connection.commit()
                return None
                
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            if hasattr(connection, 'rollback'):
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_query_to_dataframe(self, connection: oracledb.Connection, 
                                  query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            connection (oracledb.Connection): Database connection
            query (str): SQL query to execute
            params (Optional[Dict]): Query parameters
            
        Returns:
            pd.DataFrame: Query results as DataFrame
        """
        try:
            if params:
                df = pd.read_sql(query, connection, params=params)
            else:
                df = pd.read_sql(query, connection)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing query to DataFrame: {str(e)}")
            raise
    
    def get_table_metadata(self, connection: oracledb.Connection, 
                          schema: str, table_name: str, include_extended_types: bool = True) -> Dict[str, Any]:
        """
        Get metadata for a specific table including detailed column type information.
        
        Args:
            connection (oracledb.Connection): Database connection
            schema (str): Schema name
            table_name (str): Table name
            include_extended_types (bool): Include extended type information
            
        Returns:
            Dict[str, Any]: Table metadata including columns, constraints, and type details
        """
        try:
            # Get basic column information
            column_query = """
                SELECT 
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    column_id,
                    data_default,
                    char_length,
                    char_used,
                    char_col_decl_length,
                    hidden_column,
                    virtual_column,
                    qualified_col_name
                FROM all_tab_columns 
                WHERE owner = :schema 
                  AND table_name = :table_name
                ORDER BY column_id
            """
            
            columns = self.execute_query(
                connection, 
                column_query, 
                {'schema': schema.upper(), 'table_name': table_name.upper()}
            )
            
            # Get primary key information
            pk_query = """
                SELECT column_name, position
                FROM all_cons_columns c
                JOIN all_constraints con ON c.constraint_name = con.constraint_name
                WHERE con.owner = :schema 
                  AND con.table_name = :table_name
                  AND con.constraint_type = 'P'
                ORDER BY c.position
            """
            
            primary_keys = self.execute_query(
                connection,
                pk_query,
                {'schema': schema.upper(), 'table_name': table_name.upper()}
            )
            
            # Get table statistics
            stats_query = """
                SELECT num_rows, blocks, avg_row_len, last_analyzed
                FROM all_tables
                WHERE owner = :schema 
                  AND table_name = :table_name
            """
            
            table_stats = self.execute_query(
                connection,
                stats_query,
                {'schema': schema.upper(), 'table_name': table_name.upper()}
            )
            
            # Get row count (more accurate than statistics)
            count_query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
            row_count = self.execute_query(connection, count_query)[0][0]
            
            # Build extended column information if requested
            extended_columns = []
            if include_extended_types and columns:
                for col in columns:
                    col_info = {
                        'column_name': col[0],
                        'data_type': col[1],
                        'data_length': col[2],
                        'data_precision': col[3],
                        'data_scale': col[4],
                        'nullable': col[5],
                        'column_id': col[6],
                        'data_default': col[7],
                        'char_length': col[8],
                        'char_used': col[9],
                        'char_col_decl_length': col[10],
                        'hidden_column': col[11],
                        'virtual_column': col[12],
                        'qualified_col_name': col[13]
                    }
                    
                    # Add computed type information using DataTypeHandler
                    try:
                        from db_sentinel_datatypes import DataTypeHandler
                        type_handler = DataTypeHandler()
                        
                        standardized_type = type_handler.identify_data_type(col_info)
                        type_category = type_handler.get_type_category(standardized_type)
                        
                        col_info.update({
                            'standardized_type': standardized_type,
                            'type_category': type_category,
                            'is_numeric': type_category == 'numeric',
                            'is_character': type_category == 'character',
                            'is_datetime': type_category == 'datetime',
                            'is_binary': type_category == 'binary',
                            'is_special': type_category == 'special'
                        })
                    except ImportError:
                        # Fallback if DataTypeHandler is not available
                        col_info.update({
                            'standardized_type': 'unknown',
                            'type_category': 'unknown'
                        })
                    
                    extended_columns.append(col_info)
            
            return {
                'columns': columns,
                'extended_columns': extended_columns,
                'primary_keys': [pk[0] for pk in primary_keys] if primary_keys else [],
                'primary_key_details': primary_keys,
                'row_count': row_count,
                'table_stats': table_stats[0] if table_stats else None,
                'schema': schema,
                'table_name': table_name
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table metadata for {schema}.{table_name}: {str(e)}")
            raise
    
    def test_table_exists(self, connection: oracledb.Connection, 
                         schema: str, table_name: str) -> bool:
        """
        Test if a table exists in the specified schema.
        
        Args:
            connection (oracledb.Connection): Database connection
            schema (str): Schema name
            table_name (str): Table name
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            query = """
                SELECT COUNT(*) 
                FROM all_tables 
                WHERE owner = :schema 
                  AND table_name = :table_name
            """
            
            result = self.execute_query(
                connection, 
                query, 
                {'schema': schema.upper(), 'table_name': table_name.upper()}
            )
            
            return result[0][0] > 0
            
        except Exception as e:
            self.logger.error(f"Error checking if table exists {schema}.{table_name}: {str(e)}")
            return False
    
    def get_table_data_batch(self, connection: oracledb.Connection, 
                           schema: str, table_name: str, primary_keys: List[str],
                           offset: int, batch_size: int, 
                           where_clause: str = "", exclude_columns: List[str] = None) -> pd.DataFrame:
        """
        Get a batch of data from a table for comparison.
        
        Args:
            connection (oracledb.Connection): Database connection
            schema (str): Schema name
            table_name (str): Table name
            primary_keys (List[str]): Primary key columns
            offset (int): Offset for pagination
            batch_size (int): Number of rows to fetch
            where_clause (str): Additional WHERE clause
            exclude_columns (List[str]): Columns to exclude from comparison
            
        Returns:
            pd.DataFrame: Batch of table data
        """
        try:
            # Get all columns except excluded ones
            metadata = self.get_table_metadata(connection, schema, table_name)
            all_columns = [col[0] for col in metadata['columns']]
            
            if exclude_columns:
                columns = [col for col in all_columns if col not in exclude_columns]
            else:
                columns = all_columns
            
            columns_str = ', '.join(columns)
            pk_order = ', '.join(primary_keys)
            
            # Build query with pagination
            query = f"""
                SELECT * FROM (
                    SELECT {columns_str}, 
                           ROW_NUMBER() OVER (ORDER BY {pk_order}) as rn
                    FROM {schema}.{table_name}
                    {f'WHERE {where_clause}' if where_clause else ''}
                ) 
                WHERE rn > :offset AND rn <= :limit
            """
            
            params = {
                'offset': offset,
                'limit': offset + batch_size
            }
            
            df = self.execute_query_to_dataframe(connection, query, params)
            
            # Remove the row number column
            if 'RN' in df.columns:
                df = df.drop('RN', axis=1)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting table data batch: {str(e)}")
            raise
    
    def execute_batch_operations(self, connection: oracledb.Connection, 
                                statements: List[Tuple[str, Dict]]) -> int:
        """
        Execute a batch of SQL operations in a transaction.
        
        Args:
            connection (oracledb.Connection): Database connection
            statements (List[Tuple[str, Dict]]): List of (SQL, params) tuples
            
        Returns:
            int: Number of statements executed successfully
        """
        cursor = None
        executed_count = 0
        
        try:
            cursor = connection.cursor()
            
            for sql, params in statements:
                cursor.execute(sql, params)
                executed_count += 1
            
            connection.commit()
            self.logger.debug(f"Executed {executed_count} batch operations successfully")
            return executed_count
            
        except Exception as e:
            self.logger.error(f"Error in batch operations: {str(e)}")
            if hasattr(connection, 'rollback'):
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
    
    def close_thread_connections(self):
        """Close connections for the current thread."""
        if hasattr(self._thread_local, 'source_conn') and self._thread_local.source_conn:
            try:
                self._source_pool.release(self._thread_local.source_conn)
                self._thread_local.source_conn = None
                self.logger.debug("Released source connection for thread")
            except Exception as e:
                self.logger.warning(f"Error releasing source connection: {str(e)}")
        
        if hasattr(self._thread_local, 'target_conn') and self._thread_local.target_conn:
            try:
                self._target_pool.release(self._thread_local.target_conn)
                self._thread_local.target_conn = None
                self.logger.debug("Released target connection for thread")
            except Exception as e:
                self.logger.warning(f"Error releasing target connection: {str(e)}")
    
    def close_all_connections(self):
        """Close all connection pools and connections."""
        try:
            # Close thread-local connections first
            self.close_thread_connections()
            
            # Close connection pools
            if self._source_pool:
                self._source_pool.close()
                self._source_pool = None
                self.logger.info("Closed source connection pool")
            
            if self._target_pool:
                self._target_pool.close()
                self._target_pool = None
                self.logger.info("Closed target connection pool")
                
        except Exception as e:
            self.logger.error(f"Error closing connections: {str(e)}")
    
    def __del__(self):
        """Cleanup connections when object is destroyed."""
        try:
            self.close_all_connections()
        except:
            pass  # Ignore errors during cleanup
