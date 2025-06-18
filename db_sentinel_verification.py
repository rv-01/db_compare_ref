#!/usr/bin/env python3
"""
Post Verifier for DB Sentinel Utility

This module performs post-comparison verification of generated SQL statements
to ensure their validity and prevent constraint violations during synchronization.

Author: Solutions Architect
Version: 1.0
"""

import logging
from typing import Dict, Any, List, Tuple, Set, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime

from db_sentinel_db import DatabaseManager
from db_sentinel_datatypes import DataTypeHandler


class PostVerifier:
    """
    Performs post-verification of generated SQL statements.
    
    This class validates INSERT, UPDATE, and DELETE statements by checking
    current database state to prevent constraint violations and ensure
    synchronization integrity.
    """
    
    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager, run_timestamp: str):
        """
        Initialize the post verifier.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            db_manager (DatabaseManager): Database manager instance
            run_timestamp (str): Timestamp for the current run
        """
        self.config = config
        self.db_manager = db_manager
        self.run_timestamp = run_timestamp
        self.logger = logging.getLogger(__name__)
        self.data_type_handler = DataTypeHandler()
        
        # Create verified SQL directory
        self.sql_dir = Path("DB_Sentinel_sql")
        self.sql_dir.mkdir(exist_ok=True)
    
    def verify_table_sqls(self, table_name: str, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify SQL statements for a single table.
        
        This method performs comprehensive verification of all generated SQL statements:
        1. For INSERTs: Check if primary key already exists in target
        2. For UPDATEs: Check if row exists in target and verify current values
        3. For DELETEs: Check if row exists in target
        4. Generate new verified SQL files with only valid statements
        
        Args:
            table_name (str): Name of the table
            comparison_result (Dict[str, Any]): Comparison results for the table
            
        Returns:
            Dict[str, Any]: Verification results with statistics and validated operations
        """
        try:
            self.logger.info(f"Starting post-verification for table: {table_name}")
            
            # Extract necessary information
            differences = comparison_result['differences']
            source_schema = comparison_result['source_schema']
            target_schema = comparison_result['target_schema']
            primary_keys = comparison_result['primary_keys']
            
            # Verify each operation type
            verified_inserts = self._verify_insert_operations(
                table_name, target_schema, primary_keys, differences.get('inserts', [])
            )
            
            verified_updates = self._verify_update_operations(
                table_name, source_schema, target_schema, primary_keys, differences.get('updates', [])
            )
            
            verified_deletes = self._verify_delete_operations(
                table_name, target_schema, primary_keys, differences.get('deletes', [])
            )
            
            # Generate verified SQL files
            verified_sql_files = self._generate_verified_sql_files(
                table_name, source_schema, target_schema, primary_keys,
                verified_inserts, verified_updates, verified_deletes
            )
            
            # Calculate verification statistics
            verification_stats = self._calculate_verification_statistics(
                differences, verified_inserts, verified_updates, verified_deletes
            )
            
            verification_result = {
                'status': 'SUCCESS',
                'table_name': table_name,
                'verified_operations': {
                    'inserts': verified_inserts,
                    'updates': verified_updates,
                    'deletes': verified_deletes
                },
                'statistics': verification_stats,
                'generated_files': verified_sql_files
            }
            
            self.logger.info(f"Post-verification completed for table: {table_name} - "
                           f"Valid operations: {verification_stats['total_valid_operations']}")
            
            return verification_result
            
        except Exception as e:
            self.logger.error(f"Error in post-verification for table {table_name}: {str(e)}")
            return {
                'status': 'ERROR',
                'table_name': table_name,
                'error': str(e),
                'verified_operations': {'inserts': [], 'updates': [], 'deletes': []},
                'statistics': {}
            }
    
    def _verify_insert_operations(self, table_name: str, target_schema: str, 
                                 primary_keys: List[str], inserts: List[Dict]) -> List[Dict]:
        """
        Verify INSERT operations by checking if primary keys already exist in target.
        
        Args:
            table_name (str): Table name
            target_schema (str): Target schema
            primary_keys (List[str]): Primary key columns
            inserts (List[Dict]): List of insert operations
            
        Returns:
            List[Dict]: List of valid insert operations
        """
        if not inserts:
            return []
        
        valid_inserts = []
        target_conn = self.db_manager.get_target_connection()
        
        try:
            self.logger.debug(f"Verifying {len(inserts)} insert operations for {table_name}")
            
            # Batch check existing primary keys
            existing_pks = self._get_existing_primary_keys(
                target_conn, target_schema, table_name, primary_keys,
                [insert['primary_key'] for insert in inserts]
            )
            
            for insert in inserts:
                pk_values = insert['primary_key']
                
                # Check if primary key already exists
                if pk_values not in existing_pks:
                    # Additional validation: check data types and constraints
                    if self._validate_insert_data(target_conn, target_schema, table_name, insert['data']):
                        valid_inserts.append({
                            **insert,
                            'verification_status': 'VALID',
                            'verification_reason': 'Primary key does not exist in target'
                        })
                    else:
                        self.logger.warning(f"Insert data validation failed for PK {pk_values} in {table_name}")
                        valid_inserts.append({
                            **insert,
                            'verification_status': 'INVALID',
                            'verification_reason': 'Data validation failed'
                        })
                else:
                    self.logger.debug(f"Primary key {pk_values} already exists in {table_name}, skipping insert")
                    valid_inserts.append({
                        **insert,
                        'verification_status': 'INVALID',
                        'verification_reason': 'Primary key already exists in target'
                    })
            
            valid_count = sum(1 for insert in valid_inserts if insert['verification_status'] == 'VALID')
            self.logger.info(f"Insert verification for {table_name}: {valid_count}/{len(inserts)} valid operations")
            
            return valid_inserts
            
        except Exception as e:
            self.logger.error(f"Error verifying insert operations for {table_name}: {str(e)}")
            # Return original inserts with error status
            return [{**insert, 'verification_status': 'ERROR', 'verification_reason': str(e)} 
                   for insert in inserts]
    
    def _verify_update_operations(self, table_name: str, source_schema: str, target_schema: str,
                                 primary_keys: List[str], updates: List[Dict]) -> List[Dict]:
        """
        Verify UPDATE operations by checking if rows exist and current values.
        
        Args:
            table_name (str): Table name
            source_schema (str): Source schema
            target_schema (str): Target schema
            primary_keys (List[str]): Primary key columns
            updates (List[Dict]): List of update operations
            
        Returns:
            List[Dict]: List of valid update operations
        """
        if not updates:
            return []
        
        valid_updates = []
        target_conn = self.db_manager.get_target_connection()
        
        # Get column type information for proper comparison
        target_column_types = self.data_type_handler.get_column_type_info(
            target_conn, target_schema, table_name
        )
        
        try:
            self.logger.debug(f"Verifying {len(updates)} update operations for {table_name}")
            
            for update in updates:
                pk_values = update['primary_key']
                source_data = update['source_data']
                target_data = update['target_data']
                
                # Check if row exists in target
                current_row = self._get_current_row_data(
                    target_conn, target_schema, table_name, primary_keys, pk_values
                )
                
                if current_row is not None:
                    # Compare current data with expected target data using type-aware comparison
                    if self._compare_row_data(current_row, target_data, target_column_types):
                        # Row exists and matches expected data, update is valid
                        valid_updates.append({
                            **update,
                            'verification_status': 'VALID',
                            'verification_reason': 'Row exists and matches expected data',
                            'current_data': current_row
                        })
                    else:
                        # Row exists but data has changed since comparison
                        self.logger.warning(f"Row data changed since comparison for PK {pk_values} in {table_name}")
                        valid_updates.append({
                            **update,
                            'verification_status': 'INVALID',
                            'verification_reason': 'Row data changed since comparison',
                            'current_data': current_row
                        })
                else:
                    # Row doesn't exist in target (was deleted?)
                    self.logger.warning(f"Row with PK {pk_values} not found in target {table_name}")
                    valid_updates.append({
                        **update,
                        'verification_status': 'INVALID',
                        'verification_reason': 'Row not found in target'
                    })
            
            valid_count = sum(1 for update in valid_updates if update['verification_status'] == 'VALID')
            self.logger.info(f"Update verification for {table_name}: {valid_count}/{len(updates)} valid operations")
            
            return valid_updates
            
        except Exception as e:
            self.logger.error(f"Error verifying update operations for {table_name}: {str(e)}")
            return [{**update, 'verification_status': 'ERROR', 'verification_reason': str(e)} 
                   for update in updates]
    
    def _verify_delete_operations(self, table_name: str, target_schema: str,
                                 primary_keys: List[str], deletes: List[Dict]) -> List[Dict]:
        """
        Verify DELETE operations by checking if rows exist in target.
        
        Args:
            table_name (str): Table name
            target_schema (str): Target schema
            primary_keys (List[str]): Primary key columns
            deletes (List[Dict]): List of delete operations
            
        Returns:
            List[Dict]: List of valid delete operations
        """
        if not deletes:
            return []
        
        valid_deletes = []
        target_conn = self.db_manager.get_target_connection()
        
        try:
            self.logger.debug(f"Verifying {len(deletes)} delete operations for {table_name}")
            
            # Batch check existing primary keys
            existing_pks = self._get_existing_primary_keys(
                target_conn, target_schema, table_name, primary_keys,
                [delete['primary_key'] for delete in deletes]
            )
            
            for delete in deletes:
                pk_values = delete['primary_key']
                
                # Check if row exists in target
                if pk_values in existing_pks:
                    valid_deletes.append({
                        **delete,
                        'verification_status': 'VALID',
                        'verification_reason': 'Row exists in target'
                    })
                else:
                    self.logger.debug(f"Row with PK {pk_values} not found in target {table_name}, skipping delete")
                    valid_deletes.append({
                        **delete,
                        'verification_status': 'INVALID',
                        'verification_reason': 'Row not found in target'
                    })
            
            valid_count = sum(1 for delete in valid_deletes if delete['verification_status'] == 'VALID')
            self.logger.info(f"Delete verification for {table_name}: {valid_count}/{len(deletes)} valid operations")
            
            return valid_deletes
            
        except Exception as e:
            self.logger.error(f"Error verifying delete operations for {table_name}: {str(e)}")
            return [{**delete, 'verification_status': 'ERROR', 'verification_reason': str(e)} 
                   for delete in deletes]
    
    def _get_existing_primary_keys(self, connection, schema: str, table_name: str,
                                  primary_keys: List[str], pk_values_list: List[Tuple]) -> Set[Tuple]:
        """
        Get existing primary keys from the database in batch.
        
        Args:
            connection: Database connection
            schema (str): Schema name
            table_name (str): Table name
            primary_keys (List[str]): Primary key columns
            pk_values_list (List[Tuple]): List of primary key values to check
            
        Returns:
            Set[Tuple]: Set of existing primary key tuples
        """
        if not pk_values_list:
            return set()
        
        try:
            # Build query to check existing primary keys
            pk_columns = ', '.join([f'"{pk}"' for pk in primary_keys])
            
            # Create WHERE clause for batch checking
            where_conditions = []
            for pk_values in pk_values_list:
                conditions = []
                for i, pk_col in enumerate(primary_keys):
                    value = pk_values[i] if i < len(pk_values) else None
                    if value is None:
                        conditions.append(f'"{pk_col}" IS NULL')
                    elif isinstance(value, str):
                        conditions.append(f'"{pk_col}" = \'{value.replace("\'", "\'\'")}\'')
                    else:
                        conditions.append(f'"{pk_col}" = {value}')
                
                where_conditions.append(f"({' AND '.join(conditions)})")
            
            # Limit batch size to avoid query length issues
            batch_size = 1000
            existing_pks = set()
            
            for i in range(0, len(where_conditions), batch_size):
                batch_conditions = where_conditions[i:i + batch_size]
                where_clause = ' OR '.join(batch_conditions)
                
                query = f"""
                    SELECT {pk_columns}
                    FROM "{schema}"."{table_name}"
                    WHERE {where_clause}
                """
                
                result = self.db_manager.execute_query(connection, query)
                
                for row in result:
                    existing_pks.add(tuple(row))
            
            return existing_pks
            
        except Exception as e:
            self.logger.error(f"Error getting existing primary keys: {str(e)}")
            return set()
    
    def _get_current_row_data(self, connection, schema: str, table_name: str,
                             primary_keys: List[str], pk_values: Tuple) -> Optional[Dict]:
        """
        Get current row data for the specified primary key.
        
        Args:
            connection: Database connection
            schema (str): Schema name
            table_name (str): Table name
            primary_keys (List[str]): Primary key columns
            pk_values (Tuple): Primary key values
            
        Returns:
            Optional[Dict]: Current row data or None if not found
        """
        try:
            # Build WHERE clause
            where_conditions = []
            for i, pk_col in enumerate(primary_keys):
                value = pk_values[i] if i < len(pk_values) else None
                if value is None:
                    where_conditions.append(f'"{pk_col}" IS NULL')
                elif isinstance(value, str):
                    where_conditions.append(f'"{pk_col}" = \'{value.replace("\'", "\'\'")}\'')
                else:
                    where_conditions.append(f'"{pk_col}" = {value}')
            
            where_clause = ' AND '.join(where_conditions)
            
            query = f'SELECT * FROM "{schema}"."{table_name}" WHERE {where_clause}'
            
            df = self.db_manager.execute_query_to_dataframe(connection, query)
            
            if len(df) == 1:
                # Convert to dictionary and handle NaN values
                row_dict = df.iloc[0].to_dict()
                for key, value in row_dict.items():
                    if pd.isna(value):
                        row_dict[key] = None
                return row_dict
            elif len(df) > 1:
                self.logger.warning(f"Multiple rows found for primary key {pk_values}")
                return df.iloc[0].to_dict()
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting current row data: {str(e)}")
            return None
    
    def _compare_row_data(self, current_data: Dict, expected_data: Dict, 
                         column_types: Dict[str, Dict] = None) -> bool:
        """
        Compare current row data with expected data using data type-aware comparison.
        
        Args:
            current_data (Dict): Current data from database
            expected_data (Dict): Expected data from comparison
            column_types (Dict[str, Dict]): Column type information
            
        Returns:
            bool: True if data matches, False otherwise
        """
        try:
            # Compare all common columns
            for col in expected_data.keys():
                if col in current_data:
                    current_val = current_data[col]
                    expected_val = expected_data[col]
                    
                    # Get data type information
                    data_type = 'unknown'
                    if column_types and col in column_types:
                        data_type = column_types[col].get('standardized_type', 'unknown')
                    
                    # Use data type-aware comparison
                    if not self.data_type_handler.compare_values(current_val, expected_val, data_type):
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error comparing row data: {str(e)}")
            return False
    
    def _validate_insert_data(self, connection, schema: str, table_name: str, data: Dict) -> bool:
        """
        Validate insert data for basic constraints and data types.
        
        Args:
            connection: Database connection
            schema (str): Schema name
            table_name (str): Table name
            data (Dict): Data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        try:
            # Basic validation - check for required fields and data types
            # This is a simplified validation - can be extended based on requirements
            
            # Check for None values in data
            for col, value in data.items():
                # Skip validation for None values (will be handled by database constraints)
                if value is None:
                    continue
                
                # Basic string length check (arbitrary limit)
                if isinstance(value, str) and len(value) > 4000:
                    self.logger.warning(f"String value too long for column {col}: {len(value)} characters")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating insert data: {str(e)}")
            return False
    
    def _generate_verified_sql_files(self, table_name: str, source_schema: str, target_schema: str,
                                   primary_keys: List[str], verified_inserts: List[Dict],
                                   verified_updates: List[Dict], verified_deletes: List[Dict]) -> List[str]:
        """
        Generate verified SQL files containing only valid operations.
        
        Args:
            table_name (str): Table name
            source_schema (str): Source schema
            target_schema (str): Target schema
            primary_keys (List[str]): Primary key columns
            verified_inserts (List[Dict]): Verified insert operations
            verified_updates (List[Dict]): Verified update operations
            verified_deletes (List[Dict]): Verified delete operations
            
        Returns:
            List[str]: List of generated SQL file paths
        """
        generated_files = []
        
        try:
            # Generate verified source SQL
            source_sql = self._generate_verified_source_sql(
                table_name, source_schema, target_schema, primary_keys,
                verified_inserts, verified_updates, verified_deletes
            )
            
            source_file = f"verified_source_{table_name}_sync_{self.run_timestamp}.sql"
            source_path = self.sql_dir / source_file
            
            with open(source_path, 'w', encoding='utf-8') as f:
                f.write(source_sql)
            
            generated_files.append(str(source_path))
            
            # Generate verified target SQL
            target_sql = self._generate_verified_target_sql(
                table_name, source_schema, target_schema, primary_keys,
                verified_inserts, verified_updates, verified_deletes
            )
            
            target_file = f"verified_target_{table_name}_sync_{self.run_timestamp}.sql"
            target_path = self.sql_dir / target_file
            
            with open(target_path, 'w', encoding='utf-8') as f:
                f.write(target_sql)
            
            generated_files.append(str(target_path))
            
            self.logger.info(f"Generated verified SQL files for {table_name}: {generated_files}")
            
            return generated_files
            
        except Exception as e:
            self.logger.error(f"Error generating verified SQL files: {str(e)}")
            return []
    
    def _generate_verified_source_sql(self, table_name: str, source_schema: str, target_schema: str,
                                     primary_keys: List[str], verified_inserts: List[Dict],
                                     verified_updates: List[Dict], verified_deletes: List[Dict]) -> str:
        """Generate verified SQL for source database synchronization."""
        from db_sentinel_sql_generator import SQLGenerator
        
        # Filter only valid operations
        valid_inserts = [op for op in verified_deletes if op.get('verification_status') == 'VALID']
        valid_updates = [op for op in verified_updates if op.get('verification_status') == 'VALID']
        valid_deletes = [op for op in verified_inserts if op.get('verification_status') == 'VALID']
        
        sql_gen = SQLGenerator(self.config, self.run_timestamp)
        
        # Create differences dict with verified operations
        verified_differences = {
            'inserts': valid_deletes,  # Reversed for source
            'updates': valid_updates,
            'deletes': valid_inserts   # Reversed for source
        }
        
        return sql_gen._generate_source_sql(
            table_name, source_schema, target_schema, primary_keys, verified_differences
        )
    
    def _generate_verified_target_sql(self, table_name: str, source_schema: str, target_schema: str,
                                     primary_keys: List[str], verified_inserts: List[Dict],
                                     verified_updates: List[Dict], verified_deletes: List[Dict]) -> str:
        """Generate verified SQL for target database synchronization."""
        from db_sentinel_sql_generator import SQLGenerator
        
        # Filter only valid operations
        valid_inserts = [op for op in verified_inserts if op.get('verification_status') == 'VALID']
        valid_updates = [op for op in verified_updates if op.get('verification_status') == 'VALID']
        valid_deletes = [op for op in verified_deletes if op.get('verification_status') == 'VALID']
        
        sql_gen = SQLGenerator(self.config, self.run_timestamp)
        
        # Create differences dict with verified operations
        verified_differences = {
            'inserts': valid_inserts,
            'updates': valid_updates,
            'deletes': valid_deletes
        }
        
        return sql_gen._generate_target_sql(
            table_name, source_schema, target_schema, primary_keys, verified_differences
        )
    
    def _calculate_verification_statistics(self, original_differences: Dict[str, List],
                                         verified_inserts: List[Dict], verified_updates: List[Dict],
                                         verified_deletes: List[Dict]) -> Dict[str, Any]:
        """
        Calculate verification statistics.
        
        Args:
            original_differences (Dict[str, List]): Original comparison differences
            verified_inserts (List[Dict]): Verified insert operations
            verified_updates (List[Dict]): Verified update operations
            verified_deletes (List[Dict]): Verified delete operations
            
        Returns:
            Dict[str, Any]: Verification statistics
        """
        # Count original operations
        original_inserts = len(original_differences.get('inserts', []))
        original_updates = len(original_differences.get('updates', []))
        original_deletes = len(original_differences.get('deletes', []))
        original_total = original_inserts + original_updates + original_deletes
        
        # Count valid operations
        valid_inserts = sum(1 for op in verified_inserts if op.get('verification_status') == 'VALID')
        valid_updates = sum(1 for op in verified_updates if op.get('verification_status') == 'VALID')
        valid_deletes = sum(1 for op in verified_deletes if op.get('verification_status') == 'VALID')
        valid_total = valid_inserts + valid_updates + valid_deletes
        
        # Count invalid operations
        invalid_inserts = original_inserts - valid_inserts
        invalid_updates = original_updates - valid_updates
        invalid_deletes = original_deletes - valid_deletes
        invalid_total = invalid_inserts + invalid_updates + invalid_deletes
        
        # Calculate percentages
        verification_percentage = (valid_total / original_total * 100) if original_total > 0 else 100.0
        
        return {
            'original_operations': {
                'inserts': original_inserts,
                'updates': original_updates,
                'deletes': original_deletes,
                'total': original_total
            },
            'valid_operations': {
                'inserts': valid_inserts,
                'updates': valid_updates,
                'deletes': valid_deletes,
                'total': valid_total
            },
            'invalid_operations': {
                'inserts': invalid_inserts,
                'updates': invalid_updates,
                'deletes': invalid_deletes,
                'total': invalid_total
            },
            'verification_percentage': verification_percentage,
            'total_valid_operations': valid_total,
            'total_invalid_operations': invalid_total
        }
