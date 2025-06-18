#!/usr/bin/env python3
"""
Table Comparator for DB Sentinel Utility

This module handles the core table comparison logic using row-level hashing
for integrity checking between source and target Oracle databases.

Author: Solutions Architect
Version: 1.0
"""

import hashlib
import logging
import threading
from typing import Dict, Any, List, Tuple, Set, Optional
import pandas as pd
import numpy as np
from tqdm import tqdm

from db_sentinel_db import DatabaseManager
from db_sentinel_datatypes import DataTypeHandler


class TableComparator:
    """
    Handles comparison of Oracle tables between source and target databases.
    
    This class implements row-level hashing for efficient comparison of large tables,
    supporting parallel processing and batch-based operations for optimal performance.
    """
    
    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager):
        """
        Initialize the table comparator.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            db_manager (DatabaseManager): Database manager instance
        """
        self.config = config
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self.data_type_handler = DataTypeHandler()
    
    def compare_table(self, table_config: Dict[str, Any], 
                     progress_bar: tqdm, progress_lock: threading.Lock) -> Dict[str, Any]:
        """
        Compare a single table between source and target databases.
        
        This method performs batch-wise comparison using row-level hashing to identify
        differences between source and target tables. It tracks INSERT, UPDATE, and
        DELETE operations needed to synchronize the tables.
        
        Args:
            table_config (Dict[str, Any]): Table configuration
            progress_bar (tqdm): Progress bar for tracking
            progress_lock (threading.Lock): Lock for thread-safe progress updates
            
        Returns:
            Dict[str, Any]: Comparison results including differences and statistics
        """
        table_name = table_config['table_name']
        source_schema = table_config['source_schema']
        target_schema = table_config['target_schema']
        primary_keys = table_config['primary_key']
        batch_size = table_config['batch_size']
        where_clause = table_config.get('where_clause', '')
        exclude_columns = table_config.get('exclude_columns', [])
        hash_algorithm = table_config.get('hash_algorithm', 'sha256')
        
        self.logger.info(f"Starting comparison for table: {source_schema}.{table_name}")
        
        try:
            # Validate table exists in both databases
            source_conn = self.db_manager.get_source_connection()
            target_conn = self.db_manager.get_target_connection()
            
            if not self.db_manager.test_table_exists(source_conn, source_schema, table_name):
                raise ValueError(f"Source table {source_schema}.{table_name} does not exist")
            
            if not self.db_manager.test_table_exists(target_conn, target_schema, table_name):
                raise ValueError(f"Target table {target_schema}.{table_name} does not exist")
            
            # Get table metadata including column types
            source_metadata = self.db_manager.get_table_metadata(source_conn, source_schema, table_name)
            target_metadata = self.db_manager.get_table_metadata(target_conn, target_schema, table_name)
            
            # Get detailed column type information
            source_column_types = self.data_type_handler.get_column_type_info(
                source_conn, source_schema, table_name
            )
            target_column_types = self.data_type_handler.get_column_type_info(
                target_conn, target_schema, table_name
            )
            
            # Validate data type compatibility
            type_validation = self.data_type_handler.validate_data_compatibility(
                source_column_types, target_column_types
            )
            
            if type_validation['errors']:
                self.logger.warning(f"Data type compatibility issues for {table_name}: "
                                   f"{'; '.join(type_validation['errors'])}")
            
            if type_validation['warnings']:
                self.logger.info(f"Data type warnings for {table_name}: "
                                f"{'; '.join(type_validation['warnings'])}")
            
            # Validate primary keys exist in both tables
            self._validate_primary_keys(primary_keys, source_metadata, target_metadata)
            
            # Perform batch comparison with type information
            differences = self._compare_table_batches(
                source_conn, target_conn, table_config, 
                source_metadata, target_metadata, 
                source_column_types, target_column_types,
                progress_bar, progress_lock
            )
            
            # Calculate statistics
            stats = self._calculate_comparison_stats(differences, source_metadata, target_metadata)
            
            result = {
                'status': 'SUCCESS',
                'table_name': table_name,
                'source_schema': source_schema,
                'target_schema': target_schema,
                'primary_keys': primary_keys,
                'differences': differences,
                'statistics': stats,
                'hash_algorithm': hash_algorithm
            }
            
            self.logger.info(f"Completed comparison for table: {table_name} - "
                           f"Inserts: {len(differences['inserts'])}, "
                           f"Updates: {len(differences['updates'])}, "
                           f"Deletes: {len(differences['deletes'])}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error comparing table {table_name}: {str(e)}")
            return {
                'status': 'ERROR',
                'table_name': table_name,
                'error': str(e),
                'differences': {'inserts': [], 'updates': [], 'deletes': []}
            }
        finally:
            # Cleanup thread-specific connections
            self.db_manager.close_thread_connections()
    
    def _validate_primary_keys(self, primary_keys: List[str], 
                              source_metadata: Dict, target_metadata: Dict):
        """
        Validate that primary keys exist in both source and target tables.
        
        Args:
            primary_keys (List[str]): List of primary key columns
            source_metadata (Dict): Source table metadata
            target_metadata (Dict): Target table metadata
            
        Raises:
            ValueError: If primary keys are not found in both tables
        """
        source_columns = {col[0].upper() for col in source_metadata['columns']}
        target_columns = {col[0].upper() for col in target_metadata['columns']}
        
        for pk in primary_keys:
            pk_upper = pk.upper()
            if pk_upper not in source_columns:
                raise ValueError(f"Primary key column '{pk}' not found in source table")
            if pk_upper not in target_columns:
                raise ValueError(f"Primary key column '{pk}' not found in target table")
    
    def _compare_table_batches(self, source_conn, target_conn, table_config: Dict[str, Any],
                              source_metadata: Dict, target_metadata: Dict,
                              source_column_types: Dict[str, Dict], target_column_types: Dict[str, Dict],
                              progress_bar: tqdm, progress_lock: threading.Lock) -> Dict[str, List]:
        """
        Compare table data in batches using row-level hashing.
        
        This method implements the core comparison algorithm:
        1. Fetch data in batches from both databases
        2. Calculate hash for each row
        3. Compare hashes to identify differences
        4. Track INSERT, UPDATE, and DELETE operations
        
        Args:
            source_conn: Source database connection
            target_conn: Target database connection
            table_config (Dict[str, Any]): Table configuration
            source_metadata (Dict): Source table metadata
            target_metadata (Dict): Target table metadata
            progress_bar (tqdm): Progress bar for tracking
            progress_lock (threading.Lock): Lock for thread-safe progress updates
            
        Returns:
            Dict[str, List]: Dictionary containing lists of inserts, updates, and deletes
        """
        table_name = table_config['table_name']
        source_schema = table_config['source_schema']
        target_schema = table_config['target_schema']
        primary_keys = table_config['primary_key']
        batch_size = table_config['batch_size']
        where_clause = table_config.get('where_clause', '')
        exclude_columns = table_config.get('exclude_columns', [])
        hash_algorithm = table_config.get('hash_algorithm', 'sha256')
        
        differences = {
            'inserts': [],  # Rows in source but not in target
            'updates': [],  # Rows with different hashes
            'deletes': []   # Rows in target but not in source
        }
        
        # Calculate total number of batches
        source_row_count = source_metadata['row_count']
        target_row_count = target_metadata['row_count']
        max_row_count = max(source_row_count, target_row_count)
        total_batches = (max_row_count + batch_size - 1) // batch_size
        
        self.logger.info(f"Processing {total_batches} batches for table {table_name}")
        
        # Process batches
        for batch_num in range(total_batches):
            offset = batch_num * batch_size
            
            try:
                # Get source batch
                source_batch = self.db_manager.get_table_data_batch(
                    source_conn, source_schema, table_name, primary_keys,
                    offset, batch_size, where_clause, exclude_columns
                )
                
                # Get target batch
                target_batch = self.db_manager.get_table_data_batch(
                    target_conn, target_schema, table_name, primary_keys,
                    offset, batch_size, where_clause, exclude_columns
                )
                
                # Compare batches with type information
                batch_differences = self._compare_batch_data(
                    source_batch, target_batch, primary_keys, hash_algorithm,
                    source_column_types, target_column_types
                )
                
                # Merge differences
                for key in differences:
                    differences[key].extend(batch_differences[key])
                
                # Update progress
                with progress_lock:
                    progress_bar.update(1)
                
                self.logger.debug(f"Processed batch {batch_num + 1}/{total_batches} for {table_name}")
                
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num} for {table_name}: {str(e)}")
                raise
        
        return differences
    
    def _compare_batch_data(self, source_batch: pd.DataFrame, target_batch: pd.DataFrame,
                           primary_keys: List[str], hash_algorithm: str,
                           source_column_types: Dict[str, Dict], target_column_types: Dict[str, Dict]) -> Dict[str, List]:
        """
        Compare two data batches using row-level hashing.
        
        This method performs the actual comparison logic:
        1. Calculate hash for each row in both batches
        2. Create hash maps keyed by primary key
        3. Identify inserts, updates, and deletes
        
        Args:
            source_batch (pd.DataFrame): Source data batch
            target_batch (pd.DataFrame): Target data batch
            primary_keys (List[str]): Primary key columns
            hash_algorithm (str): Hash algorithm to use
            
        Returns:
            Dict[str, List]: Batch differences (inserts, updates, deletes)
        """
        batch_differences = {
            'inserts': [],
            'updates': [],
            'deletes': []
        }
        
        # Handle empty batches
        if source_batch.empty and target_batch.empty:
            return batch_differences
        
        # Calculate hashes for source batch
        source_hashes = {}
        if not source_batch.empty:
            source_hashes = self._calculate_row_hashes(
                source_batch, primary_keys, hash_algorithm, source_column_types
            )
        
        # Calculate hashes for target batch
        target_hashes = {}
        if not target_batch.empty:
            target_hashes = self._calculate_row_hashes(
                target_batch, primary_keys, hash_algorithm, target_column_types
            )
        
        # Get primary key sets
        source_pks = set(source_hashes.keys())
        target_pks = set(target_hashes.keys())
        
        # Find inserts (in source but not in target)
        insert_pks = source_pks - target_pks
        for pk in insert_pks:
            row_data = self._get_row_by_pk(source_batch, primary_keys, pk)
            if row_data is not None:
                batch_differences['inserts'].append({
                    'primary_key': pk,
                    'data': row_data,
                    'hash': source_hashes[pk]
                })
        
        # Find deletes (in target but not in source)
        delete_pks = target_pks - source_pks
        for pk in delete_pks:
            row_data = self._get_row_by_pk(target_batch, primary_keys, pk)
            if row_data is not None:
                batch_differences['deletes'].append({
                    'primary_key': pk,
                    'data': row_data,
                    'hash': target_hashes[pk]
                })
        
        # Find updates (same primary key but different hash)
        common_pks = source_pks & target_pks
        for pk in common_pks:
            if source_hashes[pk] != target_hashes[pk]:
                source_row = self._get_row_by_pk(source_batch, primary_keys, pk)
                target_row = self._get_row_by_pk(target_batch, primary_keys, pk)
                
                if source_row is not None and target_row is not None:
                    batch_differences['updates'].append({
                        'primary_key': pk,
                        'source_data': source_row,
                        'target_data': target_row,
                        'source_hash': source_hashes[pk],
                        'target_hash': target_hashes[pk]
                    })
        
        return batch_differences
    
    def _calculate_row_hashes(self, df: pd.DataFrame, primary_keys: List[str], 
                             hash_algorithm: str, column_types: Dict[str, Dict]) -> Dict[Tuple, str]:
        """
        Calculate hash for each row in the DataFrame using proper data type handling.
        
        The hashing algorithm normalizes all column values based on their data types
        and calculates a hash. Primary keys are used as the map key.
        
        Args:
            df (pd.DataFrame): DataFrame to hash
            primary_keys (List[str]): Primary key columns
            hash_algorithm (str): Hash algorithm to use
            column_types (Dict[str, Dict]): Column type information
            
        Returns:
            Dict[Tuple, str]: Dictionary mapping primary key tuples to row hashes
        """
        row_hashes = {}
        
        # Get non-primary key columns for hashing
        hash_columns = [col for col in df.columns if col.upper() not in [pk.upper() for pk in primary_keys]]
        
        # Select hash algorithm
        if hash_algorithm == 'md5':
            hash_class = hashlib.md5
        elif hash_algorithm == 'sha1':
            hash_class = hashlib.sha1
        elif hash_algorithm == 'sha256':
            hash_class = hashlib.sha256
        else:
            hash_class = hashlib.sha256  # Default to SHA256
        
        for _, row in df.iterrows():
            # Create primary key tuple
            pk_values = tuple(row[pk] for pk in primary_keys)
            
            # Create hash string from non-PK columns using proper data type normalization
            hash_components = []
            
            for col in hash_columns:
                value = row[col]
                
                # Get data type information for this column
                col_type_info = column_types.get(col, {})
                data_type = col_type_info.get('standardized_type', 'unknown')
                
                # Normalize value based on its data type
                try:
                    normalized_value = self.data_type_handler.normalize_value_for_hashing(value, data_type)
                    hash_components.append(f"{col}:{normalized_value}")
                except Exception as e:
                    self.logger.warning(f"Error normalizing column {col} with type {data_type}: {str(e)}")
                    # Fallback to basic string conversion
                    if pd.isna(value) or value is None:
                        hash_components.append(f"{col}:NULL")
                    else:
                        hash_components.append(f"{col}:{str(value)}")
            
            # Join all components with a separator
            hash_string = "|".join(hash_components)
            
            # Calculate hash
            hash_obj = hash_class()
            hash_obj.update(hash_string.encode('utf-8'))
            row_hash = hash_obj.hexdigest()
            
            row_hashes[pk_values] = row_hash
        
        return row_hashes
    
    def _get_row_by_pk(self, df: pd.DataFrame, primary_keys: List[str], 
                      pk_values: Tuple) -> Optional[Dict]:
        """
        Get a row from DataFrame by primary key values.
        
        Args:
            df (pd.DataFrame): DataFrame to search
            primary_keys (List[str]): Primary key columns
            pk_values (Tuple): Primary key values to match
            
        Returns:
            Optional[Dict]: Row data as dictionary or None if not found
        """
        try:
            # Create boolean mask for primary key match
            mask = pd.Series([True] * len(df))
            
            for i, pk_col in enumerate(primary_keys):
                mask &= (df[pk_col] == pk_values[i])
            
            matching_rows = df[mask]
            
            if len(matching_rows) == 1:
                # Convert row to dictionary, handling NaN values
                row_dict = matching_rows.iloc[0].to_dict()
                
                # Convert NaN to None for consistency
                for key, value in row_dict.items():
                    if pd.isna(value):
                        row_dict[key] = None
                
                return row_dict
            elif len(matching_rows) > 1:
                self.logger.warning(f"Multiple rows found for primary key {pk_values}")
                return matching_rows.iloc[0].to_dict()
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting row by primary key {pk_values}: {str(e)}")
            return None
    
    def _calculate_comparison_stats(self, differences: Dict[str, List], 
                                   source_metadata: Dict, target_metadata: Dict) -> Dict[str, Any]:
        """
        Calculate comparison statistics.
        
        Args:
            differences (Dict[str, List]): Comparison differences
            source_metadata (Dict): Source table metadata
            target_metadata (Dict): Target table metadata
            
        Returns:
            Dict[str, Any]: Comparison statistics
        """
        stats = {
            'source_row_count': source_metadata['row_count'],
            'target_row_count': target_metadata['row_count'],
            'insert_count': len(differences['inserts']),
            'update_count': len(differences['updates']),
            'delete_count': len(differences['deletes']),
            'total_differences': len(differences['inserts']) + len(differences['updates']) + len(differences['deletes']),
            'match_percentage': 0.0
        }
        
        # Calculate match percentage
        total_rows = max(stats['source_row_count'], stats['target_row_count'])
        if total_rows > 0:
            matched_rows = total_rows - stats['total_differences']
            stats['match_percentage'] = (matched_rows / total_rows) * 100
        
        return stats
