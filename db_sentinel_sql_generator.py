#!/usr/bin/env python3
"""
SQL Generator for DB Sentinel Utility

This module generates INSERT, UPDATE, and DELETE SQL statements based on
table comparison results for synchronizing source and target databases.

Author: Solutions Architect
Version: 1.0
"""

import os
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import re

from db_sentinel_datatypes import DataTypeHandler


class SQLGenerator:
    """
    Generates SQL statements for synchronizing tables between databases.
    
    This class creates INSERT, UPDATE, and DELETE statements based on comparison
    results, handling different data types and generating separate files for
    source and target database synchronization.
    """
    
    def __init__(self, config: Dict[str, Any], run_timestamp: str):
        """
        Initialize the SQL generator.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            run_timestamp (str): Timestamp for the current run
        """
        self.config = config
        self.run_timestamp = run_timestamp
        self.logger = logging.getLogger(__name__)
        self.data_type_handler = DataTypeHandler()
        
        # Create SQL output directory
        self.sql_dir = Path("DB_Sentinel_sql")
        self.sql_dir.mkdir(exist_ok=True)
    
    def generate_sql_files(self, table_name: str, comparison_result: Dict[str, Any], 
                          db_manager=None):
        """
        Generate SQL files for table synchronization.
        
        This method creates separate SQL files for source and target databases
        containing all necessary INSERT, UPDATE, and DELETE statements.
        
        Args:
            table_name (str): Name of the table being synchronized
            comparison_result (Dict[str, Any]): Comparison results containing differences
            db_manager: Database manager instance for getting column types
        """
        try:
            self.logger.info(f"Generating SQL files for table: {table_name}")
            
            # Extract necessary information
            differences = comparison_result['differences']
            source_schema = comparison_result['source_schema']
            target_schema = comparison_result['target_schema']
            primary_keys = comparison_result['primary_keys']
            
            # Get column type information if database manager is available
            source_column_types = {}
            target_column_types = {}
            
            if db_manager:
                try:
                    source_conn = db_manager.get_source_connection()
                    target_conn = db_manager.get_target_connection()
                    
                    source_column_types = self.data_type_handler.get_column_type_info(
                        source_conn, source_schema, table_name
                    )
                    target_column_types = self.data_type_handler.get_column_type_info(
                        target_conn, target_schema, table_name
                    )
                except Exception as e:
                    self.logger.warning(f"Could not get column types for {table_name}: {str(e)}")
            
            # Generate SQL for source database (to make it match target)
            source_sql = self._generate_source_sql(
                table_name, source_schema, target_schema, primary_keys, differences,
                source_column_types
            )
            
            # Generate SQL for target database (to make it match source)
            target_sql = self._generate_target_sql(
                table_name, source_schema, target_schema, primary_keys, differences,
                target_column_types
            )
            
            # Write SQL files
            self._write_sql_file(table_name, "source", source_sql)
            self._write_sql_file(table_name, "target", target_sql)
            
            self.logger.info(f"SQL files generated successfully for table: {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error generating SQL files for {table_name}: {str(e)}")
            raise
    
    def _generate_source_sql(self, table_name: str, source_schema: str, target_schema: str,
                            primary_keys: List[str], differences: Dict[str, List],
                            column_types: Dict[str, Dict] = None) -> str:
        """
        Generate SQL to make source database match target database.
        
        For source database synchronization:
        - INSERT rows that exist in target but not in source (reverse of normal inserts)
        - UPDATE rows to match target values
        - DELETE rows that exist in source but not in target (reverse of normal deletes)
        
        Args:
            table_name (str): Table name
            source_schema (str): Source schema name
            target_schema (str): Target schema name
            primary_keys (List[str]): Primary key columns
            differences (Dict[str, List]): Comparison differences
            
        Returns:
            str: SQL statements for source database
        """
        sql_statements = []
        
        # Add header comment
        sql_statements.append(f"-- SQL to synchronize source table {source_schema}.{table_name}")
        sql_statements.append(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_statements.append(f"-- Run ID: {self.run_timestamp}")
        sql_statements.append("-- This SQL makes the source table match the target table")
        sql_statements.append("")
        
        # Add transaction start
        sql_statements.append("-- Start transaction")
        sql_statements.append("-- COMMIT or ROLLBACK at the end based on validation")
        sql_statements.append("")
        
        # Generate DELETE statements (rows that exist in source but not in target)
        if differences['inserts']:  # These are "inserts" from target perspective, so "deletes" for source
            sql_statements.append(f"-- DELETE statements ({len(differences['inserts'])} rows)")
            sql_statements.append("-- Remove rows that exist in source but not in target")
            
            for item in differences['inserts']:
                delete_sql = self._generate_delete_sql(
                    source_schema, table_name, primary_keys, item['primary_key'], column_types
                )
                sql_statements.append(delete_sql)
        
        sql_statements.append("")
        
        # Generate INSERT statements (rows that exist in target but not in source)
        if differences['deletes']:  # These are "deletes" from target perspective, so "inserts" for source
            sql_statements.append(f"-- INSERT statements ({len(differences['deletes'])} rows)")
            sql_statements.append("-- Add rows that exist in target but not in source")
            
            for item in differences['deletes']:
                insert_sql = self._generate_insert_sql(
                    source_schema, table_name, item['data'], column_types
                )
                sql_statements.append(insert_sql)
        
        sql_statements.append("")
        
        # Generate UPDATE statements (rows with different values)
        if differences['updates']:
            sql_statements.append(f"-- UPDATE statements ({len(differences['updates'])} rows)")
            sql_statements.append("-- Update rows to match target values")
            
            for item in differences['updates']:
                update_sql = self._generate_update_sql(
                    source_schema, table_name, primary_keys, 
                    item['primary_key'], item['target_data'], column_types  # Use target data for source update
                )
                sql_statements.append(update_sql)
        
        sql_statements.append("")
        sql_statements.append("-- End of synchronization SQL")
        sql_statements.append("-- COMMIT;")
        
        return "\n".join(sql_statements)
    
    def _generate_target_sql(self, table_name: str, source_schema: str, target_schema: str,
                            primary_keys: List[str], differences: Dict[str, List],
                            column_types: Dict[str, Dict] = None) -> str:
        """
        Generate SQL to make target database match source database.
        
        For target database synchronization:
        - INSERT rows that exist in source but not in target
        - UPDATE rows to match source values
        - DELETE rows that exist in target but not in source
        
        Args:
            table_name (str): Table name
            source_schema (str): Source schema name
            target_schema (str): Target schema name
            primary_keys (List[str]): Primary key columns
            differences (Dict[str, List]): Comparison differences
            
        Returns:
            str: SQL statements for target database
        """
        sql_statements = []
        
        # Add header comment
        sql_statements.append(f"-- SQL to synchronize target table {target_schema}.{table_name}")
        sql_statements.append(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_statements.append(f"-- Run ID: {self.run_timestamp}")
        sql_statements.append("-- This SQL makes the target table match the source table")
        sql_statements.append("")
        
        # Add transaction start
        sql_statements.append("-- Start transaction")
        sql_statements.append("-- COMMIT or ROLLBACK at the end based on validation")
        sql_statements.append("")
        
        # Generate DELETE statements (rows that exist in target but not in source)
        if differences['deletes']:
            sql_statements.append(f"-- DELETE statements ({len(differences['deletes'])} rows)")
            sql_statements.append("-- Remove rows that exist in target but not in source")
            
            for item in differences['deletes']:
                delete_sql = self._generate_delete_sql(
                    target_schema, table_name, primary_keys, item['primary_key'], column_types
                )
                sql_statements.append(delete_sql)
        
        sql_statements.append("")
        
        # Generate INSERT statements (rows that exist in source but not in target)
        if differences['inserts']:
            sql_statements.append(f"-- INSERT statements ({len(differences['inserts'])} rows)")
            sql_statements.append("-- Add rows that exist in source but not in target")
            
            for item in differences['inserts']:
                insert_sql = self._generate_insert_sql(
                    target_schema, table_name, item['data'], column_types
                )
                sql_statements.append(insert_sql)
        
        sql_statements.append("")
        
        # Generate UPDATE statements (rows with different values)
        if differences['updates']:
            sql_statements.append(f"-- UPDATE statements ({len(differences['updates'])} rows)")
            sql_statements.append("-- Update rows to match source values")
            
            for item in differences['updates']:
                update_sql = self._generate_update_sql(
                    target_schema, table_name, primary_keys, 
                    item['primary_key'], item['source_data'], column_types  # Use source data for target update
                )
                sql_statements.append(update_sql)
        
        sql_statements.append("")
        sql_statements.append("-- End of synchronization SQL")
        sql_statements.append("-- COMMIT;")
        
        return "\n".join(sql_statements)
    
    def _generate_insert_sql(self, schema: str, table_name: str, row_data: Dict[str, Any], 
                            column_types: Dict[str, Dict] = None) -> str:
        """
        Generate INSERT SQL statement for a single row.
        
        Args:
            schema (str): Schema name
            table_name (str): Table name
            row_data (Dict[str, Any]): Row data to insert
            column_types (Dict[str, Dict]): Column type information
            
        Returns:
            str: INSERT SQL statement
        """
        columns = list(row_data.keys())
        column_list = ", ".join([self._quote_identifier(col) for col in columns])
        
        # Generate values with proper formatting based on data types
        values = []
        for col in columns:
            value = row_data[col]
            
            # Get data type information
            data_type = 'unknown'
            if column_types and col in column_types:
                data_type = column_types[col].get('standardized_type', 'unknown')
            
            # Format value using data type handler
            formatted_value = self.data_type_handler.format_value_for_sql(value, data_type, 'oracle')
            values.append(formatted_value)
        
        values_list = ", ".join(values)
        
        sql = (f"INSERT INTO {self._quote_identifier(schema)}.{self._quote_identifier(table_name)} "
               f"({column_list}) VALUES ({values_list});")
        
        return sql
    
    def _generate_update_sql(self, schema: str, table_name: str, primary_keys: List[str],
                            pk_values: tuple, row_data: Dict[str, Any], 
                            column_types: Dict[str, Dict] = None) -> str:
        """
        Generate UPDATE SQL statement for a single row.
        
        Args:
            schema (str): Schema name
            table_name (str): Table name
            primary_keys (List[str]): Primary key columns
            pk_values (tuple): Primary key values
            row_data (Dict[str, Any]): Updated row data
            column_types (Dict[str, Dict]): Column type information
            
        Returns:
            str: UPDATE SQL statement
        """
        # Build SET clause (exclude primary key columns)
        set_clauses = []
        for col, value in row_data.items():
            if col.upper() not in [pk.upper() for pk in primary_keys]:
                # Get data type information
                data_type = 'unknown'
                if column_types and col in column_types:
                    data_type = column_types[col].get('standardized_type', 'unknown')
                
                # Format value using data type handler
                formatted_value = self.data_type_handler.format_value_for_sql(value, data_type, 'oracle')
                set_clauses.append(f"{self._quote_identifier(col)} = {formatted_value}")
        
        set_clause = ", ".join(set_clauses)
        
        # Build WHERE clause for primary keys
        where_clauses = []
        for i, pk_col in enumerate(primary_keys):
            pk_value = pk_values[i] if i < len(pk_values) else None
            
            # Get data type for primary key column
            pk_data_type = 'unknown'
            if column_types and pk_col in column_types:
                pk_data_type = column_types[pk_col].get('standardized_type', 'unknown')
            
            # Format primary key value
            formatted_pk_value = self.data_type_handler.format_value_for_sql(pk_value, pk_data_type, 'oracle')
            where_clauses.append(f"{self._quote_identifier(pk_col)} = {formatted_pk_value}")
        
        where_clause = " AND ".join(where_clauses)
        
        sql = (f"UPDATE {self._quote_identifier(schema)}.{self._quote_identifier(table_name)} "
               f"SET {set_clause} WHERE {where_clause};")
        
        return sql
    
    def _generate_delete_sql(self, schema: str, table_name: str, primary_keys: List[str],
                            pk_values: tuple, column_types: Dict[str, Dict] = None) -> str:
        """
        Generate DELETE SQL statement for a single row.
        
        Args:
            schema (str): Schema name
            table_name (str): Table name
            primary_keys (List[str]): Primary key columns
            pk_values (tuple): Primary key values
            column_types (Dict[str, Dict]): Column type information
            
        Returns:
            str: DELETE SQL statement
        """
        # Build WHERE clause for primary keys
        where_clauses = []
        for i, pk_col in enumerate(primary_keys):
            pk_value = pk_values[i] if i < len(pk_values) else None
            
            # Get data type for primary key column
            pk_data_type = 'unknown'
            if column_types and pk_col in column_types:
                pk_data_type = column_types[pk_col].get('standardized_type', 'unknown')
            
            # Format primary key value
            formatted_pk_value = self.data_type_handler.format_value_for_sql(pk_value, pk_data_type, 'oracle')
            where_clauses.append(f"{self._quote_identifier(pk_col)} = {formatted_pk_value}")
        
        where_clause = " AND ".join(where_clauses)
        
        sql = (f"DELETE FROM {self._quote_identifier(schema)}.{self._quote_identifier(table_name)} "
               f"WHERE {where_clause};")
        
        return sql
    
    def _format_sql_value(self, value: Any) -> str:
        """
        Format a Python value for SQL statement.
        
        This method handles different data types and properly escapes values
        for Oracle SQL statements.
        
        Args:
            value (Any): Value to format
            
        Returns:
            str: Formatted SQL value
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes and wrap in quotes
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif hasattr(value, 'strftime'):  # datetime objects
            # Format as Oracle timestamp
            return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            # Convert to string and escape
            string_value = str(value).replace("'", "''")
            return f"'{string_value}'"
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote Oracle identifier if necessary.
        
        Args:
            identifier (str): Identifier to quote
            
        Returns:
            str: Quoted identifier
        """
        # Oracle identifiers should be quoted if they contain special characters
        # or are reserved words
        if re.match(r'^[A-Z][A-Z0-9_]*$', identifier.upper()):
            return identifier.upper()
        else:
            return f'"{identifier}"'
    
    def _write_sql_file(self, table_name: str, database_type: str, sql_content: str):
        """
        Write SQL content to file.
        
        Args:
            table_name (str): Table name
            database_type (str): Either 'source' or 'target'
            sql_content (str): SQL content to write
        """
        filename = f"{database_type}_{table_name}_sync_{self.run_timestamp}.sql"
        filepath = self.sql_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            
            self.logger.info(f"SQL file written: {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error writing SQL file {filepath}: {str(e)}")
            raise
    
    def generate_summary_sql(self, all_results: Dict[str, Dict[str, Any]]):
        """
        Generate summary SQL files containing all table operations.
        
        Args:
            all_results (Dict[str, Dict[str, Any]]): All comparison results
        """
        try:
            source_sql_statements = []
            target_sql_statements = []
            
            # Add header
            header = [
                "-- DB Sentinel Utility - Complete Synchronization SQL",
                f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"-- Run ID: {self.run_timestamp}",
                "",
                "-- Execute all statements in a single transaction",
                "-- Review and test before running in production",
                ""
            ]
            
            source_sql_statements.extend(header)
            target_sql_statements.extend(header)
            
            # Process each table
            for table_name, result in all_results.items():
                if result.get('status') == 'SUCCESS' and result.get('differences'):
                    differences = result['differences']
                    total_ops = (len(differences['inserts']) + 
                               len(differences['updates']) + 
                               len(differences['deletes']))
                    
                    if total_ops > 0:
                        # Add table section headers
                        table_header = [
                            f"-- ============================================",
                            f"-- Table: {table_name}",
                            f"-- Inserts: {len(differences['inserts'])}, Updates: {len(differences['updates'])}, Deletes: {len(differences['deletes'])}",
                            f"-- ============================================",
                            ""
                        ]
                        
                        source_sql_statements.extend(table_header)
                        target_sql_statements.extend(table_header)
                        
                        # Generate SQL for this table
                        source_table_sql = self._generate_source_sql(
                            table_name, result['source_schema'], result['target_schema'],
                            result['primary_keys'], differences
                        )
                        
                        target_table_sql = self._generate_target_sql(
                            table_name, result['source_schema'], result['target_schema'],
                            result['primary_keys'], differences
                        )
                        
                        source_sql_statements.append(source_table_sql)
                        target_sql_statements.append(target_table_sql)
                        source_sql_statements.append("")
                        target_sql_statements.append("")
            
            # Write summary files
            source_summary_file = self.sql_dir / f"source_complete_sync_{self.run_timestamp}.sql"
            target_summary_file = self.sql_dir / f"target_complete_sync_{self.run_timestamp}.sql"
            
            with open(source_summary_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(source_sql_statements))
            
            with open(target_summary_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(target_sql_statements))
            
            self.logger.info(f"Summary SQL files generated: {source_summary_file}, {target_summary_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating summary SQL files: {str(e)}")
            raise
