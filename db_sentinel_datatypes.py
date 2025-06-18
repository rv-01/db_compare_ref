#!/usr/bin/env python3
"""
Data Type Handler for DB Sentinel Utility

This module provides comprehensive data type handling for Oracle and other database systems,
supporting proper conversion, comparison, and SQL generation for all common data types.

Author: Solutions Architect
Version: 1.0
"""

import logging
from typing import Any, Dict, Optional, Union, List
from datetime import datetime, date, time
from decimal import Decimal
import json
import base64
import re


class DataTypeHandler:
    """
    Handles data type conversions, comparisons, and SQL formatting for database operations.
    
    This class provides comprehensive support for all common database data types including
    numeric, character, date/time, binary, and specialized types like JSON and BOOLEAN.
    """
    
    def __init__(self):
        """Initialize the data type handler."""
        self.logger = logging.getLogger(__name__)
        
        # Define data type mappings and patterns
        self._init_type_mappings()
        self._init_format_patterns()
    
    def _init_type_mappings(self):
        """Initialize data type mappings for different database systems."""
        # Oracle data type mappings
        self.oracle_types = {
            # Numeric types
            'NUMBER': 'numeric',
            'INTEGER': 'integer', 
            'INT': 'integer',
            'SMALLINT': 'integer',
            'BIGINT': 'integer',
            'FLOAT': 'float',
            'DOUBLE': 'float',
            'DOUBLE PRECISION': 'float',
            'DECIMAL': 'decimal',
            'NUMERIC': 'numeric',
            'BINARY_FLOAT': 'float',
            'BINARY_DOUBLE': 'float',
            
            # Character types
            'VARCHAR2': 'varchar',
            'VARCHAR': 'varchar',
            'CHAR': 'char',
            'NCHAR': 'nchar',
            'NVARCHAR2': 'nvarchar',
            'NVARCHAR': 'nvarchar', 
            'CLOB': 'clob',
            'NCLOB': 'nclob',
            'LONG': 'long_text',
            'TEXT': 'text',
            
            # Date/Time types
            'DATE': 'date',
            'TIMESTAMP': 'timestamp',
            'TIMESTAMP WITH TIME ZONE': 'timestamp_tz',
            'TIMESTAMP WITH LOCAL TIME ZONE': 'timestamp_ltz',
            'TIME': 'time',
            'DATETIME': 'datetime',
            'INTERVAL YEAR TO MONTH': 'interval_ym',
            'INTERVAL DAY TO SECOND': 'interval_ds',
            
            # Binary types
            'BLOB': 'blob',
            'RAW': 'binary',
            'LONG RAW': 'long_binary',
            'BINARY': 'binary',
            'VARBINARY': 'varbinary',
            'BFILE': 'bfile',
            
            # Other types
            'BOOLEAN': 'boolean',
            'JSON': 'json',
            'XMLTYPE': 'xml',
            'ROWID': 'rowid',
            'UROWID': 'urowid'
        }
        
        # Generic type categories for cross-database compatibility
        self.type_categories = {
            'numeric': ['numeric', 'integer', 'float', 'decimal'],
            'character': ['varchar', 'char', 'nchar', 'nvarchar', 'text', 'clob', 'nclob', 'long_text'],
            'datetime': ['date', 'timestamp', 'timestamp_tz', 'timestamp_ltz', 'time', 'datetime'],
            'binary': ['blob', 'binary', 'varbinary', 'long_binary', 'bfile'],
            'interval': ['interval_ym', 'interval_ds'],
            'special': ['boolean', 'json', 'xml', 'rowid', 'urowid']
        }
    
    def _init_format_patterns(self):
        """Initialize formatting patterns for different data types."""
        self.format_patterns = {
            'date_formats': [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%d-%b-%Y',
                '%d-%b-%Y %H:%M:%S',
                '%m/%d/%Y',
                '%m/%d/%Y %H:%M:%S'
            ],
            'number_precision': 15,  # Default precision for floating point comparisons
            'string_encoding': 'utf-8',
            'binary_encoding': 'base64'
        }
    
    def identify_data_type(self, column_info: Dict[str, Any]) -> str:
        """
        Identify the standardized data type from column metadata.
        
        Args:
            column_info (Dict[str, Any]): Column metadata from database
            
        Returns:
            str: Standardized data type name
        """
        try:
            data_type = str(column_info.get('DATA_TYPE', '')).upper()
            
            # Handle parameterized types (e.g., VARCHAR2(100), NUMBER(10,2))
            base_type = data_type.split('(')[0].strip()
            
            # Map to standardized type
            standardized_type = self.oracle_types.get(base_type, 'unknown')
            
            self.logger.debug(f"Mapped data type '{data_type}' to '{standardized_type}'")
            return standardized_type
            
        except Exception as e:
            self.logger.warning(f"Error identifying data type: {str(e)}")
            return 'unknown'
    
    def get_type_category(self, data_type: str) -> str:
        """
        Get the category for a data type.
        
        Args:
            data_type (str): Standardized data type
            
        Returns:
            str: Type category (numeric, character, datetime, binary, special)
        """
        for category, types in self.type_categories.items():
            if data_type in types:
                return category
        return 'unknown'
    
    def normalize_value_for_hashing(self, value: Any, data_type: str) -> str:
        """
        Normalize a value for consistent hashing across data types.
        
        This method ensures that equivalent values produce the same hash
        regardless of their representation (e.g., 1.0 vs 1, '2023-01-01' vs datetime).
        
        Args:
            value (Any): Value to normalize
            data_type (str): Standardized data type
            
        Returns:
            str: Normalized string representation for hashing
        """
        try:
            if value is None:
                return "NULL"
            
            category = self.get_type_category(data_type)
            
            if category == 'numeric':
                return self._normalize_numeric_value(value, data_type)
            elif category == 'character':
                return self._normalize_character_value(value, data_type)
            elif category == 'datetime':
                return self._normalize_datetime_value(value, data_type)
            elif category == 'binary':
                return self._normalize_binary_value(value, data_type)
            elif category == 'special':
                return self._normalize_special_value(value, data_type)
            else:
                # Fallback to string representation
                return str(value)
                
        except Exception as e:
            self.logger.warning(f"Error normalizing value for hashing: {str(e)}")
            return str(value) if value is not None else "NULL"
    
    def _normalize_numeric_value(self, value: Any, data_type: str) -> str:
        """Normalize numeric values for consistent hashing."""
        try:
            if data_type in ['integer']:
                # Convert to integer and back to string to normalize representation
                return str(int(float(value)))
            elif data_type in ['float']:
                # Round to avoid floating point precision issues
                return f"{float(value):.{self.format_patterns['number_precision']}g}"
            elif data_type in ['decimal', 'numeric']:
                # Use Decimal for precise numeric representation
                if isinstance(value, Decimal):
                    return str(value.normalize())
                else:
                    return str(Decimal(str(value)).normalize())
            else:
                # Generic numeric handling
                return str(float(value))
                
        except (ValueError, TypeError, OverflowError):
            return str(value)
    
    def _normalize_character_value(self, value: Any, data_type: str) -> str:
        """Normalize character values for consistent hashing."""
        try:
            if isinstance(value, bytes):
                # Decode bytes to string
                return value.decode(self.format_patterns['string_encoding'], errors='replace')
            elif data_type in ['clob', 'nclob', 'long_text']:
                # Handle large text objects
                text_value = str(value)
                # Normalize line endings and whitespace for consistent hashing
                return re.sub(r'\r\n|\r|\n', '\n', text_value)
            else:
                # Standard string handling
                return str(value)
                
        except Exception:
            return str(value)
    
    def _normalize_datetime_value(self, value: Any, data_type: str) -> str:
        """Normalize date/time values for consistent hashing."""
        try:
            if isinstance(value, datetime):
                if data_type == 'date':
                    return value.strftime('%Y-%m-%d')
                elif data_type == 'time':
                    return value.strftime('%H:%M:%S.%f')
                else:
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(value, date):
                return value.strftime('%Y-%m-%d')
            elif isinstance(value, time):
                return value.strftime('%H:%M:%S.%f')
            elif isinstance(value, str):
                # Try to parse string dates into a normalized format
                return self._parse_and_normalize_date_string(value, data_type)
            else:
                return str(value)
                
        except Exception:
            return str(value)
    
    def _parse_and_normalize_date_string(self, date_string: str, data_type: str) -> str:
        """Parse and normalize date strings."""
        for fmt in self.format_patterns['date_formats']:
            try:
                dt = datetime.strptime(date_string.strip(), fmt)
                if data_type == 'date':
                    return dt.strftime('%Y-%m-%d')
                elif data_type == 'time':
                    return dt.strftime('%H:%M:%S.%f')
                else:
                    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                continue
        
        # If no format matches, return as-is
        return date_string
    
    def _normalize_binary_value(self, value: Any, data_type: str) -> str:
        """Normalize binary values for consistent hashing."""
        try:
            if isinstance(value, bytes):
                # Encode binary data as base64 for consistent string representation
                return base64.b64encode(value).decode('ascii')
            elif isinstance(value, str):
                # Assume it's already encoded or hex string
                return value
            else:
                # Convert to bytes first
                return base64.b64encode(str(value).encode()).decode('ascii')
                
        except Exception:
            return str(value)
    
    def _normalize_special_value(self, value: Any, data_type: str) -> str:
        """Normalize special data types (JSON, Boolean, etc.)."""
        try:
            if data_type == 'boolean':
                # Normalize boolean values
                if isinstance(value, bool):
                    return str(value).lower()
                elif isinstance(value, (int, float)):
                    return 'true' if value != 0 else 'false'
                elif isinstance(value, str):
                    return 'true' if value.lower() in ('true', '1', 'yes', 'y') else 'false'
                else:
                    return 'false'
            elif data_type == 'json':
                # Normalize JSON by parsing and re-serializing
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        return json.dumps(parsed, sort_keys=True, separators=(',', ':'))
                    except json.JSONDecodeError:
                        return value
                else:
                    return json.dumps(value, sort_keys=True, separators=(',', ':'))
            else:
                return str(value)
                
        except Exception:
            return str(value)
    
    def format_value_for_sql(self, value: Any, data_type: str, target_db: str = 'oracle') -> str:
        """
        Format a value for SQL statement generation.
        
        Args:
            value (Any): Value to format
            data_type (str): Standardized data type
            target_db (str): Target database type (oracle, postgresql, etc.)
            
        Returns:
            str: SQL-formatted value
        """
        try:
            if value is None:
                return "NULL"
            
            category = self.get_type_category(data_type)
            
            if category == 'numeric':
                return self._format_numeric_for_sql(value, data_type, target_db)
            elif category == 'character':
                return self._format_character_for_sql(value, data_type, target_db)
            elif category == 'datetime':
                return self._format_datetime_for_sql(value, data_type, target_db)
            elif category == 'binary':
                return self._format_binary_for_sql(value, data_type, target_db)
            elif category == 'special':
                return self._format_special_for_sql(value, data_type, target_db)
            else:
                # Fallback to quoted string
                return f"'{self._escape_sql_string(str(value))}'"
                
        except Exception as e:
            self.logger.warning(f"Error formatting value for SQL: {str(e)}")
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _format_numeric_for_sql(self, value: Any, data_type: str, target_db: str) -> str:
        """Format numeric values for SQL."""
        try:
            if data_type in ['integer']:
                return str(int(float(value)))
            elif data_type in ['float']:
                return str(float(value))
            elif data_type in ['decimal', 'numeric']:
                if isinstance(value, Decimal):
                    return str(value)
                else:
                    return str(Decimal(str(value)))
            else:
                return str(float(value))
        except (ValueError, TypeError, OverflowError):
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _format_character_for_sql(self, value: Any, data_type: str, target_db: str) -> str:
        """Format character values for SQL."""
        try:
            if isinstance(value, bytes):
                value = value.decode(self.format_patterns['string_encoding'], errors='replace')
            
            string_value = str(value)
            escaped_value = self._escape_sql_string(string_value)
            
            if data_type in ['nchar', 'nvarchar', 'nclob'] and target_db == 'oracle':
                return f"N'{escaped_value}'"
            else:
                return f"'{escaped_value}'"
                
        except Exception:
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _format_datetime_for_sql(self, value: Any, data_type: str, target_db: str) -> str:
        """Format date/time values for SQL."""
        try:
            if isinstance(value, datetime):
                if target_db == 'oracle':
                    if data_type == 'date':
                        return f"DATE '{value.strftime('%Y-%m-%d')}'"
                    elif data_type in ['timestamp', 'timestamp_tz', 'timestamp_ltz']:
                        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
                    else:
                        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
                else:
                    return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            
            elif isinstance(value, date):
                if target_db == 'oracle':
                    return f"DATE '{value.strftime('%Y-%m-%d')}'"
                else:
                    return f"'{value.strftime('%Y-%m-%d')}'"
            
            elif isinstance(value, time):
                return f"'{value.strftime('%H:%M:%S.%f')}'"
            
            elif isinstance(value, str):
                # Try to parse and reformat
                normalized = self._parse_and_normalize_date_string(value, data_type)
                if target_db == 'oracle':
                    if data_type == 'date':
                        return f"DATE '{normalized}'"
                    else:
                        return f"TIMESTAMP '{normalized}'"
                else:
                    return f"'{normalized}'"
            else:
                return f"'{self._escape_sql_string(str(value))}'"
                
        except Exception:
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _format_binary_for_sql(self, value: Any, data_type: str, target_db: str) -> str:
        """Format binary values for SQL."""
        try:
            if isinstance(value, bytes):
                if target_db == 'oracle':
                    # Convert to hex string for Oracle
                    hex_value = value.hex().upper()
                    return f"HEXTORAW('{hex_value}')"
                else:
                    # Use base64 for other databases
                    b64_value = base64.b64encode(value).decode('ascii')
                    return f"'{b64_value}'"
            elif isinstance(value, str):
                # Assume it's already encoded
                if target_db == 'oracle' and data_type in ['blob', 'raw']:
                    return f"HEXTORAW('{value}')"
                else:
                    return f"'{self._escape_sql_string(value)}'"
            else:
                return f"'{self._escape_sql_string(str(value))}'"
                
        except Exception:
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _format_special_for_sql(self, value: Any, data_type: str, target_db: str) -> str:
        """Format special data types for SQL."""
        try:
            if data_type == 'boolean':
                if target_db == 'oracle':
                    # Oracle doesn't have native boolean, use 1/0
                    bool_val = self._normalize_special_value(value, 'boolean')
                    return '1' if bool_val == 'true' else '0'
                else:
                    bool_val = self._normalize_special_value(value, 'boolean')
                    return bool_val.upper()
            
            elif data_type == 'json':
                json_str = self._normalize_special_value(value, 'json')
                escaped_json = self._escape_sql_string(json_str)
                return f"'{escaped_json}'"
            
            else:
                return f"'{self._escape_sql_string(str(value))}'"
                
        except Exception:
            return f"'{self._escape_sql_string(str(value))}'"
    
    def _escape_sql_string(self, value: str) -> str:
        """Escape special characters in SQL strings."""
        if value is None:
            return ""
        
        # Replace single quotes with double single quotes (SQL standard)
        escaped = str(value).replace("'", "''")
        
        # Handle other special characters if needed
        # escaped = escaped.replace("\\", "\\\\")  # Uncomment if needed for specific databases
        
        return escaped
    
    def compare_values(self, value1: Any, value2: Any, data_type: str) -> bool:
        """
        Compare two values considering their data type.
        
        Args:
            value1 (Any): First value
            value2 (Any): Second value  
            data_type (str): Standardized data type
            
        Returns:
            bool: True if values are considered equal for this data type
        """
        try:
            # Handle None values
            if value1 is None and value2 is None:
                return True
            if value1 is None or value2 is None:
                return False
            
            # Normalize both values and compare
            norm1 = self.normalize_value_for_hashing(value1, data_type)
            norm2 = self.normalize_value_for_hashing(value2, data_type)
            
            return norm1 == norm2
            
        except Exception as e:
            self.logger.warning(f"Error comparing values: {str(e)}")
            # Fallback to string comparison
            return str(value1) == str(value2)
    
    def get_column_type_info(self, connection, schema: str, table: str) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed column type information for a table.
        
        Args:
            connection: Database connection
            schema (str): Schema name
            table (str): Table name
            
        Returns:
            Dict[str, Dict[str, Any]]: Column name to type info mapping
        """
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    data_default,
                    char_length,
                    char_used
                FROM all_tab_columns 
                WHERE owner = :schema 
                  AND table_name = :table
                ORDER BY column_id
            """
            
            cursor = connection.cursor()
            cursor.execute(query, {'schema': schema.upper(), 'table': table.upper()})
            
            columns_info = {}
            for row in cursor.fetchall():
                col_name = row[0]
                type_info = {
                    'DATA_TYPE': row[1],
                    'DATA_LENGTH': row[2],
                    'DATA_PRECISION': row[3],
                    'DATA_SCALE': row[4],
                    'NULLABLE': row[5],
                    'DATA_DEFAULT': row[6],
                    'CHAR_LENGTH': row[7],
                    'CHAR_USED': row[8],
                    'standardized_type': self.identify_data_type({'DATA_TYPE': row[1]}),
                    'category': self.get_type_category(self.identify_data_type({'DATA_TYPE': row[1]}))
                }
                columns_info[col_name] = type_info
            
            cursor.close()
            return columns_info
            
        except Exception as e:
            self.logger.error(f"Error getting column type info: {str(e)}")
            return {}
    
    def validate_data_compatibility(self, source_types: Dict[str, Dict], 
                                  target_types: Dict[str, Dict]) -> Dict[str, List[str]]:
        """
        Validate data type compatibility between source and target tables.
        
        Args:
            source_types (Dict[str, Dict]): Source table column types
            target_types (Dict[str, Dict]): Target table column types
            
        Returns:
            Dict[str, List[str]]: Validation results with warnings/errors
        """
        validation_results = {
            'compatible': [],
            'warnings': [],
            'errors': []
        }
        
        try:
            # Check columns present in both tables
            common_columns = set(source_types.keys()) & set(target_types.keys())
            
            for col_name in common_columns:
                source_type = source_types[col_name]['standardized_type']
                target_type = target_types[col_name]['standardized_type']
                
                if source_type == target_type:
                    validation_results['compatible'].append(col_name)
                elif self.get_type_category(source_type) == self.get_type_category(target_type):
                    validation_results['warnings'].append(
                        f"Column {col_name}: Compatible categories but different types "
                        f"({source_type} -> {target_type})"
                    )
                else:
                    validation_results['errors'].append(
                        f"Column {col_name}: Incompatible types "
                        f"({source_type} -> {target_type})"
                    )
            
            # Check for missing columns
            source_only = set(source_types.keys()) - set(target_types.keys())
            target_only = set(target_types.keys()) - set(source_types.keys())
            
            for col in source_only:
                validation_results['warnings'].append(f"Column {col}: Present in source but not target")
            
            for col in target_only:
                validation_results['warnings'].append(f"Column {col}: Present in target but not source")
            
        except Exception as e:
            validation_results['errors'].append(f"Validation error: {str(e)}")
        
        return validation_results


# Global instance for easy access
data_type_handler = DataTypeHandler()

# Convenience functions for external use
def normalize_for_hash(value: Any, data_type: str) -> str:
    """Convenience function for value normalization."""
    return data_type_handler.normalize_value_for_hashing(value, data_type)

def format_for_sql(value: Any, data_type: str, target_db: str = 'oracle') -> str:
    """Convenience function for SQL formatting."""
    return data_type_handler.format_value_for_sql(value, data_type, target_db)

def compare_typed_values(value1: Any, value2: Any, data_type: str) -> bool:
    """Convenience function for typed value comparison."""
    return data_type_handler.compare_values(value1, value2, data_type)
