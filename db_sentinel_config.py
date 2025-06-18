#!/usr/bin/env python3
"""
Configuration Manager for DB Sentinel Utility

This module handles loading and validating configuration from YAML files.
It ensures all required parameters are present and provides defaults where appropriate.

Author: Solutions Architect
Version: 1.0
"""

import os
import yaml
import logging
from typing import Dict, Any, List
from pathlib import Path


class ConfigManager:
    """
    Manages configuration loading and validation for DB Sentinel utility.
    
    This class handles loading YAML configuration files, validating required
    parameters, and providing sensible defaults for optional settings.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the configuration manager.
        
        Args:
            config_path (str): Path to the YAML configuration file
        """
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load and validate configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Validated configuration dictionary
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            yaml.YAMLError: If YAML parsing fails
            ValueError: If required configuration is missing
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            
            # Validate and apply defaults
            validated_config = self._validate_config(config)
            
            self.logger.info(f"Configuration loaded successfully from: {self.config_path}")
            return validated_config
            
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")
    
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate configuration and apply defaults.
        
        Args:
            config (Dict[str, Any]): Raw configuration dictionary
            
        Returns:
            Dict[str, Any]: Validated configuration with defaults applied
            
        Raises:
            ValueError: If required configuration is missing or invalid
        """
        # Define required sections and their required fields
        required_sections = {
            'source_db': ['host', 'port', 'service_name', 'username', 'password'],
            'target_db': ['host', 'port', 'service_name', 'username', 'password'],
            'tables': [],  # Will validate table structure separately
            'performance': [],
            'logging': [],
            'restart': [],
            'verification': [],
            'audit_table': []
        }
        
        # Check for required sections
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Required configuration section missing: {section}")
        
        # Validate database configurations
        self._validate_db_config(config['source_db'], 'source_db')
        self._validate_db_config(config['target_db'], 'target_db')
        
        # Validate tables configuration
        self._validate_tables_config(config['tables'])
        
        # Apply defaults for optional sections
        config = self._apply_defaults(config)
        
        return config
    
    def _validate_db_config(self, db_config: Dict[str, Any], section_name: str):
        """
        Validate database connection configuration.
        
        Args:
            db_config (Dict[str, Any]): Database configuration
            section_name (str): Name of the configuration section
            
        Raises:
            ValueError: If required database configuration is missing
        """
        required_fields = ['host', 'port', 'service_name', 'username', 'password']
        
        for field in required_fields:
            if field not in db_config or not db_config[field]:
                raise ValueError(f"Required field '{field}' missing in {section_name}")
        
        # Validate port is a number
        try:
            port = int(db_config['port'])
            if not (1 <= port <= 65535):
                raise ValueError(f"Invalid port number in {section_name}: {port}")
            db_config['port'] = port
        except (ValueError, TypeError):
            raise ValueError(f"Port must be a valid number in {section_name}")
    
    def _validate_tables_config(self, tables_config: List[Dict[str, Any]]):
        """
        Validate tables configuration.
        
        Args:
            tables_config (List[Dict[str, Any]]): List of table configurations
            
        Raises:
            ValueError: If table configuration is invalid
        """
        if not tables_config or not isinstance(tables_config, list):
            raise ValueError("Tables configuration must be a non-empty list")
        
        required_table_fields = ['source_schema', 'target_schema', 'table_name', 'primary_key']
        
        for i, table in enumerate(tables_config):
            if not isinstance(table, dict):
                raise ValueError(f"Table configuration {i} must be a dictionary")
            
            for field in required_table_fields:
                if field not in table or not table[field]:
                    raise ValueError(f"Required field '{field}' missing in table configuration {i}")
            
            # Validate primary_key is a list
            if not isinstance(table['primary_key'], list):
                raise ValueError(f"Primary key must be a list in table configuration {i}")
    
    def _apply_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply default values for optional configuration parameters.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            
        Returns:
            Dict[str, Any]: Configuration with defaults applied
        """
        # Performance defaults
        performance_defaults = {
            'batch_size': 10000,
            'max_workers': min(32, (os.cpu_count() or 1) + 4),
            'connection_pool_size': 5,
            'timeout_seconds': 300
        }
        
        for key, default_value in performance_defaults.items():
            if key not in config['performance']:
                config['performance'][key] = default_value
        
        # Logging defaults
        logging_defaults = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file_enabled': True,
            'console_enabled': True
        }
        
        for key, default_value in logging_defaults.items():
            if key not in config['logging']:
                config['logging'][key] = default_value
        
        # Restart defaults
        restart_defaults = {
            'enabled': False,
            'metadata_table': 'DB_SENTINEL_METADATA',
            'cleanup_on_success': True
        }
        
        for key, default_value in restart_defaults.items():
            if key not in config['restart']:
                config['restart'][key] = default_value
        
        # Verification defaults
        verification_defaults = {
            'enabled': True,
            'skip_existing_check': False,
            'generate_verified_sql': True
        }
        
        for key, default_value in verification_defaults.items():
            if key not in config['verification']:
                config['verification'][key] = default_value
        
        # Audit table defaults
        audit_defaults = {
            'enabled': False,
            'table_name': 'DB_SENTINEL_AUDIT',
            'schema': config['target_db'].get('username', 'SENTINEL'),
            'include_runtime_stats': True
        }
        
        for key, default_value in audit_defaults.items():
            if key not in config['audit_table']:
                config['audit_table'][key] = default_value
        
        # Apply table-level defaults
        for table in config['tables']:
            if 'batch_size' not in table:
                table['batch_size'] = config['performance']['batch_size']
            if 'where_clause' not in table:
                table['where_clause'] = ''
            if 'exclude_columns' not in table:
                table['exclude_columns'] = []
            if 'hash_algorithm' not in table:
                table['hash_algorithm'] = 'sha256'
        
        return config
    
    def validate_connection_string(self, db_config: Dict[str, Any]) -> str:
        """
        Generate Oracle connection string from database configuration.
        
        Args:
            db_config (Dict[str, Any]): Database configuration
            
        Returns:
            str: Oracle connection string
        """
        return (
            f"{db_config['username']}/{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
        )
    
    def get_table_config(self, config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific table.
        
        Args:
            config (Dict[str, Any]): Full configuration dictionary
            table_name (str): Name of the table to find
            
        Returns:
            Dict[str, Any]: Table configuration
            
        Raises:
            ValueError: If table configuration not found
        """
        for table in config['tables']:
            if table['table_name'] == table_name:
                return table
        
        raise ValueError(f"Table configuration not found for: {table_name}")
    
    def save_config(self, config: Dict[str, Any], output_path: str = None):
        """
        Save configuration to YAML file.
        
        Args:
            config (Dict[str, Any]): Configuration to save
            output_path (str, optional): Output file path. Defaults to original path.
        """
        output_path = output_path or self.config_path
        
        try:
            with open(output_path, 'w', encoding='utf-8') as file:
                yaml.dump(config, file, default_flow_style=False, indent=2)
            
            self.logger.info(f"Configuration saved to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
            raise
    
    def get_output_paths(self, run_timestamp: str) -> Dict[str, str]:
        """
        Generate output file paths for the current run.
        
        Args:
            run_timestamp (str): Timestamp for the current run
            
        Returns:
            Dict[str, str]: Dictionary of output paths
        """
        return {
            'sql_dir': 'DB_Sentinel_sql',
            'audit_dir': 'DB_Sentinel_audit',
            'report_dir': 'DB_Sentinel_report',
            'audit_log': f'DB_Sentinel_audit/audit_{run_timestamp}.log',
            'comparison_report': f'DB_Sentinel_report/comparison_report_{run_timestamp}.txt',
            'verified_report': f'DB_Sentinel_report/verified_comparison_report_{run_timestamp}.txt'
        }
