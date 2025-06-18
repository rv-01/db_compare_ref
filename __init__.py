#!/usr/bin/env python3
"""
DB Sentinel Utility - Production-grade Oracle Database Comparison Tool

This package provides comprehensive database comparison and synchronization
capabilities for Oracle databases with advanced features including:

- Row-level hashing for efficient comparison
- Multi-threading for parallel processing
- Restart/resume capability
- Post-verification of generated SQL statements
- Comprehensive audit logging and reporting
- Generic support for multiple schemas and tables

Author: Solutions Architect
Version: 1.0
Python: 3.8+
License: Enterprise
"""

__version__ = "1.0.0"
__author__ = "Solutions Architect"
__email__ = "architect@company.com"
__license__ = "Enterprise"
__description__ = "Production-grade Oracle Database Comparison and Synchronization Utility"

# Package metadata
__all__ = [
    'DBSentinelUtility',
    'ConfigManager', 
    'DatabaseManager',
    'TableComparator',
    'SQLGenerator',
    'AuditLogger',
    'ReportGenerator',
    'PostVerifier',
    'DataTypeHandler'
]

# Import main classes for easier access
try:
    from .DB_Sentinel_utility_refreshed import DBSentinelUtility
    from .db_sentinel_config import ConfigManager
    from .db_sentinel_db import DatabaseManager
    from .db_sentinel_comparison import TableComparator
    from .db_sentinel_sql_generator import SQLGenerator
    from .db_sentinel_audit import AuditLogger
    from .db_sentinel_reports import ReportGenerator
    from .db_sentinel_verification import PostVerifier
    from .db_sentinel_datatypes import DataTypeHandler
    
except ImportError as e:
    # Handle import errors gracefully for development
    import warnings
    warnings.warn(f"Some modules could not be imported: {e}")

# Version information
VERSION_INFO = {
    'major': 1,
    'minor': 0,
    'micro': 0,
    'release': 'stable'
}

# Package requirements
REQUIREMENTS = [
    'oracledb>=1.0.0',
    'pandas>=1.3.0',
    'numpy>=1.21.0',
    'PyYAML>=5.4.0',
    'tqdm>=4.60.0'
]

# Supported Python versions
PYTHON_REQUIRES = ">=3.8"

# Package configuration
DEFAULT_CONFIG = {
    'version': __version__,
    'debug': False,
    'log_level': 'INFO'
}

def get_version():
    """
    Return the package version string.
    
    Returns:
        str: Version string in format 'major.minor.micro'
    """
    return __version__

def get_version_info():
    """
    Return detailed version information.
    
    Returns:
        dict: Dictionary containing version components
    """
    return VERSION_INFO.copy()

def check_dependencies():
    """
    Check if all required dependencies are installed.
    
    Returns:
        dict: Dictionary with dependency status
    """
    import importlib
    
    dependency_status = {}
    
    dependencies = {
        'oracledb': 'oracledb',
        'pandas': 'pandas', 
        'numpy': 'numpy',
        'yaml': 'PyYAML',
        'tqdm': 'tqdm'
    }
    
    for module, package in dependencies.items():
        try:
            importlib.import_module(module)
            dependency_status[package] = {'installed': True, 'error': None}
        except ImportError as e:
            dependency_status[package] = {'installed': False, 'error': str(e)}
    
    return dependency_status

def print_banner():
    """Print the DB Sentinel banner."""
    banner = f"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                          DB SENTINEL UTILITY v{__version__}                          ║
║                 Production-grade Oracle Database Comparison Tool              ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  Features:                                                                    ║
║  • Row-level hashing for efficient data comparison                           ║
║  • Multi-threaded parallel processing                                        ║
║  • Restart/resume capability with metadata tracking                          ║
║  • Post-verification of generated SQL statements                             ║
║  • Comprehensive audit logging and reporting                                 ║
║  • Generic support for multiple schemas and tables                           ║
║  • Advanced configuration with YAML support                                  ║
║                                                                               ║
║  Author: {__author__:<60} ║
║  License: {__license__:<58} ║
╚═══════════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)

def create_sample_config(output_path='config_sample.yaml'):
    """
    Create a sample configuration file.
    
    Args:
        output_path (str): Path where to create the sample config
    """
    import yaml
    from pathlib import Path
    
    sample_config = {
        'source_db': {
            'host': 'source-server.company.com',
            'port': 1521,
            'service_name': 'SOURCEDB',
            'username': 'source_user',
            'password': 'source_password'
        },
        'target_db': {
            'host': 'target-server.company.com', 
            'port': 1521,
            'service_name': 'TARGETDB',
            'username': 'target_user',
            'password': 'target_password'
        },
        'tables': [
            {
                'source_schema': 'SCHEMA1',
                'target_schema': 'SCHEMA1_COPY',
                'table_name': 'SAMPLE_TABLE',
                'primary_key': ['ID'],
                'batch_size': 10000,
                'where_clause': '',
                'exclude_columns': [],
                'hash_algorithm': 'sha256'
            }
        ],
        'performance': {
            'batch_size': 10000,
            'max_workers': 4,
            'connection_pool_size': 5,
            'timeout_seconds': 300
        },
        'logging': {
            'level': 'INFO',
            'file_enabled': True,
            'console_enabled': True
        },
        'restart': {
            'enabled': True,
            'metadata_table': 'DB_SENTINEL_METADATA',
            'cleanup_on_success': True
        },
        'verification': {
            'enabled': True,
            'generate_verified_sql': True
        },
        'audit_table': {
            'enabled': False,
            'table_name': 'DB_SENTINEL_AUDIT',
            'schema': 'SENTINEL_ADMIN'
        }
    }
    
    try:
        with open(output_path, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
        print(f"Sample configuration created: {output_path}")
    except Exception as e:
        print(f"Error creating sample config: {e}")

# Initialize logging for the package
import logging

# Create package logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Only add handler if none exists to avoid duplicate logs
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Package initialization message
logger.info(f"DB Sentinel Utility v{__version__} initialized")

# Export utility functions
__all__.extend([
    'get_version',
    'get_version_info', 
    'check_dependencies',
    'print_banner',
    'create_sample_config'
])

# Compatibility check
import sys
if sys.version_info < (3, 8):
    raise RuntimeError(f"DB Sentinel requires Python 3.8 or higher. Current version: {sys.version}")

# Module level constants
MODULE_PATH = Path(__file__).parent.absolute()
DEFAULT_CONFIG_PATH = MODULE_PATH / 'config.yaml'
DEFAULT_OUTPUT_DIR = Path.cwd()

# Export constants
__all__.extend([
    'MODULE_PATH',
    'DEFAULT_CONFIG_PATH', 
    'DEFAULT_OUTPUT_DIR',
    'VERSION_INFO',
    'REQUIREMENTS',
    'PYTHON_REQUIRES'
])
