#!/usr/bin/env python3
"""
Audit Logger for DB Sentinel Utility

This module handles audit logging for the DB Sentinel utility, tracking
all events, errors, and operations for compliance and troubleshooting.

Author: Solutions Architect
Version: 1.0
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import threading


class AuditLogger:
    """
    Manages audit logging for DB Sentinel operations.
    
    This class provides comprehensive audit logging capabilities including
    event tracking, error logging, performance metrics, and compliance reporting.
    """
    
    def __init__(self, config: Dict[str, Any], run_timestamp: str):
        """
        Initialize the audit logger.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            run_timestamp (str): Timestamp for the current run
        """
        self.config = config
        self.run_timestamp = run_timestamp
        self.logger = logging.getLogger(__name__)
        
        # Create audit directory
        self.audit_dir = Path("DB_Sentinel_audit")
        self.audit_dir.mkdir(exist_ok=True)
        
        # Audit log file
        self.audit_log_file = self.audit_dir / f"audit_{run_timestamp}.log"
        
        # Thread lock for audit logging
        self.audit_lock = threading.Lock()
        
        # Initialize audit session
        self._initialize_audit_session()
    
    def _initialize_audit_session(self):
        """Initialize the audit session with session metadata."""
        try:
            session_info = {
                'session_id': self.run_timestamp,
                'start_time': datetime.now().isoformat(),
                'version': '1.0',
                'config_summary': {
                    'source_host': self.config['source_db']['host'],
                    'target_host': self.config['target_db']['host'],
                    'table_count': len(self.config['tables']),
                    'batch_size': self.config['performance']['batch_size'],
                    'max_workers': self.config['performance']['max_workers']
                }
            }
            
            self.log_event("SESSION_START", "DB Sentinel audit session initialized", session_info)
            
        except Exception as e:
            self.logger.error(f"Error initializing audit session: {str(e)}")
    
    def log_event(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Log an audit event.
        
        Args:
            event_type (str): Type of event (e.g., 'TABLE_START', 'ERROR', 'WARNING')
            message (str): Event message
            details (Optional[Dict[str, Any]]): Additional event details
        """
        try:
            with self.audit_lock:
                audit_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'session_id': self.run_timestamp,
                    'event_type': event_type,
                    'message': message,
                    'thread_id': threading.current_thread().ident,
                    'details': details or {}
                }
                
                # Write to audit log file
                with open(self.audit_log_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(audit_entry) + '\n')
                
                # Also log to standard logger for immediate visibility
                if event_type in ['ERROR', 'CRITICAL']:
                    self.logger.error(f"AUDIT: {message}")
                elif event_type == 'WARNING':
                    self.logger.warning(f"AUDIT: {message}")
                else:
                    self.logger.info(f"AUDIT: {message}")
                    
        except Exception as e:
            # Fallback logging if audit logging fails
            self.logger.error(f"Failed to write audit log: {str(e)}")
    
    def log_table_start(self, table_name: str, source_schema: str, target_schema: str,
                       primary_keys: List[str]):
        """
        Log the start of table comparison.
        
        Args:
            table_name (str): Table name
            source_schema (str): Source schema
            target_schema (str): Target schema
            primary_keys (List[str]): Primary key columns
        """
        details = {
            'table_name': table_name,
            'source_schema': source_schema,
            'target_schema': target_schema,
            'primary_keys': primary_keys
        }
        
        self.log_event("TABLE_START", f"Started comparison for table {table_name}", details)
    
    def log_table_complete(self, table_name: str, comparison_result: Dict[str, Any],
                          processing_time: float):
        """
        Log the completion of table comparison.
        
        Args:
            table_name (str): Table name
            comparison_result (Dict[str, Any]): Comparison results
            processing_time (float): Processing time in seconds
        """
        if comparison_result.get('status') == 'SUCCESS':
            stats = comparison_result.get('statistics', {})
            details = {
                'table_name': table_name,
                'processing_time_seconds': processing_time,
                'source_row_count': stats.get('source_row_count', 0),
                'target_row_count': stats.get('target_row_count', 0),
                'insert_count': stats.get('insert_count', 0),
                'update_count': stats.get('update_count', 0),
                'delete_count': stats.get('delete_count', 0),
                'total_differences': stats.get('total_differences', 0),
                'match_percentage': stats.get('match_percentage', 0.0)
            }
            
            self.log_event("TABLE_COMPLETE", f"Completed comparison for table {table_name}", details)
        else:
            details = {
                'table_name': table_name,
                'processing_time_seconds': processing_time,
                'error': comparison_result.get('error', 'Unknown error')
            }
            
            self.log_event("TABLE_ERROR", f"Error comparing table {table_name}", details)
    
    def log_sql_generation(self, table_name: str, file_paths: List[str], 
                          statement_counts: Dict[str, int]):
        """
        Log SQL file generation.
        
        Args:
            table_name (str): Table name
            file_paths (List[str]): Generated SQL file paths
            statement_counts (Dict[str, int]): Count of each statement type
        """
        details = {
            'table_name': table_name,
            'generated_files': file_paths,
            'statement_counts': statement_counts
        }
        
        self.log_event("SQL_GENERATED", f"Generated SQL files for table {table_name}", details)
    
    def log_verification_start(self, table_name: str):
        """
        Log the start of post-verification process.
        
        Args:
            table_name (str): Table name
        """
        details = {'table_name': table_name}
        self.log_event("VERIFICATION_START", f"Started post-verification for table {table_name}", details)
    
    def log_verification_complete(self, table_name: str, verification_result: Dict[str, Any]):
        """
        Log the completion of post-verification process.
        
        Args:
            table_name (str): Table name
            verification_result (Dict[str, Any]): Verification results
        """
        details = {
            'table_name': table_name,
            'verification_result': verification_result
        }
        
        self.log_event("VERIFICATION_COMPLETE", f"Completed post-verification for table {table_name}", details)
    
    def log_connection_event(self, database: str, event_type: str, details: Optional[Dict] = None):
        """
        Log database connection events.
        
        Args:
            database (str): Database identifier ('source' or 'target')
            event_type (str): Event type ('CONNECT', 'DISCONNECT', 'ERROR')
            details (Optional[Dict]): Additional details
        """
        event_details = {'database': database}
        if details:
            event_details.update(details)
        
        self.log_event(f"DB_{event_type}", f"Database {event_type.lower()} - {database}", event_details)
    
    def log_performance_metric(self, metric_name: str, value: float, unit: str = "seconds",
                              context: Optional[Dict] = None):
        """
        Log performance metrics.
        
        Args:
            metric_name (str): Name of the metric
            value (float): Metric value
            unit (str): Unit of measurement
            context (Optional[Dict]): Additional context
        """
        details = {
            'metric_name': metric_name,
            'value': value,
            'unit': unit,
            'context': context or {}
        }
        
        self.log_event("PERFORMANCE_METRIC", f"Performance metric: {metric_name} = {value} {unit}", details)
    
    def log_error(self, error_message: str, exception: Optional[Exception] = None,
                 context: Optional[Dict] = None):
        """
        Log error events with full context.
        
        Args:
            error_message (str): Error message
            exception (Optional[Exception]): Exception object if available
            context (Optional[Dict]): Additional context information
        """
        details = {'error_message': error_message}
        
        if exception:
            details['exception_type'] = type(exception).__name__
            details['exception_details'] = str(exception)
        
        if context:
            details['context'] = context
        
        self.log_event("ERROR", error_message, details)
    
    def log_warning(self, warning_message: str, context: Optional[Dict] = None):
        """
        Log warning events.
        
        Args:
            warning_message (str): Warning message
            context (Optional[Dict]): Additional context information
        """
        details = {'context': context or {}}
        self.log_event("WARNING", warning_message, details)
    
    def log_batch_progress(self, table_name: str, batch_number: int, total_batches: int,
                          rows_processed: int, processing_time: float):
        """
        Log batch processing progress.
        
        Args:
            table_name (str): Table name
            batch_number (int): Current batch number
            total_batches (int): Total number of batches
            rows_processed (int): Number of rows processed in this batch
            processing_time (float): Time taken to process this batch
        """
        details = {
            'table_name': table_name,
            'batch_number': batch_number,
            'total_batches': total_batches,
            'rows_processed': rows_processed,
            'processing_time_seconds': processing_time,
            'progress_percentage': (batch_number / total_batches) * 100
        }
        
        self.log_event("BATCH_PROGRESS", 
                      f"Processed batch {batch_number}/{total_batches} for table {table_name}", 
                      details)
    
    def generate_audit_summary(self) -> Dict[str, Any]:
        """
        Generate audit summary from audit log.
        
        Returns:
            Dict[str, Any]: Audit summary with statistics and metrics
        """
        try:
            summary = {
                'session_id': self.run_timestamp,
                'total_events': 0,
                'event_types': {},
                'tables_processed': set(),
                'errors': [],
                'warnings': [],
                'performance_metrics': {},
                'session_duration': None
            }
            
            session_start_time = None
            session_end_time = None
            
            # Read and analyze audit log
            if self.audit_log_file.exists():
                with open(self.audit_log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            summary['total_events'] += 1
                            
                            event_type = entry.get('event_type', 'UNKNOWN')
                            summary['event_types'][event_type] = summary['event_types'].get(event_type, 0) + 1
                            
                            # Track session timing
                            if event_type == 'SESSION_START':
                                session_start_time = datetime.fromisoformat(entry['timestamp'])
                            
                            # Track tables
                            details = entry.get('details', {})
                            if 'table_name' in details:
                                summary['tables_processed'].add(details['table_name'])
                            
                            # Collect errors and warnings
                            if event_type == 'ERROR':
                                summary['errors'].append({
                                    'timestamp': entry['timestamp'],
                                    'message': entry['message'],
                                    'details': details
                                })
                            elif event_type == 'WARNING':
                                summary['warnings'].append({
                                    'timestamp': entry['timestamp'],
                                    'message': entry['message'],
                                    'details': details
                                })
                            
                            # Collect performance metrics
                            if event_type == 'PERFORMANCE_METRIC':
                                metric_name = details.get('metric_name')
                                if metric_name:
                                    summary['performance_metrics'][metric_name] = {
                                        'value': details.get('value'),
                                        'unit': details.get('unit', 'unknown')
                                    }
                        
                        except json.JSONDecodeError:
                            continue  # Skip malformed lines
            
            # Calculate session duration
            session_end_time = datetime.now()
            if session_start_time:
                duration = session_end_time - session_start_time
                summary['session_duration'] = duration.total_seconds()
            
            # Convert set to list for JSON serialization
            summary['tables_processed'] = list(summary['tables_processed'])
            
            # Log the summary
            self.log_event("AUDIT_SUMMARY", "Generated audit summary", summary)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating audit summary: {str(e)}")
            return {'error': str(e)}
    
    def finalize_audit_session(self):
        """Finalize the audit session and generate summary."""
        try:
            # Generate audit summary
            summary = self.generate_audit_summary()
            
            # Log session end
            self.log_event("SESSION_END", "DB Sentinel audit session completed", {
                'end_time': datetime.now().isoformat(),
                'summary': summary
            })
            
            # Write summary to separate file
            summary_file = self.audit_dir / f"audit_summary_{self.run_timestamp}.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info(f"Audit session finalized. Summary written to: {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Error finalizing audit session: {str(e)}")
    
    def get_audit_file_path(self) -> str:
        """
        Get the path to the current audit log file.
        
        Returns:
            str: Path to audit log file
        """
        return str(self.audit_log_file)
    
    def get_events_by_type(self, event_type: str) -> List[Dict[str, Any]]:
        """
        Get all events of a specific type from the audit log.
        
        Args:
            event_type (str): Event type to filter by
            
        Returns:
            List[Dict[str, Any]]: List of matching events
        """
        events = []
        
        try:
            if self.audit_log_file.exists():
                with open(self.audit_log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            if entry.get('event_type') == event_type:
                                events.append(entry)
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            self.logger.error(f"Error reading audit events: {str(e)}")
        
        return events
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - finalize audit session."""
        if exc_type:
            self.log_error(f"Exception during execution: {exc_val}", exc_val)
        
        self.finalize_audit_session()
