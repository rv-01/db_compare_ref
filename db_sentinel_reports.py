#!/usr/bin/env python3
"""
Report Generator for DB Sentinel Utility

This module generates comprehensive comparison reports including statistics,
summaries, and detailed difference analysis for database synchronization.

Author: Solutions Architect
Version: 1.0
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import json
import csv


class ReportGenerator:
    """
    Generates comprehensive reports for DB Sentinel comparison results.
    
    This class creates detailed reports including comparison summaries,
    statistics, and verification results for audit and analysis purposes.
    """
    
    def __init__(self, config: Dict[str, Any], run_timestamp: str):
        """
        Initialize the report generator.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
            run_timestamp (str): Timestamp for the current run
        """
        self.config = config
        self.run_timestamp = run_timestamp
        self.logger = logging.getLogger(__name__)
        
        # Create report directory
        self.report_dir = Path("DB_Sentinel_report")
        self.report_dir.mkdir(exist_ok=True)
    
    def generate_reports(self, comparison_results: Dict[str, Any]):
        """
        Generate all comparison reports.
        
        Args:
            comparison_results (Dict[str, Any]): Results from table comparisons
        """
        try:
            self.logger.info("Generating comparison reports")
            
            # Generate main comparison report
            self._generate_comparison_report(comparison_results)
            
            # Generate detailed difference report
            self._generate_detailed_report(comparison_results)
            
            # Generate CSV summary
            self._generate_csv_summary(comparison_results)
            
            # Generate JSON report for programmatic access
            self._generate_json_report(comparison_results)
            
            self.logger.info("All reports generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating reports: {str(e)}")
            raise
    
    def _generate_comparison_report(self, comparison_results: Dict[str, Any]):
        """
        Generate the main comparison report.
        
        Args:
            comparison_results (Dict[str, Any]): Comparison results
        """
        report_file = self.report_dir / f"comparison_report_{self.run_timestamp}.txt"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                # Write header
                f.write("=" * 80 + "\n")
                f.write("DB SENTINEL UTILITY - COMPARISON REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Run ID: {self.run_timestamp}\n")
                f.write(f"Source Database: {self.config['source_db']['host']}:{self.config['source_db']['port']}\n")
                f.write(f"Target Database: {self.config['target_db']['host']}:{self.config['target_db']['port']}\n")
                f.write("\n")
                
                # Executive Summary
                f.write("EXECUTIVE SUMMARY\n")
                f.write("-" * 40 + "\n")
                
                total_tables = len(comparison_results)
                successful_tables = sum(1 for r in comparison_results.values() if r.get('status') == 'SUCCESS')
                failed_tables = total_tables - successful_tables
                
                total_differences = sum(
                    len(r.get('differences', {}).get('inserts', [])) +
                    len(r.get('differences', {}).get('updates', [])) +
                    len(r.get('differences', {}).get('deletes', []))
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                f.write(f"Total Tables Compared: {total_tables}\n")
                f.write(f"Successful Comparisons: {successful_tables}\n")
                f.write(f"Failed Comparisons: {failed_tables}\n")
                f.write(f"Total Differences Found: {total_differences}\n")
                f.write(f"Tables Requiring Sync: {sum(1 for r in comparison_results.values() if self._has_differences(r))}\n")
                f.write("\n")
                
                # Table-by-Table Results
                f.write("TABLE-BY-TABLE RESULTS\n")
                f.write("-" * 40 + "\n")
                
                for table_name, result in comparison_results.items():
                    f.write(f"\nTable: {table_name}\n")
                    f.write("-" * (len(table_name) + 7) + "\n")
                    
                    if result.get('status') == 'SUCCESS':
                        stats = result.get('statistics', {})
                        differences = result.get('differences', {})
                        
                        f.write(f"Status: SUCCESS\n")
                        f.write(f"Source Schema: {result.get('source_schema', 'N/A')}\n")
                        f.write(f"Target Schema: {result.get('target_schema', 'N/A')}\n")
                        f.write(f"Primary Keys: {', '.join(result.get('primary_keys', []))}\n")
                        f.write(f"Source Row Count: {stats.get('source_row_count', 0):,}\n")
                        f.write(f"Target Row Count: {stats.get('target_row_count', 0):,}\n")
                        f.write(f"Insert Operations: {len(differences.get('inserts', []))}\n")
                        f.write(f"Update Operations: {len(differences.get('updates', []))}\n")
                        f.write(f"Delete Operations: {len(differences.get('deletes', []))}\n")
                        f.write(f"Total Differences: {stats.get('total_differences', 0)}\n")
                        f.write(f"Match Percentage: {stats.get('match_percentage', 0):.2f}%\n")
                        
                        # Verification status if available
                        if 'verified' in result:
                            verified = result['verified']
                            f.write(f"Post-Verification: {'PASSED' if verified.get('status') == 'SUCCESS' else 'FAILED'}\n")
                            if verified.get('status') == 'SUCCESS':
                                verified_stats = verified.get('statistics', {})
                                f.write(f"Verified Inserts: {verified_stats.get('valid_inserts', 0)}\n")
                                f.write(f"Verified Updates: {verified_stats.get('valid_updates', 0)}\n")
                                f.write(f"Verified Deletes: {verified_stats.get('valid_deletes', 0)}\n")
                    else:
                        f.write(f"Status: FAILED\n")
                        f.write(f"Error: {result.get('error', 'Unknown error')}\n")
                
                # Summary Statistics
                f.write("\n\nSUMMARY STATISTICS\n")
                f.write("-" * 40 + "\n")
                
                total_source_rows = sum(
                    r.get('statistics', {}).get('source_row_count', 0)
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                total_target_rows = sum(
                    r.get('statistics', {}).get('target_row_count', 0)
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                total_inserts = sum(
                    len(r.get('differences', {}).get('inserts', []))
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                total_updates = sum(
                    len(r.get('differences', {}).get('updates', []))
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                total_deletes = sum(
                    len(r.get('differences', {}).get('deletes', []))
                    for r in comparison_results.values() if r.get('status') == 'SUCCESS'
                )
                
                f.write(f"Total Source Rows: {total_source_rows:,}\n")
                f.write(f"Total Target Rows: {total_target_rows:,}\n")
                f.write(f"Total Insert Operations: {total_inserts:,}\n")
                f.write(f"Total Update Operations: {total_updates:,}\n")
                f.write(f"Total Delete Operations: {total_deletes:,}\n")
                f.write(f"Overall Differences: {total_differences:,}\n")
                
                if max(total_source_rows, total_target_rows) > 0:
                    overall_match_pct = ((max(total_source_rows, total_target_rows) - total_differences) / 
                                       max(total_source_rows, total_target_rows)) * 100
                    f.write(f"Overall Match Percentage: {overall_match_pct:.2f}%\n")
                
                # Configuration Summary
                f.write("\n\nCONFIGURATION SUMMARY\n")
                f.write("-" * 40 + "\n")
                f.write(f"Batch Size: {self.config['performance']['batch_size']:,}\n")
                f.write(f"Max Workers: {self.config['performance']['max_workers']}\n")
                f.write(f"Verification Enabled: {'Yes' if self.config['verification']['enabled'] else 'No'}\n")
                f.write(f"Restart Capability: {'Yes' if self.config['restart']['enabled'] else 'No'}\n")
                
                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")
            
            self.logger.info(f"Main comparison report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating comparison report: {str(e)}")
            raise
    
    def _generate_detailed_report(self, comparison_results: Dict[str, Any]):
        """
        Generate detailed difference report with sample data.
        
        Args:
            comparison_results (Dict[str, Any]): Comparison results
        """
        report_file = self.report_dir / f"detailed_differences_{self.run_timestamp}.txt"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("DB SENTINEL UTILITY - DETAILED DIFFERENCES REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Run ID: {self.run_timestamp}\n")
                f.write("\n")
                
                for table_name, result in comparison_results.items():
                    if result.get('status') == 'SUCCESS' and self._has_differences(result):
                        differences = result.get('differences', {})
                        
                        f.write(f"\nTABLE: {table_name}\n")
                        f.write("=" * (len(table_name) + 7) + "\n")
                        
                        # Insert differences
                        inserts = differences.get('inserts', [])
                        if inserts:
                            f.write(f"\nINSERT OPERATIONS ({len(inserts)} rows)\n")
                            f.write("-" * 40 + "\n")
                            
                            # Show sample inserts (first 5)
                            for i, insert in enumerate(inserts[:5]):
                                f.write(f"\nInsert #{i+1}:\n")
                                f.write(f"Primary Key: {insert['primary_key']}\n")
                                f.write(f"Hash: {insert['hash'][:16]}...\n")
                                
                                # Show data sample
                                data = insert.get('data', {})
                                for col, value in list(data.items())[:10]:  # First 10 columns
                                    f.write(f"  {col}: {str(value)[:50]}\n")
                                
                                if len(data) > 10:
                                    f.write(f"  ... and {len(data) - 10} more columns\n")
                            
                            if len(inserts) > 5:
                                f.write(f"\n... and {len(inserts) - 5} more insert operations\n")
                        
                        # Update differences
                        updates = differences.get('updates', [])
                        if updates:
                            f.write(f"\nUPDATE OPERATIONS ({len(updates)} rows)\n")
                            f.write("-" * 40 + "\n")
                            
                            # Show sample updates (first 5)
                            for i, update in enumerate(updates[:5]):
                                f.write(f"\nUpdate #{i+1}:\n")
                                f.write(f"Primary Key: {update['primary_key']}\n")
                                f.write(f"Source Hash: {update['source_hash'][:16]}...\n")
                                f.write(f"Target Hash: {update['target_hash'][:16]}...\n")
                                
                                # Show changed columns
                                source_data = update.get('source_data', {})
                                target_data = update.get('target_data', {})
                                
                                f.write("Changed Columns:\n")
                                changed_count = 0
                                for col in source_data.keys():
                                    if col in target_data and str(source_data[col]) != str(target_data[col]):
                                        f.write(f"  {col}: '{target_data[col]}' -> '{source_data[col]}'\n")
                                        changed_count += 1
                                        if changed_count >= 10:  # Limit output
                                            break
                                
                                if changed_count == 0:
                                    f.write("  (No visible differences in displayed columns)\n")
                            
                            if len(updates) > 5:
                                f.write(f"\n... and {len(updates) - 5} more update operations\n")
                        
                        # Delete differences
                        deletes = differences.get('deletes', [])
                        if deletes:
                            f.write(f"\nDELETE OPERATIONS ({len(deletes)} rows)\n")
                            f.write("-" * 40 + "\n")
                            
                            # Show sample deletes (first 5)
                            for i, delete in enumerate(deletes[:5]):
                                f.write(f"\nDelete #{i+1}:\n")
                                f.write(f"Primary Key: {delete['primary_key']}\n")
                                f.write(f"Hash: {delete['hash'][:16]}...\n")
                                
                                # Show data sample
                                data = delete.get('data', {})
                                for col, value in list(data.items())[:10]:  # First 10 columns
                                    f.write(f"  {col}: {str(value)[:50]}\n")
                                
                                if len(data) > 10:
                                    f.write(f"  ... and {len(data) - 10} more columns\n")
                            
                            if len(deletes) > 5:
                                f.write(f"\n... and {len(deletes) - 5} more delete operations\n")
            
            self.logger.info(f"Detailed differences report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating detailed report: {str(e)}")
            raise
    
    def _generate_csv_summary(self, comparison_results: Dict[str, Any]):
        """
        Generate CSV summary for easy data analysis.
        
        Args:
            comparison_results (Dict[str, Any]): Comparison results
        """
        csv_file = self.report_dir / f"summary_{self.run_timestamp}.csv"
        
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'Table Name', 'Status', 'Source Schema', 'Target Schema',
                    'Source Row Count', 'Target Row Count', 'Insert Count',
                    'Update Count', 'Delete Count', 'Total Differences',
                    'Match Percentage', 'Verification Status', 'Error Message'
                ])
                
                # Write data rows
                for table_name, result in comparison_results.items():
                    if result.get('status') == 'SUCCESS':
                        stats = result.get('statistics', {})
                        differences = result.get('differences', {})
                        verified = result.get('verified', {})
                        
                        writer.writerow([
                            table_name,
                            result.get('status', ''),
                            result.get('source_schema', ''),
                            result.get('target_schema', ''),
                            stats.get('source_row_count', 0),
                            stats.get('target_row_count', 0),
                            len(differences.get('inserts', [])),
                            len(differences.get('updates', [])),
                            len(differences.get('deletes', [])),
                            stats.get('total_differences', 0),
                            round(stats.get('match_percentage', 0), 2),
                            verified.get('status', 'N/A'),
                            ''
                        ])
                    else:
                        writer.writerow([
                            table_name,
                            result.get('status', ''),
                            '', '', 0, 0, 0, 0, 0, 0, 0.0, 'N/A',
                            result.get('error', '')
                        ])
            
            self.logger.info(f"CSV summary generated: {csv_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating CSV summary: {str(e)}")
            raise
    
    def _generate_json_report(self, comparison_results: Dict[str, Any]):
        """
        Generate JSON report for programmatic access.
        
        Args:
            comparison_results (Dict[str, Any]): Comparison results
        """
        json_file = self.report_dir / f"results_{self.run_timestamp}.json"
        
        try:
            # Create a clean version of results for JSON serialization
            clean_results = {}
            
            for table_name, result in comparison_results.items():
                clean_result = {
                    'table_name': table_name,
                    'status': result.get('status', ''),
                    'timestamp': self.run_timestamp
                }
                
                if result.get('status') == 'SUCCESS':
                    clean_result.update({
                        'source_schema': result.get('source_schema', ''),
                        'target_schema': result.get('target_schema', ''),
                        'primary_keys': result.get('primary_keys', []),
                        'statistics': result.get('statistics', {}),
                        'difference_counts': {
                            'inserts': len(result.get('differences', {}).get('inserts', [])),
                            'updates': len(result.get('differences', {}).get('updates', [])),
                            'deletes': len(result.get('differences', {}).get('deletes', []))
                        },
                        'hash_algorithm': result.get('hash_algorithm', 'sha256')
                    })
                    
                    if 'verified' in result:
                        clean_result['verification'] = result['verified']
                else:
                    clean_result['error'] = result.get('error', '')
                
                clean_results[table_name] = clean_result
            
            # Add metadata
            report_data = {
                'metadata': {
                    'run_id': self.run_timestamp,
                    'generated_at': datetime.now().isoformat(),
                    'source_host': self.config['source_db']['host'],
                    'target_host': self.config['target_db']['host'],
                    'total_tables': len(comparison_results),
                    'successful_tables': sum(1 for r in comparison_results.values() if r.get('status') == 'SUCCESS'),
                    'failed_tables': sum(1 for r in comparison_results.values() if r.get('status') != 'SUCCESS')
                },
                'results': clean_results
            }
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            self.logger.info(f"JSON report generated: {json_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating JSON report: {str(e)}")
            raise
    
    def generate_verified_report(self, comparison_results: Dict[str, Any]):
        """
        Generate post-verification report.
        
        Args:
            comparison_results (Dict[str, Any]): Comparison results with verification data
        """
        report_file = self.report_dir / f"verified_comparison_report_{self.run_timestamp}.txt"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("DB SENTINEL UTILITY - POST-VERIFICATION REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Run ID: {self.run_timestamp}\n")
                f.write("\n")
                
                f.write("VERIFICATION SUMMARY\n")
                f.write("-" * 40 + "\n")
                
                verified_tables = 0
                total_verified_operations = 0
                
                for table_name, result in comparison_results.items():
                    if result.get('status') == 'SUCCESS' and 'verified' in result:
                        verified_tables += 1
                        verified = result['verified']
                        if verified.get('status') == 'SUCCESS':
                            stats = verified.get('statistics', {})
                            total_verified_operations += (
                                stats.get('valid_inserts', 0) +
                                stats.get('valid_updates', 0) +
                                stats.get('valid_deletes', 0)
                            )
                
                f.write(f"Tables Verified: {verified_tables}\n")
                f.write(f"Total Verified Operations: {total_verified_operations}\n")
                f.write("\n")
                
                # Table-by-table verification results
                f.write("VERIFICATION RESULTS BY TABLE\n")
                f.write("-" * 40 + "\n")
                
                for table_name, result in comparison_results.items():
                    if result.get('status') == 'SUCCESS' and 'verified' in result:
                        verified = result['verified']
                        
                        f.write(f"\nTable: {table_name}\n")
                        f.write("-" * (len(table_name) + 7) + "\n")
                        f.write(f"Verification Status: {verified.get('status', 'UNKNOWN')}\n")
                        
                        if verified.get('status') == 'SUCCESS':
                            stats = verified.get('statistics', {})
                            f.write(f"Valid Insert Operations: {stats.get('valid_inserts', 0)}\n")
                            f.write(f"Valid Update Operations: {stats.get('valid_updates', 0)}\n")
                            f.write(f"Valid Delete Operations: {stats.get('valid_deletes', 0)}\n")
                            f.write(f"Invalid Operations: {stats.get('invalid_operations', 0)}\n")
                            f.write(f"Verification Percentage: {stats.get('verification_percentage', 0):.2f}%\n")
                            
                            if 'generated_files' in verified:
                                f.write(f"Verified SQL Files: {', '.join(verified['generated_files'])}\n")
                        else:
                            f.write(f"Verification Error: {verified.get('error', 'Unknown error')}\n")
                
                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF VERIFICATION REPORT\n")
                f.write("=" * 80 + "\n")
            
            self.logger.info(f"Post-verification report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating verification report: {str(e)}")
            raise
    
    def _has_differences(self, result: Dict[str, Any]) -> bool:
        """
        Check if a table has any differences.
        
        Args:
            result (Dict[str, Any]): Table comparison result
            
        Returns:
            bool: True if table has differences, False otherwise
        """
        if result.get('status') != 'SUCCESS':
            return False
        
        differences = result.get('differences', {})
        return (len(differences.get('inserts', [])) > 0 or
                len(differences.get('updates', [])) > 0 or
                len(differences.get('deletes', [])) > 0)
    
    def get_report_paths(self) -> Dict[str, str]:
        """
        Get paths to all generated reports.
        
        Returns:
            Dict[str, str]: Dictionary of report types to file paths
        """
        return {
            'comparison_report': str(self.report_dir / f"comparison_report_{self.run_timestamp}.txt"),
            'detailed_report': str(self.report_dir / f"detailed_differences_{self.run_timestamp}.txt"),
            'csv_summary': str(self.report_dir / f"summary_{self.run_timestamp}.csv"),
            'json_report': str(self.report_dir / f"results_{self.run_timestamp}.json"),
            'verified_report': str(self.report_dir / f"verified_comparison_report_{self.run_timestamp}.txt")
        }
