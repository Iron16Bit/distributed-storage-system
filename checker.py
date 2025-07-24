#!/usr/bin/env python3
"""
Correctness checker for distributed storage system execution.
Verifies data consistency, replication, and system behavior from log files.
Works with both Main.java and LargeScaleTest outputs.
"""

import re
import sys
from collections import defaultdict, namedtuple
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class LogEntry:
    timestamp: str
    level: str
    logger: str
    message: str
    line: str

@dataclass
class VersionedValue:
    value: str
    version: int
    
    def __str__(self):
        return f"{self.value} (v{self.version})"

@dataclass
class UpdateOperation:
    node_id: int
    key: int
    value: str
    operation_id: Optional[str] = None
    timestamp: Optional[str] = None

@dataclass
class GetOperation:
    node_id: int
    key: int
    timestamp: Optional[str] = None

class DistributedStorageChecker:
    def __init__(self):
        self.log_entries: List[LogEntry] = []
        self.node_registry_states: Dict[str, Set[int]] = {}  # timestamp -> set of active nodes
        self.update_operations: List[UpdateOperation] = []
        self.get_operations: List[GetOperation] = []
        self.client_responses: List[Dict] = []
        self.node_datastores: Dict[int, Dict[int, VersionedValue]] = defaultdict(dict)
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.join_operations: List[Tuple[str, int, str]] = []  # timestamp, node_id, bootstrap_peer
        
    def parse_log_file(self, filename: str):
        """Parse the log file and extract relevant information."""
        print(f"Parsing log file: {filename}")
        
        with open(filename, 'r') as f:
            lines = f.readlines()
            
        parsed_count = 0
        for line_num, line in enumerate(lines, 1):
            try:
                entry = self._parse_log_line(line.strip())
                if entry:
                    self.log_entries.append(entry)
                    self._extract_operations(entry)
                    parsed_count += 1
            except Exception as e:
                self.warnings.append(f"Failed to parse line {line_num}: {e}")
        
        print(f"Successfully parsed {parsed_count} log entries from {len(lines)} total lines")
                
    def _parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line."""
        # Skip non-log lines (like build output)
        if not re.search(r'\d{2}:\d{2}:\d{2}\.\d{3}', line):
            return None
            
        # First, remove ALL ANSI escape codes to simplify parsing
        clean_line = re.sub(r'\x1b\[[0-9;]*m', '', line)
        clean_line = re.sub(r'\[[0-9;]*m', '', clean_line)  # Also handle \[XXm format
        
        # Debug: print first few lines to see exact format
        if len(self.log_entries) < 5:
            print(f"DEBUG: Original line: {repr(line[:100])}")
            print(f"DEBUG: Cleaned line: {repr(clean_line[:100])}")
        
        # Pattern for logback format: timestamp LEVEL logger - message
        pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})\s+(\w+)\s+([^\s-]+)\s+-\s+(.+)'
        match = re.match(pattern, clean_line)
        
        if match:
            timestamp, level, logger, message = match.groups()
            if len(self.log_entries) < 5:
                print(f"DEBUG: Successfully parsed - Time: {timestamp}, Level: {level}, Logger: {logger}, Message: {message[:50]}...")
            return LogEntry(timestamp, level, logger.strip(), message, line)
        
        # Alternative pattern without strict spacing
        alt_pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})\s+(\w+)\s+(.+?)\s+-\s+(.+)'
        alt_match = re.match(alt_pattern, clean_line)
        
        if alt_match:
            timestamp, level, logger, message = alt_match.groups()
            if len(self.log_entries) < 5:
                print(f"DEBUG: Successfully parsed with alt pattern - Time: {timestamp}, Level: {level}, Logger: {logger}")
            return LogEntry(timestamp, level, logger.strip(), message, line)
        
        if len(self.log_entries) < 5:
            print(f"DEBUG: Failed to parse cleaned line: {repr(clean_line[:100])}")
        return None
        
    def _extract_operations(self, entry: LogEntry):
        """Extract operations and state changes from log entries."""
        message = entry.message
        
        # Extract UPDATE operations
        if "UPDATE request:" in message:
            match = re.search(r'Node (\d+) - UPDATE request: key=(\d+), value=(.+)', message)
            if match:
                node_id, key, value = int(match.group(1)), int(match.group(2)), match.group(3)
                self.update_operations.append(UpdateOperation(node_id, key, value, timestamp=entry.timestamp))
                
        # Extract GET operations
        elif "GET request:" in message:
            match = re.search(r'Node (\d+) - GET request: key=(\d+)', message)
            if match:
                node_id, key = int(match.group(1)), int(match.group(2))
                self.get_operations.append(GetOperation(node_id, key, timestamp=entry.timestamp))
                
        # Extract client responses
        elif "UPDATE ACCEPTED" in message or "GET ACCEPTED" in message or "GET REJECTED" in message:
            self._parse_client_response(entry)
            
        # Extract node datastore states
        elif "DataStore:" in message:
            self._parse_datastore_state(entry)
            
        # Extract join operations (for both Main.java and LargeScaleTest)
        elif "Add new node while system is active" in message or "Node Join operation" in message:
            # This is a join test phase
            pass
        elif "joining network via bootstrap peer" in message:
            match = re.search(r'Node (\d+) joining network via bootstrap peer (\d+)', message)
            if match:
                node_id, bootstrap_peer = int(match.group(1)), match.group(2)
                self.join_operations.append((entry.timestamp, node_id, bootstrap_peer))
                
    def _parse_client_response(self, entry: LogEntry):
        """Parse client response messages."""
        message = entry.message
        
        if "UPDATE ACCEPTED" in message:
            match = re.search(r'UPDATE ACCEPTED - Key: (\d+), Value: (.+) \(v(\d+)\)', message)
            if match:
                key, value, version = int(match.group(1)), match.group(2), int(match.group(3))
                self.client_responses.append({
                    'type': 'UPDATE_ACCEPTED',
                    'key': key,
                    'value': value,
                    'version': version,
                    'timestamp': entry.timestamp
                })
                
        elif "GET ACCEPTED" in message:
            match = re.search(r'GET ACCEPTED - Key: (\d+), Value: (.+)', message)
            if match:
                key, value_version = int(match.group(1)), match.group(2)
                # Extract version from value string
                version_match = re.search(r'\(v(\d+)\)', value_version)
                version = int(version_match.group(1)) if version_match else 1
                value = re.sub(r' \(v\d+\)', '', value_version)
                
                self.client_responses.append({
                    'type': 'GET_ACCEPTED',
                    'key': key,
                    'value': value,
                    'version': version,
                    'timestamp': entry.timestamp
                })
                
        elif "GET REJECTED" in message:
            match = re.search(r'GET REJECTED - Key (\d+) not found', message)
            if match:
                key = int(match.group(1))
                self.client_responses.append({
                    'type': 'GET_REJECTED',
                    'key': key,
                    'timestamp': entry.timestamp
                })
                
    def _parse_datastore_state(self, entry: LogEntry):
        """Parse node datastore state from debug prints."""
        message = entry.message
        
        # Handle both formats: "Node X DataStore: {}" and "Node X DataStore: {key=value (vN), ...}"
        match = re.search(r'Node (\d+) DataStore: \{([^}]*)\}', message)
        
        if match:
            node_id = int(match.group(1))
            datastore_str = match.group(2)
            
            # Parse key-value pairs
            datastore = {}
            if datastore_str.strip():
                # Handle the format: key=value (vX), key=value (vY), ...
                # More flexible pattern to handle spaces and commas
                items = re.findall(r'(\d+)=([^(),]+)\s*\(v(\d+)\)', datastore_str)
                for key_str, value, version_str in items:
                    key, version = int(key_str), int(version_str)
                    datastore[key] = VersionedValue(value.strip(), version)
                    
            self.node_datastores[node_id] = datastore
            
            # Debug: print what we parsed
            if len(self.node_datastores) <= 5:
                print(f"DEBUG: Parsed datastore for Node {node_id}: {len(datastore)} items")
            
    def check_data_consistency(self) -> List[str]:
        """Check if all nodes have consistent data for each key."""
        issues = []
        
        # Get all keys across all nodes
        all_keys = set()
        for node_data in self.node_datastores.values():
            all_keys.update(node_data.keys())
            
        print(f"Checking consistency for {len(all_keys)} keys across {len(self.node_datastores)} nodes")
        
        for key in all_keys:
            # Get all versions of this key across nodes
            key_data = {}  # (value, version) -> list of node_ids
            nodes_with_key = []
            
            for node_id, datastore in self.node_datastores.items():
                if key in datastore:
                    versioned_value = datastore[key]
                    value_version_tuple = (versioned_value.value, versioned_value.version)
                    
                    if value_version_tuple not in key_data:
                        key_data[value_version_tuple] = []
                    key_data[value_version_tuple].append(node_id)
                    nodes_with_key.append(node_id)
            
            # Check 1: Same version should have same value
            version_to_values = defaultdict(set)
            for (value, version), nodes in key_data.items():
                version_to_values[version].add(value)
                
            for version, values in version_to_values.items():
                if len(values) > 1:
                    issues.append(f"Inconsistent values for key {key} version {version}: {list(values)}")
            
            # Check 2: Majority should have the most recent version
            if len(key_data) > 1:  # Only check if there are multiple versions
                # Find the most recent version
                max_version = max(version for (value, version) in key_data.keys())
                
                # Count nodes with each version
                version_counts = defaultdict(int)
                for (value, version), nodes in key_data.items():
                    version_counts[version] += len(nodes)
                
                # Check if majority has the most recent version
                total_nodes_with_key = len(nodes_with_key)
                nodes_with_latest = version_counts[max_version]
                majority_threshold = (total_nodes_with_key + 1) // 2  # More than half
                
                if nodes_with_latest < majority_threshold:
                    issues.append(f"Key {key}: Latest version v{max_version} not on majority of nodes ({nodes_with_latest}/{total_nodes_with_key})")
                    
                    # Show the distribution for debugging
                    issues.append(f"  Version distribution for key {key}:")
                    for (value, version), nodes in sorted(key_data.items(), key=lambda x: x[0][1], reverse=True):
                        issues.append(f"    {value} (v{version}): nodes {nodes} ({len(nodes)} nodes)")
                    
        return issues
        
    def check_replication_factor(self) -> List[str]:
        """Check if data is properly replicated across nodes."""
        issues = []
        
        # Count nodes that have each key
        key_replication = defaultdict(int)
        for datastore in self.node_datastores.values():
            for key in datastore.keys():
                key_replication[key] += 1
                
        total_nodes = len(self.node_datastores)
        # Based on DataStoreManager.java, N=2 for replication
        expected_replicas = min(2, total_nodes) if total_nodes > 0 else 0
        
        for key, replica_count in key_replication.items():
            if replica_count < expected_replicas:
                issues.append(f"Key {key} under-replicated: {replica_count}/{expected_replicas} replicas")
                
        return issues
        
    def check_version_monotonicity(self) -> List[str]:
        """Check if version numbers are monotonically increasing for each key."""
        issues = []
        
        # Track version updates for each key from client responses
        key_versions = defaultdict(list)
        
        for response in self.client_responses:
            if response['type'] == 'UPDATE_ACCEPTED':
                key = response['key']
                version = response['version']
                timestamp = response['timestamp']
                key_versions[key].append((timestamp, version))
                
        for key, version_history in key_versions.items():
            # Sort by timestamp
            version_history.sort(key=lambda x: x[0])
            
            for i in range(1, len(version_history)):
                prev_version = version_history[i-1][1]
                curr_version = version_history[i][1]
                
                if curr_version <= prev_version:
                    issues.append(f"Non-monotonic version for key {key}: v{prev_version} -> v{curr_version}")
                    
        return issues
        
    def check_successful_operations(self) -> Dict[str, int]:
        """Count successful operations."""
        stats = {
            'total_updates': len(self.update_operations),
            'total_gets': len(self.get_operations),
            'successful_updates': len([r for r in self.client_responses if r['type'] == 'UPDATE_ACCEPTED']),
            'successful_gets': len([r for r in self.client_responses if r['type'] == 'GET_ACCEPTED']),
            'failed_gets': len([r for r in self.client_responses if r['type'] == 'GET_REJECTED']),
            'total_joins': len(self.join_operations)
        }
        
        return stats
        
    def check_join_operations(self) -> List[str]:
        """Verify join operations are working correctly."""
        issues = []
        
        # For Main.java test, we expect fewer nodes (3 initial + maybe 1 joined)
        # For LargeScaleTest, expect 30 nodes (10 initial + 20 joining)
        # Determine test type based on log content
        is_large_scale = any("Large Scale" in entry.message for entry in self.log_entries)
        
        if is_large_scale:
            expected_initial_nodes = 10
            expected_joining_nodes = 20
            expected_final_nodes = expected_initial_nodes + expected_joining_nodes
        else:
            expected_initial_nodes = 3  # nodes 1, 5, 10 from Main.java
            expected_final_nodes = 4  # potentially one more joining
        
        actual_nodes = len(self.node_datastores)
        
        if actual_nodes < expected_initial_nodes:
            issues.append(f"Expected at least {expected_initial_nodes} initial nodes, but found {actual_nodes} in final state")
        
        # Check if there's reasonable data distribution
        if self.node_datastores:
            total_items = sum(len(datastore) for datastore in self.node_datastores.values())
            if total_items == 0:
                issues.append("No data found in any node datastores")
        else:
            issues.append("No node datastores found in log")
            
        return issues
    
    def analyze_test_phases(self) -> Dict[str, any]:
        """Analyze different phases of the test execution."""
        phases = {
            'initialization': 0,
            'basic_operations': 0,
            'multi_client_operations': 0,
            'quorum_tests': 0,
            'membership_tests': 0,
            'concurrency_tests': 0,
            'error_scenarios': 0,
            'resilience_tests': 0,
            'random_selection_tests': 0,
            'large_scale_phases': 0
        }
        
        # Count operations in different phases based on both Main.java and LargeScaleTest
        for entry in self.log_entries:
            message = entry.message
            if "Starting Distributed Storage System" in message or "Starting Large Scale" in message:
                phases['initialization'] += 1
            elif "TESTING BASIC CRUD OPERATIONS" in message:
                phases['basic_operations'] += 1
            elif "TESTING MULTI-CLIENT OPERATIONS" in message:
                phases['multi_client_operations'] += 1
            elif "TESTING QUORUM WITH CRASHED NODES" in message:
                phases['quorum_tests'] += 1
            elif "TESTING MEMBERSHIP UNDER LOAD" in message:
                phases['membership_tests'] += 1
            elif "TESTING CONCURRENCY AND RACE CONDITIONS" in message:
                phases['concurrency_tests'] += 1
            elif "TESTING ERROR SCENARIOS" in message:
                phases['error_scenarios'] += 1
            elif "TESTING SYSTEM RESILIENCE" in message:
                phases['resilience_tests'] += 1
            elif "TESTING RANDOM NODE SELECTION" in message:
                phases['random_selection_tests'] += 1
            elif "Large Scale" in message:
                phases['large_scale_phases'] += 1
                
        return phases
        
    def generate_report(self) -> str:
        """Generate a comprehensive correctness report."""
        report = []
        report.append("=" * 80)
        report.append("DISTRIBUTED STORAGE SYSTEM CORRECTNESS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Test phases analysis
        phases = self.analyze_test_phases()
        report.append("TEST EXECUTION PHASES:")
        for phase_name, count in phases.items():
            if count > 0:  # Only show phases that occurred
                report.append(f"  {phase_name.replace('_', ' ').title()}: {count} events")
        report.append("")
        
        # Basic statistics
        stats = self.check_successful_operations()
        report.append("OPERATION STATISTICS:")
        report.append(f"  Total UPDATE operations: {stats['total_updates']}")
        report.append(f"  Successful UPDATE operations: {stats['successful_updates']}")
        report.append(f"  Total GET operations: {stats['total_gets']}")
        report.append(f"  Successful GET operations: {stats['successful_gets']}")
        report.append(f"  Failed GET operations: {stats['failed_gets']}")
        report.append(f"  Total JOIN operations: {stats['total_joins']}")
        report.append(f"  Active nodes in final state: {len(self.node_datastores)}")
        report.append("")
        
        # Data consistency check
        consistency_issues = self.check_data_consistency()
        report.append("DATA CONSISTENCY CHECK:")
        if consistency_issues:
            report.append("  ❌ ISSUES FOUND:")
            for issue in consistency_issues:
                report.append(f"    {issue}")
        else:
            report.append("  ✅ All data is consistent across nodes")
        report.append("")
        
        # Replication check
        replication_issues = self.check_replication_factor()
        report.append("REPLICATION CHECK:")
        if replication_issues:
            report.append("  ⚠️  ISSUES FOUND:")
            for issue in replication_issues:
                report.append(f"    {issue}")
        else:
            report.append("  ✅ All keys are properly replicated")
        report.append("")
        
        # Version monotonicity check
        version_issues = self.check_version_monotonicity()
        report.append("VERSION MONOTONICITY CHECK:")
        if version_issues:
            report.append("  ❌ ISSUES FOUND:")
            for issue in version_issues:
                report.append(f"    {issue}")
        else:
            report.append("  ✅ Version numbers are monotonically increasing")
        report.append("")
        
        # Join operations check
        join_issues = self.check_join_operations()
        report.append("JOIN OPERATIONS CHECK:")
        if join_issues:
            report.append("  ⚠️  ISSUES FOUND:")
            for issue in join_issues:
                report.append(f"    {issue}")
        else:
            report.append("  ✅ Join operations working correctly")
        report.append("")
        
        # Summary of parsed data
        if self.node_datastores:
            report.append("FINAL SYSTEM STATE:")
            report.append(f"  Nodes with data: {sorted(self.node_datastores.keys())}")
            total_keys = sum(len(datastore) for datastore in self.node_datastores.values())
            unique_keys = set()
            for datastore in self.node_datastores.values():
                unique_keys.update(datastore.keys())
            report.append(f"  Total data items: {total_keys}")
            report.append(f"  Unique keys: {len(unique_keys)}")
            
            # Show sample data from each node
            report.append("  Sample data per node:")
            for node_id in sorted(self.node_datastores.keys()):
                datastore = self.node_datastores[node_id]
                sample_items = list(datastore.items())[:3]  # Show first 3 items
                sample_str = ", ".join([f"{k}={v}" for k, v in sample_items])
                if len(datastore) > 3:
                    sample_str += f" ... (+{len(datastore)-3} more)"
                report.append(f"    Node {node_id}: {sample_str}" if sample_str else f"    Node {node_id}: (empty)")
            report.append("")
        
        # Overall verdict
        total_issues = len(consistency_issues) + len(version_issues)
        total_warnings = len(replication_issues) + len(join_issues)
        
        report.append("OVERALL VERDICT:")
        if total_issues == 0 and total_warnings == 0:
            report.append("  🎉 SYSTEM PASSED ALL CHECKS")
        elif total_issues == 0:
            report.append(f"  ⚠️  SYSTEM PASSED WITH {total_warnings} WARNINGS")
        else:
            report.append(f"  ❌ SYSTEM FAILED WITH {total_issues} CRITICAL ISSUES AND {total_warnings} WARNINGS")
        
        report.append("=" * 80)
        
        return "\n".join(report)
        
    def run_checks(self, log_file: str) -> str:
        """Run all correctness checks on the log file."""
        self.parse_log_file(log_file)
        return self.generate_report()

def main():
    if len(sys.argv) != 2:
        print("Usage: python checker.py <log_file>")
        sys.exit(1)
        
    log_file = sys.argv[1]
    checker = DistributedStorageChecker()
    
    try:
        report = checker.run_checks(log_file)
        print(report)
        
        # Also save report to file
        report_file = log_file.replace('.txt', '_report.txt')
        with open(report_file, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {report_file}")
        
    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()