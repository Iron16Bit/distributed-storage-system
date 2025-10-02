# Distributed Storage System

A p2p key-value storage system implementation using the Actor model with Akka. This system provides distributed data storage with configurable replication, consistency guarantees, and failure handling capabilities.

The Repo includes the project description (instructions) and our final report.

### Premises

We decided to adopt a 4-eyes, 2-hands approach for this project rathen than a 4-hands approach. This means that we didn't work in parallel on different parts of the code, but always worked together on the same piece of code. On one hand this slowed us down, but on the other hand we've been able to produce much better code. This approach allowed the person that wasn't actively typing to think about the flaws and notice the other person's errors. As a result, the work went smoothly and we rarely found problems in our solution.

## System Architecture

The system implements a distributed storage system with the following key features:

- **Actor-based Architecture**: Built using Akka actors for concurrent and distributed processing
- **Configurable Replication**: Supports N/W/R parameters for tunable consistency and availability
- **Fault Tolerance**: Handles node crashes, network partitions, and recovery scenarios
- **Quorum-based Operations**: Ensures data consistency through configurable read/write quorums
- **Dynamic Membership**: Supports joining and leaving of nodes during runtime

### Core Components

```text
src/main/java/it/unitn/ds1/
├── actors/
│   ├── StorageNode.java      # Core storage node implementation
│   └── Client.java           # Client actor for operations
├── test/
│   ├── InteractiveTest.java  # Interactive testing framework
│   └── LargeScaleTest.java   # Automated large-scale tests
├── types/
│   └── OperationType.java    # Operation type definitions
├── utils/
│   ├── VersionedValue.java   # Version-based data consistency
│   ├── TimeoutDelay.java     # Timeout management
│   └── OperationDelays.java  # Operation timing configuration 
├── Messages.java             # Actor message definitions
├── DataStoreManager.java     # Global configuration manager
└── Main.java                # Basic demo application
```

## Quick Start

### Prerequisites

- Java 21 or higher
- Gradle 9.0 was used

### Running the System

```bash
# Build the project
gradle build

# Run interactive testing framework (recommended)
gradle run --console=plain

# Alternative: Run specific main classes by editing build.gradle:
# - it.unitn.ds1.test.InteractiveTest (default - interactive testing)
# - it.unitn.ds1.test.LargeScaleTest (automated large-scale tests)  
# - it.unitn.ds1.Main (basic demo scenarios)
```

### Running Tests

```bash
# Run with interactive test framework (default)
gradle run

# Run large-scale automated tests
# Edit build.gradle to uncomment: mainClassName = "it.unitn.ds1.test.LargeScaleTest"
gradle run

# Run basic demo
# Edit build.gradle to uncomment: mainClassName = "it.unitn.ds1.Main"
gradle run
```

## Interactive Testing Framework

The system includes a comprehensive interactive testing framework accessible via [`InteractiveTest.java`](src/main/java/it/unitn/ds1/test/InteractiveTest.java):

### Available Commands

```bash
# System Information
help, h                  # Show help message
status, s                # Show system status  
nodes                    # List all nodes and their states
clients                  # List all clients
data                     # Show data distribution

# Basic Operations
get <key> [nodeId] [clientId]         # Read value for key
put <key> <value> [nodeId] [clientId] # Store key-value pair

# Node Management  
addnode [nodeId]         # Add a new node to the system
removenode <nodeId>      # Remove a node from the system
crash <nodeId>           # Crash a specific node
recover <nodeId> [peerNodeId] # Recover a crashed node
join <nodeId> <peerNodeId>    # Join a node via bootstrap peer
leave <nodeId>           # Node graceful departure

# Client Management
addclient                # Add a new client actor
removeclient <clientId>  # Remove a client actor

# Testing Scenarios
test basic               # Test basic CRUD operations
test quorum             # Test quorum behavior with failures
test concurrency        # Test concurrent operations
test consistency        # Test data consistency across nodes
test membership         # Test join/leave operations
test partition          # Test network partition scenarios
test recovery           # Test crash recovery scenarios
test edge-cases         # Test edge cases and error conditions
test timeout            # Test client timeout scenarios

# Performance Testing
benchmark <operations>   # Run performance benchmark
stress <duration>        # Run stress test for specified seconds

# System Control
reset                    # Reset entire system to initial state
clear                    # Clear screen
quit, exit, q           # Exit the system
```
