# Distributed Storage System

A p2p key-value storage system implementation using the Actor model with Akka. This system provides distributed data storage with configurable replication, consistency guarantees, and failure handling capabilities.

The Repo includes the project description (instructions) and our final report.

### Premises

We decided to adopt a 4-eyes, 2-hands approach for this project rathen than a 4-hands approach. This means that we didn't work in parallel on different parts of the code, but always worked together on the same piece of code. On one hand this slowed us down, but on the other hand we've been able to produce much better code. This approach allowed the person that wasn't actively typing to think about the flaws and notice the other person's errors. As a result, the work went smoothly and we rarely found problems in our solution.

## 🏗️ System Architecture

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

## 🚀 Quick Start

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

## 🎯 Key Features

### 1. Distributed Key-Value Storage

- **N Replication**: Keys are distributed across N nodes using consistent hashing
- **Versioned Values**: Each value is versioned to handle concurrent updates
- **Atomic Operations**: Read and write operations are atomic at the key level

### 2. Configurable Consistency Model

The system uses N/W/R parameters:

- **N**: Total number of replicas per key
- **W**: Number of nodes that must acknowledge a write
- **R**: Number of nodes that must respond to a read

Default configuration: N=3, W=2, R=2 (strong consistency)

### 3. Fault Tolerance

- **Node Crashes**: System continues operating when nodes fail (at least one has to remain alive)
- **Network Partitions**: Handles network splits gracefully
- **Timeout Handling**: Client and node timeouts for unresponsive operations
- **Recovery**: Crashed nodes can rejoin and synchronize data

### 4. Dynamic Membership

- **Join Protocol**: New nodes can join the network dynamically
- **Leave Protocol**: Nodes can gracefully leave, transferring their data
- **Data Repartitioning**: Automatic data redistribution on membership changes

## 🧪 Interactive Testing Framework

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

### System Initialization

The system automatically initializes with default configuration (N=3, W=2, R=2) when started. The configuration is managed by [`DataStoreManager`](src/main/java/it/unitn/ds1/DataStoreManager.java) and cannot be changed at runtime.

## 📊 System Parameters

### Default Configuration

```java
// DataStoreManager defaults (from DataStoreManager.getInstance())
N = 3  // Number of replicas
W = 2  // Write quorum size  
R = 2  // Read quorum size
```

### Timeout Configuration

Timeouts are compile-time constants defined in [`TimeoutDelay.java`](src/main/java/it/unitn/ds1/utils/TimeoutDelay.java) and [`OperationDelays.java`](src/main/java/it/unitn/ds1/utils/OperationDelays.java):

```java
// From TimeoutDelay.java - these are static final constants
CLIENT_GET_DELAY = BASE_DELAY * 4.5      // ~2025ms
CLIENT_UPDATE_DELAY = BASE_DELAY * 4.75  // ~2137ms  
JOIN_DELAY = BASE_DELAY * 6.2            // ~2790ms
```

### Logging Configuration

The system uses Logback for structured logging. Configuration in `src/main/resources/logback.xml`:

```xml
<!-- Configurable log levels per component -->
- StorageNode operations
- Client requests and responses  
- System membership changes
- Error conditions and timeouts
```

## 🔧 Advanced Usage

### Custom Test Scenarios

You can create custom test scenarios by extending the testing framework:

```java
// Example: Custom consistency test
private static void customConsistencyTest() {
    // Your test implementation
    ActorRef client = getRandomClient();
    ActorRef coordinator = getRandomActiveNode();
    
    // Perform operations and validate consistency
    client.tell(new Messages.InitiateUpdate(key, value, coordinator), ActorRef.noSender());
    // ... validation logic
}
```

### Configuration Tuning

Modify `DataStoreManager` initialization for different consistency models:

```java
// Strong consistency (default)
DataStoreManager.initialize(3, 2, 2);

// Eventual consistency  
DataStoreManager.initialize(3, 1, 1);

// Custom configuration
DataStoreManager.initialize(N, W, R);
```

## 📈 Performance Characteristics

### Throughput

- **Write Operations**: Depends on W (write quorum size)
- **Read Operations**: Depends on R (read quorum size)  
- **Network Overhead**: O(N) messages per operation

### Latency

- **Best Case**: Single round-trip to coordinator
- **Worst Case**: Multiple rounds for quorum satisfaction
- **Timeout Handling**: Configurable per operation type

### Scalability

- **Horizontal Scaling**: Add nodes dynamically
- **Load Distribution**: Even key distribution via hashing
- **Fault Tolerance**: System operates with (N-W+1) node failures

## 🐛 Troubleshooting

### Common Issues

1. **Nodes Not Responding**

   ```bash
   # Check node status
   > status
   
   # Verify network connectivity
   > nodes
   ```

2. **Timeout Errors**

   ```bash
   # Adjust timeout values in TimeoutDelay.java
   # Check system load and delays
   ```

3. **Inconsistent Reads**

   ```bash
   # Verify R >= W/2 + 1 for strong consistency
   # Check for network partitions
   ```

## 📚 References

- **Akka Documentation**: [https://akka.io/docs/](https://akka.io/docs/)
- **Distributed Systems Concepts**: Consistency, Availability, Replication, Fault Tolerance, Sequential Consistency, Quorum, Concurrency.

## 🤝 Contributing

This is an academic project for distributed systems coursework. The implementation demonstrates:

- Actor model programming with Akka
- Distributed consensus and consistency protocols  
- Fault tolerance and recovery mechanisms
- Performance testing and evaluation methodologies

## 📄 License

Academic project - do whatever you please
