# Distributed Storage System

A fault-tolerant, peer-to-peer key-value storage system implementation using the Actor model with Akka. This system provides distributed data storage with configurable replication, consistency guarantees, and failure handling capabilities.

## 🏗️ System Architecture

The system implements a distributed hash table (DHT) with the following key features:

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
│   ├── OperationDelays.java  # Operation timing configuration
│   └── FzfIntegration.java   # Command-line fuzzy search
├── Messages.java             # Actor message definitions
├── DataStoreManager.java     # Global configuration manager
└── Main.java                # Basic demo application
```

## 🚀 Quick Start

### Prerequisites

- Java 11 or higher
- Gradle 6.0+

### Building the Project

```bash
# Clone the repository
git clone <repository-url>
cd distributed-storage-system

# Build the project
gradle build

# Run the interactive test suite
gradle run --console=plain
```

### Running Tests

```bash
# Run with interactive test framework (default)
gradle run

# Run large-scale automated tests
# Edit build.gradle to uncomment: mainClassName = "it.unitn.ds1.test.LargeScaleTest"
gardle run

# Run basic demo
# Edit build.gradle to uncomment: mainClassName = "it.unitn.ds1.Main"
gardle run
```

## 🎯 Key Features

### 1. Distributed Key-Value Storage

- **Hash-based Distribution**: Keys are distributed across nodes using consistent hashing
- **Versioned Values**: Each value is versioned to handle concurrent updates
- **Atomic Operations**: Read and write operations are atomic at the key level

### 2. Configurable Consistency Model

The system uses N/W/R parameters:

- **N**: Total number of replicas per key
- **W**: Number of nodes that must acknowledge a write
- **R**: Number of nodes that must respond to a read

Default configuration: N=3, W=2, R=2 (strong consistency)

### 3. Fault Tolerance

- **Node Crashes**: System continues operating when nodes fail
- **Network Partitions**: Handles network splits gracefully
- **Timeout Handling**: Client and node timeouts for unresponsive operations
- **Recovery**: Crashed nodes can rejoin and synchronize data

### 4. Dynamic Membership

- **Join Protocol**: New nodes can join the network dynamically
- **Leave Protocol**: Nodes can gracefully leave, transferring their data
- **Data Repartitioning**: Automatic data redistribution on membership changes

## 🧪 Interactive Testing Framework

The system includes a comprehensive interactive testing framework with fuzzy command search:

### Available Test Commands

```bash
# System Management
init [N] [W] [R]          # Initialize system with N nodes, W write quorum, R read quorum
add-node [count]          # Add new storage nodes
add-client [count]        # Add new client actors
reset                     # Reset entire system
quit                      # Exit the system

# Basic Operations
write <key> <value> [node_id]   # Write key-value pair
read <key> [node_id]            # Read value for key
list-nodes                      # Show all active nodes
list-clients                    # Show all clients

# Testing Scenarios
test basic                # Test basic CRUD operations
test quorum              # Test quorum behavior
test concurrency         # Test concurrent operations
test consistency         # Test data consistency
test membership          # Test join/leave operations
test partition           # Test network partition scenarios
test recovery            # Test crash recovery
test edge-cases          # Test edge cases and error scenarios
test timeout             # Test client timeout scenarios

# Advanced Operations
crash <node_id>          # Crash a specific node
recover <node_id>        # Recover a crashed node
join <node_id>           # Join a new node to network
leave <node_id>          # Remove node from network
benchmark <operations>   # Run performance benchmark
stress <duration>        # Run stress test

# Debugging
debug <node_id>          # Print node's data store
debug-all                # Print all nodes' data stores
log-level <level>        # Set logging level (DEBUG, INFO, WARN, ERROR)
```

### Fuzzy Command Search

The interactive framework includes `fzf` integration for enhanced command discovery:

```bash
# Type 'fzf' or 'help' to open fuzzy command search
fzf

# Search commands by typing partial names or descriptions
# Example: typing "test" shows all testing commands
# Example: typing "crash" shows crash-related commands
```

## 📊 System Parameters

### Default Configuration

```java
// DataStoreManager defaults
N = 3  // Number of replicas
W = 2  // Write quorum size  
R = 2  // Read quorum size

// Timeouts (configurable via TimeoutDelay)
CLIENT_OPERATION_TIMEOUT = 3000ms
NODE_OPERATION_TIMEOUT = 2000ms
MEMBERSHIP_TIMEOUT = 1500ms
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
   debug-all
   
   # Verify network connectivity
   list-nodes
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
- **Distributed Systems Concepts**: Consistency, Availability, Partition tolerance (CAP theorem)
- **Dynamo Paper**: Amazon's Dynamo architecture principles

## 🤝 Contributing

This is an academic project for distributed systems coursework. The implementation demonstrates:

- Actor model programming with Akka
- Distributed consensus and consistency protocols  
- Fault tolerance and recovery mechanisms
- Performance testing and evaluation methodologies

## 📄 License

Academic project - do whatever you please
