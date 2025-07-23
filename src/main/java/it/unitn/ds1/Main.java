package it.unitn.ds1;

import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;
import it.unitn.ds1.types.UpdateType;
import it.unitn.ds1.utils.OperationDelays;
import it.unitn.ds1.utils.OperationDelays.OperationType;

public class Main {

    private static ActorSystem system;
    private static ActorRef client1, client2, client3;
    private static TreeMap<Integer, ActorRef> testNodeRegistry;
    private static ActorRef node1, node5, node7, node10, nodeSystem;

    // Helper method to print node contents with appropriate delay
    private static void printNodeContents(TreeMap<Integer, ActorRef> nodeRegistry) {
        System.out.println("=== NODE CONTENTS ===");
        for (Integer nodeId : nodeRegistry.keySet()) {
            ActorRef node = nodeRegistry.get(nodeId);
            node.tell(new Messages.DebugPrintDataStore(), ActorRef.noSender());
        }
        try { 
            Thread.sleep(OperationDelays.getDelayForOperation(OperationType.DEBUG_PRINT)); 
        } catch (InterruptedException e) { 
            Thread.currentThread().interrupt();
        }
        System.out.println("=====================");
    }
    
    // Helper method to sleep for operation-specific delay
    private static void sleepForOperation(OperationType operationType) {
        try {
            Thread.sleep(OperationDelays.getDelayForOperation(operationType));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Thread interrupted during " + operationType + " delay");
        }
    }

    // Initialize the system and actors
    private static void initializeSystem() {
        System.out.println("Starting Distributed Storage System");
        
        system = ActorSystem.create("Distributed-Storage-System");
        
        node1 = system.actorOf(StorageNode.props(1), "node-1");
        node5 = system.actorOf(StorageNode.props(5), "node-5");
        node10 = system.actorOf(StorageNode.props(10), "node-10");
        node7 = system.actorOf(StorageNode.props(7), "node-7");
        nodeSystem = system.actorOf(StorageNode.props(-1), "Storage-Manager");

        testNodeRegistry = new TreeMap<>();
        testNodeRegistry.put(1, node1);
        testNodeRegistry.put(5, node5);
        testNodeRegistry.put(10, node10);

        // Initialize node registries
        node1.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node5.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node10.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());

        sleepForOperation(OperationType.CRASH);

        // Create multiple clients
        client1 = system.actorOf(Client.props(), "client-1");
        client2 = system.actorOf(Client.props(), "client-2");
        client3 = system.actorOf(Client.props(), "client-3");
    }

    // Test basic CRUD operations
    private static void testBasicOperations() {
        System.out.println("\n========== TESTING BASIC CRUD OPERATIONS ==========");
        
        // Store values across different keys
        System.out.println("=== Operation 1: Store key=1, value=Aluminum ===");
        node1.tell(new Messages.ClientUpdate(1, "Aluminum"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n=== Operation 2: Store key=6, value=Gold ===");
        node5.tell(new Messages.ClientUpdate(6, "Gold"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Read the stored values
        System.out.println("\n=== Operation 3: Get key=1 ===");
        node10.tell(new Messages.ClientGet(1), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        System.out.println("\n=== Operation 4: Get key=6 ===");
        node1.tell(new Messages.ClientGet(6), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        // Try to get a non-existent key
        System.out.println("\n=== Operation 5: Get key=200 (non-existent) ===");
        node5.tell(new Messages.ClientGet(200), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        printNodeContents(testNodeRegistry);
    }

    // Test multiple clients accessing same/different coordinators
    private static void testMultiClientOperations() {
        System.out.println("\n========== TESTING MULTI-CLIENT OPERATIONS ==========");
        
        // Multiple clients accessing same coordinator
        System.out.println("=== Operation 6: Multiple clients via same coordinator (Node 1) ===");
        node1.tell(new Messages.ClientUpdate(2, "Silver"), client1);
        node1.tell(new Messages.ClientUpdate(3, "Bronze"), client2);
        node1.tell(new Messages.ClientGet(1), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Multiple clients accessing different coordinators for same key
        System.out.println("\n=== Operation 7: Multiple clients via different coordinators (same key=1) ===");
        node1.tell(new Messages.ClientGet(1), client1);
        node5.tell(new Messages.ClientGet(1), client2);
        node10.tell(new Messages.ClientUpdate(1, "Platinum"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Multiple clients accessing different coordinators for different keys
        System.out.println("\n=== Operation 8: Multiple clients via different coordinators (different keys) ===");
        node1.tell(new Messages.ClientUpdate(4, "Copper"), client1);
        node5.tell(new Messages.ClientUpdate(8, "Iron"), client2);
        node10.tell(new Messages.ClientUpdate(12, "Zinc"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);
    }

    // Test operations with crashed nodes to verify quorum behavior
    private static void testQuorumWithCrashedNodes() {
        System.out.println("\n========== TESTING QUORUM WITH CRASHED NODES ==========");
        
        // First, ensure we have data distributed
        System.out.println("=== Setup: Store data across the system ===");
        node1.tell(new Messages.ClientUpdate(15, "TestData1"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        // Crash one node and test read quorum
        System.out.println("\n=== Operation 9: Crash Node 10 and test read quorum ===");
        node10.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        // Try to read - should still work if quorum is available
        node1.tell(new Messages.ClientGet(15), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        // Try to write - should still work if write quorum is available
        System.out.println("\n=== Operation 10: Update with one node crashed ===");
        node5.tell(new Messages.ClientUpdate(15, "TestData1Updated"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Crash another node - test if system still works
        System.out.println("\n=== Operation 11: Crash Node 5, test with 2 nodes down ===");
        node5.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        // This might fail depending on quorum requirements
        node1.tell(new Messages.ClientGet(15), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        // Recover one node
        System.out.println("\n=== Operation 12: Recover Node 10 ===");
        node10.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(testNodeRegistry);
    }

    // Test join/leave operations with system under load
    private static void testMembershipUnderLoad() {
        System.out.println("\n========== TESTING MEMBERSHIP UNDER LOAD ==========");
        
        // Add Node 7 while system is handling operations
        System.out.println("=== Operation 13: Join Node 7 while handling client requests ===");
        node7.tell(new Messages.Join(node1), nodeSystem);
        
        // Immediately start client operations
        node1.tell(new Messages.ClientUpdate(20, "ConcurrentData1"), client1);
        node1.tell(new Messages.ClientUpdate(21, "ConcurrentData2"), client2);
        
        sleepForOperation(OperationType.JOIN);
        testNodeRegistry.put(7, node7);
        printNodeContents(testNodeRegistry);

        // Recover Node 5 while handling operations
        System.out.println("\n=== Operation 14: Recover Node 5 while handling requests ===");
        node5.tell(new Messages.Recovery(node7), nodeSystem);
        
        // Concurrent operations during recovery
        node7.tell(new Messages.ClientGet(20), client1);
        node1.tell(new Messages.ClientUpdate(22, "RecoveryData"), client3);
        
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(testNodeRegistry);

        // Test leave operation under load
        System.out.println("\n=== Operation 15: Node 1 leaves while handling requests ===");
        node1.tell(new Messages.Leave(), nodeSystem);
        
        // Operations during leave
        node5.tell(new Messages.ClientGet(21), client1);
        node7.tell(new Messages.ClientGet(22), client2);
        
        sleepForOperation(OperationType.LEAVE);
        testNodeRegistry.remove(1);
        printNodeContents(testNodeRegistry);
    }

    // Test concurrent operations and race conditions
    private static void testConcurrencyAndRaceConditions() {
        System.out.println("\n========== TESTING CONCURRENCY AND RACE CONDITIONS ==========");
        
        // Concurrent updates to same key from different coordinators
        System.out.println("=== Operation 16: Concurrent updates to same key from different coordinators ===");
        node5.tell(new Messages.ClientUpdate(25, "Version1"), client1);
        node7.tell(new Messages.ClientUpdate(25, "Version2"), client2);
        node10.tell(new Messages.ClientUpdate(25, "Version3"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Concurrent read-write operations
        System.out.println("\n=== Operation 17: Concurrent read-write to same key ===");
        node5.tell(new Messages.ClientGet(25), client1);
        node7.tell(new Messages.ClientUpdate(25, "ReadWriteTest"), client2);
        node10.tell(new Messages.ClientGet(25), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        // Sequential updates to test versioning
        System.out.println("\n=== Operation 18: Sequential updates to test versioning ===");
        node5.tell(new Messages.ClientUpdate(26, "V1"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node7.tell(new Messages.ClientUpdate(26, "V2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node10.tell(new Messages.ClientUpdate(26, "V3"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);
    }

    // Test error scenarios and edge cases
    private static void testErrorScenarios() {
        System.out.println("\n========== TESTING ERROR SCENARIOS ==========");
        
        // Test operations on crashed coordinator
        System.out.println("=== Operation 19: Operations on crashed coordinator ===");
        node7.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        // Try to use crashed node as coordinator
        node7.tell(new Messages.ClientGet(25), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        // Test timeout scenarios (if implemented)
        System.out.println("\n=== Operation 20: Test timeout scenarios ===");
        // Operations that might timeout due to insufficient nodes
        printNodeContents(testNodeRegistry);
        
        // Test double join attempt
        System.out.println("\n=== Operation 21: Test double join attempt ===");
        node1.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        // Try to join again
        node1.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        testNodeRegistry.put(1, node1);
        printNodeContents(testNodeRegistry);
    }

    // Test system resilience and recovery scenarios
    private static void testSystemResilience() {
        System.out.println("\n========== TESTING SYSTEM RESILIENCE ==========");
        
        // Test cascading failures
        System.out.println("=== Operation 22: Test cascading node failures ===");
        node5.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        node10.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        // Try operations with minimal nodes
        node1.tell(new Messages.ClientGet(26), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        // Test system recovery
        System.out.println("\n=== Operation 23: Test system recovery ===");
        node5.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        node10.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        node7.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        printNodeContents(testNodeRegistry);

        // Test data consistency after recovery
        System.out.println("\n=== Operation 24: Test data consistency after recovery ===");
        node5.tell(new Messages.ClientGet(25), client1);
        node10.tell(new Messages.ClientGet(26), client2);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);
    }

    public static void main(String[] args) {
        try {
            initializeSystem();
            
            // Run test suites in logical order
            testBasicOperations();
            testMultiClientOperations();
            testQuorumWithCrashedNodes();
            testMembershipUnderLoad();
            testConcurrencyAndRaceConditions();
            testErrorScenarios();
            testSystemResilience();
            
            System.out.println("\n=== All tests completed ===");
            
        } catch (Exception e) {
            System.err.println("Test execution failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean shutdown
            if (system != null) {
                System.out.println("Shutting down system...");
                system.terminate();
            }
        }
    }
}