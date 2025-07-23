package it.unitn.ds1;

import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;
import it.unitn.ds1.types.UpdateType;
import it.unitn.ds1.utils.OperationDelays;
import it.unitn.ds1.utils.OperationDelays.OperationType;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static ActorSystem system;
    private static ActorRef client1, client2, client3;
    private static TreeMap<Integer, ActorRef> testNodeRegistry;
    private static ActorRef node1, node5, node7, node10, nodeSystem;

    // Helper method to print node contents with appropriate delay
    private static void printNodeContents(TreeMap<Integer, ActorRef> nodeRegistry) {
        logger.info("=== NODE CONTENTS ===");
        for (Integer nodeId : nodeRegistry.keySet()) {
            ActorRef node = nodeRegistry.get(nodeId);
            node.tell(new Messages.DebugPrintDataStore(), ActorRef.noSender());
        }
        try { 
            Thread.sleep(OperationDelays.getDelayForOperation(OperationType.DEBUG_PRINT)); 
        } catch (InterruptedException e) { 
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during debug print delay", e);
        }
        logger.info("=====================");
        logger.info("\n");
    }
    
    // Helper method to sleep for operation-specific delay
    private static void sleepForOperation(OperationType operationType) {
        try {
            Thread.sleep(OperationDelays.getDelayForOperation(operationType));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during {} delay", operationType);
        }
    }

    // Initialize the system and actors
    private static void initializeSystem() {
        logger.info("Starting Distributed Storage System");
        
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

        logger.info("Initializing node registries...");
        node1.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node5.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node10.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());

        sleepForOperation(OperationType.CRASH);

        logger.info("Creating client actors...");
        client1 = system.actorOf(Client.props(), "client-1");
        client2 = system.actorOf(Client.props(), "client-2");
        client3 = system.actorOf(Client.props(), "client-3");
        
        logger.info("System initialization completed");
        logger.info("\n");
    }

    // Test basic CRUD operations
    private static void testBasicOperations() {
        logger.info("========== TESTING BASIC CRUD OPERATIONS ==========");
        
        logger.info("=== Operation 1: Store key=1, value=Aluminum ===");
        node1.tell(new Messages.ClientUpdate(1, "Aluminum"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 2: Store key=6, value=Gold ===");
        node5.tell(new Messages.ClientUpdate(6, "Gold"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 3: Get key=1 ===");
        node10.tell(new Messages.ClientGet(1), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 4: Get key=6 ===");
        node1.tell(new Messages.ClientGet(6), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 5: Get key=200 (non-existent) ===");
        node5.tell(new Messages.ClientGet(200), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        printNodeContents(testNodeRegistry);
    }

    // Test multiple clients accessing same/different coordinators
    private static void testMultiClientOperations() {
        logger.info("========== TESTING MULTI-CLIENT OPERATIONS ==========");
        
        logger.info("=== Operation 6: Multiple clients storing different keys ===");
        node1.tell(new Messages.ClientUpdate(2, "Silver"), client1);
        node5.tell(new Messages.ClientUpdate(7, "Copper"), client2);
        node10.tell(new Messages.ClientUpdate(3, "Iron"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 7: Multiple clients reading same key ===");
        node1.tell(new Messages.ClientGet(1), client1);
        node5.tell(new Messages.ClientGet(1), client2);
        node10.tell(new Messages.ClientGet(1), client3);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 8: Concurrent updates to same key (deadlock -> timeout) ===");
        node1.tell(new Messages.ClientUpdate(1, "Aluminum-Updated-1"), client1);
        node5.tell(new Messages.ClientUpdate(1, "Aluminum-Updated-2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);
    }

    // Test operations with crashed nodes to verify quorum behavior
    private static void testQuorumWithCrashedNodes() {
        logger.info("========== TESTING QUORUM WITH CRASHED NODES ==========");
        
        logger.info("=== Operation 9: Crash one node and test operations ===");
        node1.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        logger.info("=== Operation 10: Read with one node down ===");
        node5.tell(new Messages.ClientGet(6), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        logger.info("=== Operation 11: Write with one node down ===");
        node10.tell(new Messages.ClientUpdate(6, "Gold-Updated"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 12: Recover crashed node ===");
        node1.tell(new Messages.Recovery(node5), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(testNodeRegistry);
    }

    // Test join/leave operations with system under load
    private static void testMembershipUnderLoad() {
        logger.info("========== TESTING MEMBERSHIP UNDER LOAD ==========");
        
        logger.info("=== Operation 13: Add new node while system is active ===");
        testNodeRegistry.put(7, node7);
        node7.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 14: Operations with new topology ===");
        node7.tell(new Messages.ClientUpdate(8, "Platinum"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node7.tell(new Messages.ClientGet(1), client2);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 15: Node leave operation ===");
        node10.tell(new Messages.Leave(), nodeSystem);
        sleepForOperation(OperationType.LEAVE);
        testNodeRegistry.remove(10);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 15.5: Node Join operation ===");
        node10.tell(new Messages.Join(node1), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        testNodeRegistry.put(10,node10);
        printNodeContents(testNodeRegistry);
    }

    // Test concurrent operations and race conditions
    private static void testConcurrencyAndRaceConditions() {
        logger.info("========== TESTING CONCURRENCY AND RACE CONDITIONS ==========");
        
        logger.info("=== Operation 16: Concurrent read/write operations ===");
        node1.tell(new Messages.ClientUpdate(25, "Titanium"), client1);
        node5.tell(new Messages.ClientGet(25), client2);
        node7.tell(new Messages.ClientUpdate(25, "Titanium-V2"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);

        logger.info("=== Operation 17: Read after concurrent writes ===");
        node10.tell(new Messages.ClientGet(25), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 18: Sequential updates to test versioning ===");
        node10.tell(new Messages.ClientUpdate(26, "V1"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node7.tell(new Messages.ClientUpdate(26, "V2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node5.tell(new Messages.ClientUpdate(26, "V3"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);
    }

    // Test error scenarios and edge cases
    private static void testErrorScenarios() {
        logger.info("========== TESTING ERROR SCENARIOS ==========");
        
        logger.info("=== Operation 19: Operations on crashed coordinator ===");
        node7.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        node7.tell(new Messages.ClientGet(25), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        logger.info("=== Operation 20: Test timeout scenarios ===");
        printNodeContents(testNodeRegistry);
        
        logger.info("=== Operation 21: Test double join attempt ===");
        node1.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        testNodeRegistry.put(1, node1);
        printNodeContents(testNodeRegistry);
    }

    // Test system resilience and recovery scenarios
    private static void testSystemResilience() {
        logger.info("========== TESTING SYSTEM RESILIENCE ==========");
        
        logger.info("=== Operation 22: Test cascading node failures ===");
        node5.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        node10.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        
        node1.tell(new Messages.ClientGet(26), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 23: Test system recovery ===");
        node5.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        node10.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        node7.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        
        printNodeContents(testNodeRegistry);

        logger.info("=== Operation 24: Test data consistency after recovery ===");
        node5.tell(new Messages.ClientGet(25), client1);
        node10.tell(new Messages.ClientGet(26), client2);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);
    }

    public static void main(String[] args) {
        try {
            initializeSystem();
            
            logger.info("Starting comprehensive test suite...");
            testBasicOperations();
            testMultiClientOperations();
            testQuorumWithCrashedNodes();
            testMembershipUnderLoad();
            testConcurrencyAndRaceConditions();
            testErrorScenarios();
            testSystemResilience();
            
            logger.info("All tests completed successfully");
            
        } catch (Exception e) {
            logger.error("Test execution failed", e);
        } finally {
            if (system != null) {
                logger.info("Shutting down system...");
                system.terminate();
            }
        }
    }
}