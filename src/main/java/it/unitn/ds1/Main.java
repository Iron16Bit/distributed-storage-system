package it.unitn.ds1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.OperationDelays;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static ActorSystem system;
    private static ActorRef client1, client2, client3;
    private static TreeMap<Integer, ActorRef> nodeRegistry;
    private static ActorRef node1, node5, node7, node10, nodeSystem;

    private static DataStoreManager dataStoreManager = DataStoreManager.getInstance();

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

        logger.info("DataStore parameters: N-{}, W-{}, R-{}", dataStoreManager.N, dataStoreManager.W, dataStoreManager.R);

        system = ActorSystem.create("Distributed-Storage-System");

        node1 = system.actorOf(StorageNode.props(1), "node-1");
        node5 = system.actorOf(StorageNode.props(5), "node-5");
        node10 = system.actorOf(StorageNode.props(10), "node-10");
        node7 = system.actorOf(StorageNode.props(7), "node-7");
        nodeSystem = system.actorOf(StorageNode.props(-1), "Storage-Manager");

        nodeRegistry = new TreeMap<>();
        nodeRegistry.put(1, node1);
        nodeRegistry.put(5, node5);
        nodeRegistry.put(10, node10);

        logger.info("Initializing node registries...");
        node1.tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());
        node5.tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());
        node10.tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());

        sleepForOperation(OperationType.INIT);

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
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 2: Store key=6, value=Gold ===");
        node5.tell(new Messages.ClientUpdate(6, "Gold"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 3: Get key=1 ===");
        node10.tell(new Messages.ClientGet(1), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 4: Get key=6 ===");
        node1.tell(new Messages.ClientGet(6), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 5: Get key=200 (non-existent) ===");
        node5.tell(new Messages.ClientGet(200), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 5.5: Update key=-1 (non-existent) ===");
        node5.tell(new Messages.ClientGet(-1), client1);
        sleepForOperation(OperationType.CLIENT_GET);


        logger.info("=== Operation 5.75: Update key=-782 (non-existent) ===");
        node1.tell(new Messages.ClientUpdate(-782, "ERROR"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);


        printNodeContents(nodeRegistry);
    }

    // Test multiple clients accessing same/different coordinators
    private static void testMultiClientOperations() {
        logger.info("========== TESTING MULTI-CLIENT OPERATIONS ==========");

        logger.info("=== Operation 6: Multiple clients storing different keys ===");
        node1.tell(new Messages.ClientUpdate(2, "Silver"), client1);
        node5.tell(new Messages.ClientUpdate(7, "Copper"), client2);
        node10.tell(new Messages.ClientUpdate(3, "Iron"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 7: Multiple clients reading same key ===");
        node1.tell(new Messages.ClientGet(1), client1);
        node5.tell(new Messages.ClientGet(1), client2);
        node10.tell(new Messages.ClientGet(1), client3);
        sleepForOperation(OperationType.CLIENT_GET);

        logger.info("=== Operation 8: Concurrent updates to same key (deadlock -> timeout) ===");
        node1.tell(new Messages.ClientUpdate(1, "Aluminum-Updated-1"), client1);
        node5.tell(new Messages.ClientUpdate(1, "Aluminum-Updated-2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);
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
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 12: Recover crashed node ===");
        node1.tell(new Messages.Recovery(node5), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(nodeRegistry);
    }

    // Test join/leave operations with system under load
    private static void testMembershipUnderLoad() {
        logger.info("========== TESTING MEMBERSHIP UNDER LOAD ==========");

        logger.info("=== Operation 13: Add new node while system is active ===");
        nodeRegistry.put(7, node7);
        node7.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 14: Operations with new topology ===");
        node7.tell(new Messages.ClientUpdate(8, "Platinum"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);

        node7.tell(new Messages.ClientGet(1), client2);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 15: Node leave operation ===");
        node10.tell(new Messages.Leave(), nodeSystem);
        sleepForOperation(OperationType.LEAVE);
        nodeRegistry.remove(10);
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 15.5: Node Join operation ===");
        node10.tell(new Messages.Join(node1), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        nodeRegistry.put(10, node10);
        printNodeContents(nodeRegistry);


        // logger.info("=== Operation 15.75: Node 10 Re-Join operation ===");
        // ActorRef node10_5 = system.actorOf(StorageNode.props(10), "node-10-quello-falso");
        // node10_5.tell(new Messages.Join(node10), nodeSystem);
        // sleepForOperation(OperationType.JOIN);
        // nodeRegistry.put(11, node10_5);
        // printNodeContents(nodeRegistry);

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
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 18: Sequential updates to test versioning ===");
        node10.tell(new Messages.ClientUpdate(26, "V1"), client1);
        sleepForOperation(OperationType.CLIENT_UPDATE);

        node7.tell(new Messages.ClientUpdate(26, "V2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);

        node5.tell(new Messages.ClientUpdate(26, "V3"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);
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
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 21: Test double join attempt ===");
        node1.tell(new Messages.Join(node5), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        nodeRegistry.put(1, node1);
        printNodeContents(nodeRegistry);
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
        printNodeContents(nodeRegistry);

        logger.info("=== Operation 23: Test system recovery ===");
        node5.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);

        node10.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);

        node7.tell(new Messages.Recovery(node1), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);

        printNodeContents(nodeRegistry);

        logger.info("=== Operation 24: Test data consistency after recovery ===");
        node5.tell(new Messages.ClientGet(25), client1);
        node10.tell(new Messages.ClientGet(26), client2);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(nodeRegistry);
    }

    private static void testRandomNodeSelection() {
        logger.info("========== TESTING RANDOM NODE SELECTION ==========");

        // Operation 25: Random GET operation
        logger.info("=== Operation 25: Random GET operation ===");
        ActorRef randomNode1 = getRandomNode();
        logger.info("Selected Node ID: {} for GET operation", getNodeId(randomNode1));
        randomNode1.tell(new Messages.ClientGet(1), client1);
        sleepForOperation(OperationType.CLIENT_GET);

        // Operation 26: Random UPDATE operation
        logger.info("=== Operation 26: Random UPDATE operation ===");
        ActorRef randomNode2 = getRandomNode();
        logger.info("Selected Node ID: {} for UPDATE operation", getNodeId(randomNode2));
        randomNode2.tell(new Messages.ClientUpdate(30, "RandomValue1"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);

        // Operation 27: Multiple random operations on same key
        logger.info("=== Operation 27: Multiple random coordinators for same key ===");
        ActorRef randomNode3 = getRandomNode();
        ActorRef randomNode4 = getRandomNode();
        logger.info("Node {} and Node {} performing operations on key 31",
                getNodeId(randomNode3), getNodeId(randomNode4));

        randomNode3.tell(new Messages.ClientUpdate(31, "ConcurrentValue1"), client1);
        randomNode4.tell(new Messages.ClientUpdate(31, "SeMiScriviMiAmmazzo"), client3);
        sleepForOperation(OperationType.CLIENT_UPDATE);

        ActorRef[] clients = { client1, client2, client3 };
        // Operation 28: Random UPDATEoperations on the same key
        logger.info("=== Operation 28: Random UPDATE operations on the same key ===");
        for (int i = 0; i < 3; i++) {
            ActorRef randomNode = getRandomNode();
            int key = 32;
            String randomValue = "RandomValue" + (i + 2);

            logger.info("Random Node {} updating key {} with value {}",
                    getNodeId(randomNode), key, randomValue);
            randomNode.tell(new Messages.ClientUpdate(key, randomValue),
                    clients[i]);
        }
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);

        // Operation 29: Random GET operations
        logger.info("=== Operation 29: Random GET operations on the same key ===");
        for (int i = 0; i < 3; i++) {
            ActorRef randomNode = getRandomNode();
            int key = 32;

            logger.info("Random Node {} reading key {}", getNodeId(randomNode), key);
            randomNode.tell(new Messages.ClientGet(key), clients[i]);
        }
        sleepForOperation(OperationType.CLIENT_GET);

        // Operation 30: Random stress test
        logger.info("=== Operation 30: Random stress test on the same key ===");
        for (int i = 0; i < 3; i++) {
            ActorRef randomNode = getRandomNode();
            int key = 35;
            String randomValue = "StressValue" + i;

            if (Math.random() > 0.5) {
                logger.info("Stress test: Node {} UPDATE key {} = {}",
                        getNodeId(randomNode), key, randomValue);
                randomNode.tell(new Messages.ClientUpdate(key, randomValue), clients[i]);
            } else {
                logger.info("Stress test: Node {} GET key {}",
                        getNodeId(randomNode), key);
                randomNode.tell(new Messages.ClientGet(key), clients[i]);
            }
        }
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(nodeRegistry);
    }

    // Helper method to get a random node from the registry
    private static ActorRef getRandomNode() {
        List<ActorRef> nodes = new ArrayList<>(nodeRegistry.values());
        int randomIndex = (int) (Math.random() * nodes.size());
        return nodes.get(randomIndex);
    }

    // Helper method to get node ID from ActorRef (for logging purposes)
    private static Integer getNodeId(ActorRef node) {
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (entry.getValue().equals(node)) {
                return entry.getKey();
            }
        }
        return -1; // Unknown node
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
            testRandomNodeSelection();

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