package it.unitn.ds1.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.Messages;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.OperationDelays;

public class LargeScaleTest {

    private static final Logger logger = LoggerFactory.getLogger(LargeScaleTest.class);

    private static ActorSystem system;
    private static TreeMap<Integer, ActorRef> nodeRegistry;
    private static List<ActorRef> allNodes;
    private static List<ActorRef> clients;
    private static List<Integer> crashedNodeIds = new ArrayList<>();

    // Configuration
    private static final int INITIAL_NODES = 10;
    private static final int JOINING_NODES = 20;
    private static final int TOTAL_CLIENTS = 5;
    private static final int OPERATIONS_PER_PHASE = 50;

    // Helper method to sleep for operation-specific delay
    private static void sleepForOperation(OperationType operationType) {
        try {
            Thread.sleep(OperationDelays.getDelayForOperation(operationType));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during {} delay", operationType);
        }
    }

    // Helper method to print node contents
    private static void printNodeContents() {
        logger.info("=== NODE REGISTRY STATUS ({} nodes) ===", nodeRegistry.size());
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

    // Initialize the system with initial nodes
    private static void initializeSystem() {
        logger.info("Starting Large Scale Distributed Storage System Test");
        logger.info("Initial nodes: {}, Joining nodes: {}, Total clients: {}",
                INITIAL_NODES, JOINING_NODES, TOTAL_CLIENTS);

        system = ActorSystem.create("Large-Scale-Storage-System");
        nodeRegistry = new TreeMap<>();
        allNodes = new ArrayList<>();
        clients = new ArrayList<>();

        // Create all nodes (initial + joining)
        logger.info("Creating {} total nodes...", INITIAL_NODES + JOINING_NODES);
        for (int i = 1; i <= INITIAL_NODES + JOINING_NODES; i++) {
            int nodeId = i * 10; // Node IDs: 10, 20, 30, 40, ...
            ActorRef node = system.actorOf(StorageNode.props(nodeId), "node-" + nodeId);
            allNodes.add(node);

            // Add first 10 nodes to initial registry
            if (i <= INITIAL_NODES) {
                nodeRegistry.put(nodeId, node);
            }
        }

        // Initialize the first 10 nodes
        logger.info("Initializing {} initial nodes...", INITIAL_NODES);
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            entry.getValue().tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());
        }

        sleepForOperation(OperationType.CRASH);

        // Create clients
        logger.info("Creating {} client actors...", TOTAL_CLIENTS);
        for (int i = 1; i <= TOTAL_CLIENTS; i++) {
            ActorRef client = system.actorOf(Client.props(), "client-" + i);
            clients.add(client);
        }

        logger.info("System initialization completed with {} nodes and {} clients",
                nodeRegistry.size(), clients.size());
        logger.info("\n");
    }

    // Helper method to get a random active node (not crashed)
    private static ActorRef getRandomNode() {
        List<ActorRef> activeNodes = new ArrayList<>();

        // Only include non-crashed nodes
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (!crashedNodeIds.contains(entry.getKey())) {
                activeNodes.add(entry.getValue());
            }
        }

        if (activeNodes.isEmpty()) {
            logger.error("No active nodes available!");
            return null;
        }

        int randomIndex = (int) (Math.random() * activeNodes.size());
        return activeNodes.get(randomIndex);
    }

    // Helper method to get a random client
    private static ActorRef getRandomClient() {
        int randomIndex = (int) (Math.random() * clients.size());
        return clients.get(randomIndex);
    }

    // Helper method to get node ID from ActorRef
    private static Integer getNodeId(ActorRef node) {
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (entry.getValue().equals(node)) {
                return entry.getKey();
            }
        }
        return -1; // Unknown node
    }

    // Helper method to get a unique client for concurrent operations
    private static ActorRef getUniqueClient(int operationIndex) {
        // Create additional clients if needed for concurrency
        while (clients.size() <= operationIndex) {
            ActorRef newClient = system.actorOf(Client.props(), "client-" + (clients.size() + 1));
            clients.add(newClient);
            logger.debug("Created additional client: client-{}", clients.size());
        }
        return clients.get(operationIndex);
    }

    // Test initial operations with 10 nodes
    private static void testInitialOperations() {
        logger.info("========== TESTING INITIAL OPERATIONS ({} nodes) ==========", nodeRegistry.size());

        // Populate some initial data
        for (int i = 0; i < 15; i++) {
            ActorRef randomNode = getRandomNode();
            if (randomNode == null) {
                logger.warn("No active nodes available for initial operation {}", i);
                continue;
            }

            ActorRef uniqueClient = getUniqueClient(i); // Use unique client for each operation
            int key = 100 + i;
            String value = "InitialValue" + i;

            logger.info("Initial data: Node {} storing key {} = {} (client-{})",
                    getNodeId(randomNode), key, value, (i + 1));
            uniqueClient.tell(new Messages.InitiateUpdate(key, value, randomNode), ActorRef.noSender());

            // Small delay between operations
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents();
    }

    // Gradually add nodes to the network
    private static void addNodesGradually() {
        logger.info("========== ADDING {} NODES GRADUALLY ==========", JOINING_NODES);

        int currentNodeIndex = INITIAL_NODES;

        for (int wave = 1; wave <= 4; wave++) {
            int nodesInThisWave = Math.min(5, JOINING_NODES - (wave - 1) * 5);
            logger.info("=== Wave {}: Adding {} nodes ===", wave, nodesInThisWave);

            for (int i = 0; i < nodesInThisWave && currentNodeIndex < allNodes.size(); i++) {
                ActorRef newNode = allNodes.get(currentNodeIndex);
                int newNodeId = (currentNodeIndex + 1) * 10;

                // Choose a random existing node as bootstrap peer
                ActorRef bootstrapPeer = getRandomNode();

                logger.info("Node {} joining network via bootstrap peer {}",
                        newNodeId, getNodeId(bootstrapPeer));

                newNode.tell(new Messages.Join(bootstrapPeer), ActorRef.noSender());
                nodeRegistry.put(newNodeId, newNode);

                sleepForOperation(OperationType.JOIN);
                currentNodeIndex++;

                // Perform some operations after each join
                performRandomOperations(3, "post-join-" + newNodeId);
            }

            logger.info("Wave {} completed. Total active nodes: {}", wave, nodeRegistry.size());
            printNodeContents();
        }
    }

    // Perform random operations with random coordinators
    private static void performRandomOperations(int numOperations, String phase) {
        logger.info("=== Random Operations Phase: {} ({} operations) ===", phase, numOperations);

        for (int i = 0; i < numOperations; i++) {
            ActorRef randomNode = getRandomNode();
            if (randomNode == null) {
                logger.warn("No active nodes available for operation {}", i);
                continue;
            }

            ActorRef uniqueClient = getUniqueClient(i); // Use unique client for each operation

            // Mix of different key ranges to test distribution
            int keyRange = (int) (Math.random() * 3);
            int key = switch (keyRange) {
                case 0 -> 200 + (int) (Math.random() * 50); // Range 200-249
                case 1 -> 500 + (int) (Math.random() * 50); // Range 500-549
                case 2 -> 800 + (int) (Math.random() * 50); // Range 800-849
                default -> 200;
            };

            if (Math.random() > 0.3) { // 70% UPDATE, 30% GET
                String value = phase + "-Value" + i;
                logger.info("{}: Node {} UPDATE key {} = {} (client-{})",
                        phase, getNodeId(randomNode), key, value, (i + 1));
                uniqueClient.tell(new Messages.InitiateUpdate(key, value, randomNode), ActorRef.noSender());
            } else {
                logger.info("{}: Node {} GET key {} (client-{})",
                        phase, getNodeId(randomNode), key, (i + 1));
                uniqueClient.tell(new Messages.InitiateGet(key, randomNode), ActorRef.noSender());
            }

            // Small delay between operations to avoid overwhelming
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        sleepForOperation(OperationType.CLIENT_UPDATE);
    }

    // Test concurrent operations across many nodes
    private static void testMassiveConcurrency() {
        logger.info("========== TESTING MASSIVE CONCURRENCY ({} nodes) ==========", nodeRegistry.size());

        // Phase 1: Concurrent operations on same keys
        logger.info("=== Phase 1: Concurrent operations on same keys ===");
        int[] hotKeys = { 1000, 1001, 1002, 1003, 1004 };

        for (int i = 0; i < 25; i++) {
            ActorRef randomNode = getRandomNode();
            if (randomNode == null) {
                logger.warn("No active nodes available for concurrent operation {}", i);
                continue;
            }

            ActorRef uniqueClient = getUniqueClient(i); // Each concurrent operation gets unique client
            int hotKey = hotKeys[i % hotKeys.length];

            if (Math.random() > 0.4) {
                String value = "ConcurrentValue" + i;
                logger.info("Concurrent: Node {} UPDATE hot key {} = {} (client-{})",
                        getNodeId(randomNode), hotKey, value, (i + 1));
                uniqueClient.tell(new Messages.InitiateUpdate(hotKey, value, randomNode), ActorRef.noSender());
            } else {
                logger.info("Concurrent: Node {} GET hot key {} (client-{})",
                        getNodeId(randomNode), hotKey, (i + 1));
                uniqueClient.tell(new Messages.InitiateGet(hotKey, randomNode), ActorRef.noSender());
            }
        }

        sleepForOperation(OperationType.CLIENT_UPDATE);

        // Phase 2: Distributed operations across key space
        logger.info("=== Phase 2: Distributed operations across key space ===");
        performRandomOperations(OPERATIONS_PER_PHASE, "distributed-load");

        printNodeContents();
    }

    // Test system resilience with node failures
    private static void testSystemResilience() {
        logger.info("========== TESTING SYSTEM RESILIENCE ==========");

        // Crash a few random nodes
        List<ActorRef> crashedNodes = new ArrayList<>();

        int nodesToCrash = Math.min(5, nodeRegistry.size() / 4);
        logger.info("=== Crashing {} random nodes ===", nodesToCrash);

        for (int i = 0; i < nodesToCrash; i++) {
            ActorRef randomNode = getRandomNode();
            if (randomNode == null) {
                logger.warn("No active nodes available to crash");
                break;
            }

            Integer nodeId = getNodeId(randomNode);

            if (!crashedNodeIds.contains(nodeId)) {
                logger.info("Crashing node {}", nodeId);
                randomNode.tell(new Messages.Crash(), ActorRef.noSender());
                crashedNodes.add(randomNode);
                crashedNodeIds.add(nodeId); // Track crashed node
                sleepForOperation(OperationType.CRASH);
            }
        }

        // Continue operations with reduced nodes
        logger.info("=== Operations with {} nodes crashed ===", crashedNodes.size());
        performRandomOperations(20, "resilience-test");

        // Recover some nodes
        logger.info("=== Recovering crashed nodes ===");
        ActorRef recoveryPeer = getRandomNode();

        if (recoveryPeer != null) {
            for (int i = 0; i < Math.min(3, crashedNodes.size()); i++) {
                ActorRef crashedNode = crashedNodes.get(i);
                Integer nodeId = getNodeId(crashedNode);

                logger.info("Recovering node {} via peer {}", nodeId, getNodeId(recoveryPeer));
                crashedNode.tell(new Messages.Recovery(recoveryPeer), ActorRef.noSender());
                crashedNodeIds.remove(nodeId); // Remove from crashed list
                sleepForOperation(OperationType.RECOVERY);
            }
        }

        // Final operations after recovery
        performRandomOperations(15, "post-recovery");
        printNodeContents();
    }

    // Test with node departures
    private static void testNodeDepartures() {
        logger.info("========== TESTING NODE DEPARTURES ==========");

        int nodesToRemove = Math.min(3, nodeRegistry.size() / 5);
        logger.info("=== Removing {} nodes gracefully ===", nodesToRemove);

        List<Integer> removedNodeIds = new ArrayList<>();

        for (int i = 0; i < nodesToRemove; i++) {
            if (nodeRegistry.size() <= 3)
                break; // Keep minimum nodes

            ActorRef randomNode = getRandomNode();
            Integer nodeId = getNodeId(randomNode);

            logger.info("Node {} leaving the network", nodeId);
            randomNode.tell(new Messages.Leave(), ActorRef.noSender());
            sleepForOperation(OperationType.LEAVE);

            nodeRegistry.remove(nodeId);
            removedNodeIds.add(nodeId);
        }

        // Operations after departures
        performRandomOperations(20, "post-departure");
        printNodeContents();

        logger.info("Final network size: {} nodes", nodeRegistry.size());
    }

    public static void main(String[] args) {
        try {
            initializeSystem();

            logger.info("Starting large-scale test suite...");

            // Test with initial 10 nodes
            testInitialOperations();

            // Gradually add 20 more nodes
            addNodesGradually();

            // Test massive concurrency with all nodes
            testMassiveConcurrency();

            // Test system resilience
            testSystemResilience();

            // Test node departures
            testNodeDepartures();

            logger.info("Large-scale test completed successfully!");
            logger.info("Final statistics: {} active nodes, {} total operations performed",
                    nodeRegistry.size(), OPERATIONS_PER_PHASE * 4 + 100);

        } catch (Exception e) {
            logger.error("Large-scale test execution failed", e);
        } finally {
            if (system != null) {
                logger.info("Shutting down system...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                system.terminate();
            }
        }
    }
}