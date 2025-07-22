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

    // Helper method to print node contents with appropriate delay
    private static void printNodeContents(TreeMap<Integer, ActorRef> nodeRegistry) {
        System.out.println("=== NODE CONTENTS ===");
        for (Integer nodeId : nodeRegistry.keySet()) {
            ActorRef node = nodeRegistry.get(nodeId);
            // Send a debug message to get node contents
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
    
    public static void main(String[] args) {
        System.out.printf("Starting Distributed Storage System\n");

        final ActorSystem system = ActorSystem.create("Distributed-Storage-System");
        
        final ActorRef node1 = system.actorOf(
            StorageNode.props(1),
            "node-1"
        );

        final ActorRef node5 = system.actorOf(
            StorageNode.props(5),
            "node-5"
        );

        final ActorRef node10 = system.actorOf(
            StorageNode.props(10),
            "node-10"
        );

        final ActorRef node7 = system.actorOf(
            StorageNode.props(7),
            "node-7"
        );

        final ActorRef nodeSystem = system.actorOf(
            StorageNode.props(-1),
            "Storage-Manager"
        );

        // For Testing we are going to make them know of each other
        TreeMap<Integer, ActorRef> testNodeRegistry = new TreeMap<>();
        testNodeRegistry.put(1, node1);
        testNodeRegistry.put(5, node5);
        testNodeRegistry.put(10, node10);

        // Send the registry to both nodes
        node1.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node5.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());
        node10.tell(new Messages.UpdateNodeRegistry(testNodeRegistry, UpdateType.INIT), ActorRef.noSender());

        // Small delay to ensure registry updates are processed
        sleepForOperation(OperationType.CRASH);

        final ActorRef client = system.actorOf(
            Client.props(),
            "client-1"
        );

        // Store a value (client sends UpdateValueMsg to node)
        System.out.println("=== Operation 1: Store key=1, value=Alluminium ===");
        node1.tell(new Messages.ClientUpdate(1, "Alluminium"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");
        
        // Get a value (client sends GetValueMsg to node)
        System.out.println("=== Operation 2: Get key=1 ===");
        node1.tell(new Messages.ClientGet(1), client);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");
        
        // Store another value
        System.out.println("=== Operation 3: Store key=6, value=Gold ===");
        node1.tell(new Messages.ClientUpdate(6, "Gold"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");
        
        // Get the second value
        System.out.println("=== Operation 4: Get key=6 ===");
        node1.tell(new Messages.ClientGet(6), client);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");
        
        // Try to get a non-existent key
        System.out.println("=== Operation 5: Get key=200 (non-existent) ===");
        node1.tell(new Messages.ClientGet(200), client);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Try to update a local value
        System.out.println("=== Operation 6: Update key=1 value=Silver ===");
        node1.tell(new Messages.ClientUpdate(1, "Silver"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        System.out.println("=== Operation 6.5: Update key=1 value=LOL ===");
        node1.tell(new Messages.ClientUpdate(1, "LOL"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Try to update a remote value
        System.out.println("=== Operation 7: Update key=6 value=Sapphire ===");
        node1.tell(new Messages.ClientUpdate(6, "Sapphire"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Try to update a remote value
        System.out.println("=== Operation 8: Update key=9 value=Diamond ===");
        node1.tell(new Messages.ClientUpdate(9, "Diamond"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Try to update a remote value
        System.out.println("=== Operation 9: Update key=9 value=Diamonds ===");
        node1.tell(new Messages.ClientUpdate(9, "Diamonds"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Try to read a remote value
        System.out.println("=== Operation 10: Get key=9 ===");
        node1.tell(new Messages.ClientGet(9), client);
        sleepForOperation(OperationType.CLIENT_GET);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        System.out.println("=== Operation 11: Join Node 7 ===");
        node7.tell(new Messages.Join(node1), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        // Update registry to include node7 for printing
        testNodeRegistry.put(7, node7);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        System.out.println("=== Operation 12: Update key=6 value=Emeralds ===");
        node1.tell(new Messages.ClientUpdate(6, "Emeralds"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Just for testing, need to change coordinator on Client
        System.out.println("=== Operation 13: Node 7 crashes ===");
        node7.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Just for testing, need to change coordinator on Client
        System.out.println("=== Operation 14: Node 1 leaves ===");
        node1.tell(new Messages.Leave(), nodeSystem);
        sleepForOperation(OperationType.LEAVE);
        testNodeRegistry.remove(1);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        // Just for testing, need to change coordinator on Client
        System.out.println("=== Operation 15: Node 7 Recovers ===");
        node7.tell(new Messages.Recovery(node10), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        System.out.println("=== Operation 16: Node 7 crashes ===");
        node7.tell(new Messages.Crash(), nodeSystem);
        sleepForOperation(OperationType.CRASH);
        printNodeContents(testNodeRegistry);

        // Just for testing, need to change coordinator on Client
        System.out.println("=== Operation 17: Node 1 Joins ===");
        node1.tell(new Messages.Join(node10), nodeSystem);
        sleepForOperation(OperationType.JOIN);
        testNodeRegistry.put(1, node1);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");

        System.out.println("=== Operation 18: Node 7 Recovers ===");
        node7.tell(new Messages.Recovery(node10), nodeSystem);
        sleepForOperation(OperationType.RECOVERY);
        printNodeContents(testNodeRegistry);

        System.out.println("\n");
        System.out.println("=== All operations completed ===");
            
    }
}