package it.unitn.ds1;

import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;

public class Main {
    public static void main(String[] args) {
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

        // For Testing we are going to make them know of each other
        TreeMap<Integer, ActorRef> testNodeRegistry = new TreeMap<>();
        testNodeRegistry.put(1, node1);
        testNodeRegistry.put(5, node5);
        testNodeRegistry.put(10, node10);

        // Send the registry to both nodes
        node1.tell(new Messages.UpdateNodeRegistry(testNodeRegistry), ActorRef.noSender());
        node5.tell(new Messages.UpdateNodeRegistry(testNodeRegistry), ActorRef.noSender());
        node10.tell(new Messages.UpdateNodeRegistry(testNodeRegistry), ActorRef.noSender());

        // Small delay to ensure registry updates are processed
        try { Thread.sleep(100); } catch (InterruptedException e) { }

        final ActorRef client = system.actorOf(
            Client.props(node1),
            "client-1"
        );

        try {
            // Store a value (client sends UpdateValueMsg to node)
            System.out.println("=== Operation 1: Store key=1, value=Alluminium ===");
            node1.tell(new Messages.ClientUpdate(1, "Alluminium"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");
            
            // Get a value (client sends GetValueMsg to node)
            System.out.println("=== Operation 2: Get key=1 ===");
            node1.tell(new Messages.ClientGet(1), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");
            
            // Store another value
            System.out.println("=== Operation 3: Store key=3, value=Gold ===");
            node1.tell(new Messages.ClientUpdate(6, "Gold"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");
            
            // Get the second value
            System.out.println("=== Operation 4: Get key=6 ===");
            node1.tell(new Messages.ClientGet(6), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");
            
            // Try to get a non-existent key
            System.out.println("=== Operation 5: Get key=200 (non-existent) ===");
            node1.tell(new Messages.ClientGet(200), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");

            // Try to update a local value
            System.out.println("=== Operation 6: Update key=1 value=Silver ===");
            node1.tell(new Messages.ClientUpdate(1, "Silver"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");

            // Try to update a remote value
            System.out.println("=== Operation 7: Update key=6 value=Sapphire ===");
            node1.tell(new Messages.ClientUpdate(6, "Sapphire"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");

            // Try to update a remote value
            System.out.println("=== Operation 8: Update key=11 value=Diamond ===");
            node1.tell(new Messages.ClientUpdate(9, "Diamond"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");

            // Try to update a remote value
            System.out.println("=== Operation 9: Update key=11 value=Diamonds ===");
            node1.tell(new Messages.ClientUpdate(9, "Diamonds"), client);
            Thread.sleep(1000); // 1 second delay

            System.out.println("\n");

            // Try to read a remote value
            System.out.println("=== Operation 10: Get key=11 ===");
            node1.tell(new Messages.ClientGet(9), client);
            Thread.sleep(1000); // 1 second delay
            
            System.out.println("=== All operations completed ===");
            
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
        }
    }
}