package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("Distributed-Storage-System");
        
        final ActorRef node1 = system.actorOf(
            Node.props(1),
            "node-1"
        );

        final ActorRef node2 = system.actorOf(
            Node.props(5),
            "node-5"
        );

        final ActorRef client = system.actorOf(
            Client.props(node1),
            "client-1"
        );

        // Store a value (client sends UpdateValueMsg to node)
        node1.tell(new Node.UpdateValueMsg(100, "Hello World"), client);
        
        // Get a value (client sends GetValueMsg to node)
        node1.tell(new Node.GetValueMsg(100), client);
        
        // Store another value
        node1.tell(new Node.UpdateValueMsg(200, "Another Value"), client);
        
        // Get the second value
        node1.tell(new Node.GetValueMsg(200), client);
        
        // Try to get a non-existent key
        node1.tell(new Node.GetValueMsg(999), client);
    }
}