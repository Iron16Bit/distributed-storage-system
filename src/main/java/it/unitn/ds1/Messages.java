package it.unitn.ds1;

import java.io.Serializable;
import java.util.TreeMap;

import akka.actor.ActorRef;
import it.unitn.ds1.utils.VersionedValue;

public class Messages {

    //--Messages--
    public static class JoinMsg implements Serializable {
        public final ActorRef bootstrappingPeer;
    
        public JoinMsg(ActorRef bootstrappingPeer) {
            this.bootstrappingPeer = bootstrappingPeer;
        }
    
    }

    public static class AskAvailableNodes implements Serializable{}

    public static class BootStrappingResponse implements Serializable{
        final TreeMap<Integer, ActorRef> nodeRegistry;
    
        public BootStrappingResponse( TreeMap<Integer,ActorRef> nodeRegistry) {
            this.nodeRegistry = nodeRegistry;
        }
    
    }

    public static class AskDataItems implements Serializable{
        final int askingID;
    
        public AskDataItems(int askingID) {
            this.askingID = askingID;
        }
    
    }

    public static class LeaveMsg implements Serializable {}

    public static class RecoveryMsg implements Serializable {}

    public static class CrashMsg implements Serializable {}

    public static class ClientUpdate implements Serializable {
        public final int key;
        public final String value;
        
        public ClientUpdate(int key, String value) {
            this.key = key;
            this.value = value;
        }

    }

    public static class ReplicaUpdate implements Serializable {
        public final int key;
        public final String value;
        public final String operationId;
        public final VersionedValue currentValue;

        public ReplicaUpdate(int key, String value, String operationId, VersionedValue currentValue) {
            this.key = key;
            this.value = value;
            this.operationId = operationId;
            this.currentValue = currentValue;
        }
    }

    public static class UpdateResponse implements Serializable {
        public final int key;
        public final VersionedValue versionedValue;
        public final String operationId;

        public UpdateResponse(int key, VersionedValue versionedValue, String operationId) {
            this.key = key;
            this.versionedValue = versionedValue;
            this.operationId = operationId;
        }

    }

    public static class ReplicaFinalUpdate implements Serializable {
        public final int key;
        public final VersionedValue versionedValue;
        
        public ReplicaFinalUpdate(int key, VersionedValue versionedValue) {
            this.key = key;
            this.versionedValue = versionedValue;
        }
    }

    public static class ClientGet implements Serializable {
        public final int key;
    
        public ClientGet(int key) {
            this.key = key;
        }
        
    }

    public static class ReplicaGet implements Serializable {
        public final int key;
        public final String operationId;

        public ReplicaGet(int key, String operationId) {
            this.key = key;
            this.operationId = operationId;
        }
    }

    public static class GetResponse implements Serializable {
        public final int key;
        public final VersionedValue value;
        public final String operationId;
    
        public GetResponse(int key, VersionedValue value, String operationId) {
            this.key = key;
            this.value = value;
            this.operationId = operationId;
        }
    }


    // Add this to your Messages class
    public static class UpdateNodeRegistry implements Serializable {
        public final TreeMap<Integer, ActorRef> nodeRegistry;
        
        public UpdateNodeRegistry(TreeMap<Integer, ActorRef> nodeRegistry) {
            this.nodeRegistry = nodeRegistry;
        }
    }
    
}
