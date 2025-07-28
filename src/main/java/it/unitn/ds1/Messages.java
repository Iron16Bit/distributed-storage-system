package it.unitn.ds1;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;

import akka.actor.ActorRef;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.VersionedValue;

public class Messages {

    public static final int DELAY = 450;

    // Debug message to print the contents of a node's data store
    public static class DebugPrintDataStore implements Serializable {}


    public static class Timeout implements Serializable {
        public final String operationId;
        public final OperationType operationType;
        public final int key;

        public Timeout(String operationId, OperationType operationType, int key) {
            this.operationId = operationId;
            this.operationType = operationType;
            this.key = key;
        }
    }

    //--Messages--
    public static class Join implements Serializable {
        public final ActorRef bootstrappingPeer;
    
        public Join(ActorRef bootstrappingPeer) {
            this.bootstrappingPeer = bootstrappingPeer;
        }
    
    }

    public static class RequestDataItems implements Serializable{
        public final int askingID;
        public final OperationType operationType;
    
        public RequestDataItems(int askingID, OperationType operationType) {
            this.askingID = askingID;
            this.operationType = operationType;
        }
    }

    public static class DataItemsResponse implements Serializable {
        public final Map<Integer, VersionedValue> dataItems;
        public final OperationType operationType;
        
        public DataItemsResponse(Map<Integer, VersionedValue> dataItems, OperationType operationType) {
            this.dataItems = dataItems;
            this.operationType = operationType;
        }
    }

    public static class Leave implements Serializable {}

    public static class NotifyLeave implements Serializable {}

    public static class LeaveACK implements Serializable {}

    public static class RepartitionData implements Serializable {
        public final int leavingId;
        public final Map<Integer, VersionedValue> items;

        public RepartitionData(int leavingId, Map<Integer, VersionedValue> items) {
            this.leavingId = leavingId;
            this.items = items;
        }
    }

    public static class Recovery implements Serializable {
        public final ActorRef recoveryNode;

        public Recovery(ActorRef recoveryNode) {
            this.recoveryNode = recoveryNode;
        }
    }

    public static class Crash implements Serializable {}

    public static class InitiateUpdate implements Serializable {
        public final int key;
        public final String value;
        public final ActorRef coordinator;

        public InitiateUpdate(int key, String value, ActorRef coordinator) {
            this.key = key;
            this.value = value;
            this.coordinator = coordinator;
        }
    }

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

    public static class InitiateGet implements Serializable {
        public final int key;
        public final ActorRef coordinator;

        public InitiateGet(int key, ActorRef coordinator) {
            this.key = key;
            this.coordinator = coordinator;
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
        public final OperationType operationType;

        public ReplicaGet(int key, String operationId, OperationType operationType) {
            this.key = key;
            this.operationId = operationId;
            this.operationType = operationType;
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

    public static class RequestNodeRegistry implements Serializable {
        public final OperationType operationType;

        public RequestNodeRegistry(OperationType operationType) {
            this.operationType = operationType;
        }
    }

    public static class Announce implements Serializable {
        public final int announcingId;

        public Announce(int announcingId) {
            this.announcingId = announcingId;
        }

    }

    public static class Error implements Serializable {
        public final int key;
        public final String operationId;
        public final OperationType operationType;

        public Error(int key, String operationId, OperationType operationType) {
            this.key = key;
            this.operationId = operationId;
            this.operationType = operationType;
        }
    }


    // Add this to your Messages class
    public static class UpdateNodeRegistry implements Serializable {
        public final SortedMap<Integer, ActorRef> nodeRegistry;
        public final OperationType operationType;
        
        public UpdateNodeRegistry(SortedMap<Integer, ActorRef> nodeRegistry, OperationType operationType) {
            this.nodeRegistry = nodeRegistry;
            this.operationType = operationType;
        }
    }
    
}
