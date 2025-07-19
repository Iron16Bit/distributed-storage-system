package it.unitn.ds1;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;

import akka.actor.ActorRef;
import it.unitn.ds1.types.GetType;
import it.unitn.ds1.utils.VersionedValue;

public class Messages {

    //--Messages--
    public static class Join implements Serializable {
        public final ActorRef bootstrappingPeer;
    
        public Join(ActorRef bootstrappingPeer) {
            this.bootstrappingPeer = bootstrappingPeer;
        }
    
    }

    public static class RequestDataItems implements Serializable{
        public final int askingID;
    
        public RequestDataItems(int askingID) {
            this.askingID = askingID;
        }
    }

    public static class DataItemsResponse implements Serializable {
        public final Map<Integer, VersionedValue> dataItems;
        
        public DataItemsResponse(Map<Integer, VersionedValue> dataItems) {
            this.dataItems = dataItems;
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
        public final GetType getType;

        public ReplicaGet(int key, String operationId, GetType getType) {
            this.key = key;
            this.operationId = operationId;
            this.getType = getType;
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

    public static class RequestNodeRegistry implements Serializable {}

    public static class Announce implements Serializable {
        public final int announcingId;

        public Announce(int announcingId) {
            this.announcingId = announcingId;
        }

    }


    // Add this to your Messages class
    public static class UpdateNodeRegistry implements Serializable {
        public final SortedMap<Integer, ActorRef> nodeRegistry;
        public final boolean isInit;
        
        public UpdateNodeRegistry(SortedMap<Integer, ActorRef> nodeRegistry, boolean isInit) {
            this.nodeRegistry = nodeRegistry;
            this.isInit = isInit;
        }
    }
    
}
