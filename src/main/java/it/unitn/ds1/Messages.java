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

    public static class UpdateValueMsg implements Serializable {
        public final int key;
        public final String value;
        
        public UpdateValueMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class GetValueMsg implements Serializable {
        public final int key;
    
        public GetValueMsg(int key) {
            this.key = key;
        }
        
    }

    public static class GetValueResponseMsg implements Serializable {
        public final int key;
        public final VersionedValue value;
    
        public GetValueResponseMsg(int key, VersionedValue value) {
            this.key = key;
            this.value = value;
        }
    }
    
}
