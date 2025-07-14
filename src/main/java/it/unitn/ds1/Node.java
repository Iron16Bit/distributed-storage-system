package it.unitn.ds1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.AbstractActor;

/* 
 * Action to be implemented:
 * Announcing to whole system:
 *  - Join: get value + message in broadcast
 *  - Quit: message in broadcast + drop values to all other nodes
 * Handle crash state (receiving of message) + Recovery
 * Hanlde request for read and write (quorum)
 */


interface DataService {
  public void update(int key, String value);
  public VersionedValue get(int key);
}

public class Node extends AbstractActor implements DataService {
    
    private final Integer ID;
    private ActorRef nextNode;
    private boolean isAlive = true;
    private boolean isCrashed = false;

    private Map<Integer,VersionedValue> dataStore = new HashMap<>();
    private SortedSet<Integer> nodesAlive = new TreeSet<>();


    //--Messages--
    public static class JoinMsg implements Serializable {
        final ActorRef bootstrappingPeer;

        public JoinMsg(ActorRef bootstrappingPeer) {
            this.bootstrappingPeer = bootstrappingPeer;
        }

    }
    public static class AskAvailableNodes implements Serializable{}
    public static class BootStrappingResponse implements Serializable{
        final SortedSet<Integer> nodesAlive;

        public BootStrappingResponse(SortedSet<Integer> nodesAlive) {
            this.nodesAlive = nodesAlive;
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
        final int key;
        final String value;
        
        public UpdateValueMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    public static class GetValueMsg implements Serializable {
        final int key;

        public GetValueMsg(int key) {
            this.key = key;
        }
        
    }
    public static class GetValueResponse implements Serializable {
        final int key;
        final VersionedValue value;
    
        public GetValueResponse(int key, VersionedValue value) {
            this.key = key;
            this.value = value;
        }
    }

    //--Constructor--
    public Node(int ID) {
        this.ID = ID;
        this.nodesAlive.add(ID);
    }

    //--Getters and Setters--
    public Integer getID() {
        return ID;
    }

    public ActorRef getNextNode() {
        return nextNode;
    }

    public void setNextNode(ActorRef nextNode) {
        this.nextNode = nextNode;
    }

    public Map<Integer, VersionedValue> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<Integer, VersionedValue> dataStore) {
        this.dataStore = dataStore;
    }
    
    public SortedSet<Integer> getNodesAlive() {
        return nodesAlive;
    }

    public void setNodesAlive(SortedSet<Integer> nodesAlive) {
        this.nodesAlive = nodesAlive;
    }

    //--DataService--
    @Override
    public void update(int key, String value) {
        //TODO implement in case the data is in another node
        VersionedValue versionedValue = get(key);
        if (versionedValue == null) {
            // Create new versioned value if it doesn't exist
            versionedValue = new VersionedValue(value, 1);
            dataStore.put(key, versionedValue);
        } else {
            // Update existing value
            versionedValue.setValue(value);
            versionedValue.setVersion(versionedValue.getVersion() + 1);
        }
    }

    @Override
    public VersionedValue get(int key) {
        return getDataStore().get(key); // Can return null if key doesn't exist
    }

    //--Actions on Messages--
    private void onJoin(JoinMsg msg) {
        //TODO implement

    }
    private void onLeave(LeaveMsg msg) {
        //TODO implement
    }

    private void onCrash(CrashMsg msg) {
        //TODO implement
    }

    private void onRecovery(RecoveryMsg msg) {
        //TODO implement
    }

    private void onUpdateValue(UpdateValueMsg msg) {
        //TODO implement
        update(msg.key,msg.value);

    }

    private void onGetValue (GetValueMsg msg) {
        //TODO implement
        get(msg.key);
    }
    
    //--akka--
    static public Props props(int ID) {
        return Props.create(Node.class, () -> new Node(ID));
    }

    @Override
    public Receive createReceive() {
        //TODO check if something missing
        return receiveBuilder()
            .match(JoinMsg.class, this::onJoin)
            .match(LeaveMsg.class, this::onLeave)
            .match(CrashMsg.class, this::onCrash)
            .match(RecoveryMsg.class, this::onRecovery)
            .match(UpdateValueMsg.class, this::onUpdateValue)
            .match(GetValueMsg.class, this::onGetValue)
            .build();
    }    
}
