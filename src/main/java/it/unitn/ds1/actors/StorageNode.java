package it.unitn.ds1.actors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.utils.VersionedValue;
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

public class StorageNode extends AbstractActor implements DataService {
    
    private final Integer id;
    private ActorRef nextNode = null;
    private boolean isAlive = true;
    private boolean isCrashed = false;
    private boolean isCoordinator = false;

    private Map<Integer,VersionedValue> dataStore = new HashMap<>();
    // private SortedSet<Integer> nodesAlive = new TreeSet<>();

    private TreeMap<Integer, ActorRef> nodeRegistry = new TreeMap<>();


    //--Constructor--
    //TODO coordinator needs to be handled differently
    public StorageNode(int id, boolean isCoordinator) {
        this.id = id;
        this.isCoordinator = isCoordinator;
        this.nodeRegistry.put(id, getSelf());
    }

    //--Getters and Setters--
    public Integer getID() {
        return id;
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
    
    public TreeMap<Integer,ActorRef> getNodesAlive() {
        return nodeRegistry;
    }

    public void setNodesAlive(TreeMap<Integer,ActorRef> nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    private Map<Integer,ActorRef> getNextNNodes(int key) {
        TreeMap<Integer, ActorRef> result = new TreeMap<>();
        
        if(key < 0) {
            return result;
        }

        if(this.nodeRegistry.size() == 1) {
            int mapKey = this.nodeRegistry.firstKey();
            result.put(mapKey, this.nodeRegistry.get(mapKey));
            return result;
        }

        Integer[] nodesAlive = (Integer[])this.nodeRegistry.keySet().toArray();
        
        int startIndex = -1;
        for (int i = 0; i < nodesAlive.length; i++) {
            if (nodesAlive[i] > key) {
                startIndex = i;
                break;
            }
        }

        startIndex = startIndex == -1 ? 0 : startIndex;

        for (int i = startIndex; i < DataStoreManager.REPLICATION_FACTOR; i++) {
            int index = (startIndex + i) % nodesAlive.length;
            result.put(nodesAlive[index], this.nodeRegistry.get(nodesAlive[index]));
        }

        return result;
    }

    //--Data Service--
    @Override
    public void update(int key, String value) {
        //TODO implement in case the data is in another node
        //TODO still have to handle replication part

        //case that it's found in my node
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
    private void onJoin(Messages.JoinMsg msg) {
        //TODO implement
        if (msg.bootstrappingPeer != null) {
            //TODO make it send the request for available nodes
        }

        //This means that the node is the first in the network.
        this.nextNode = null;
        System.out.println("[JOIN NODE] id: " + this.id);
    }

    private void onAskAvailableNodes(Messages.AskAvailableNodes msg) {
        //TODO implement
    }

    private void onBootstrappingResponse(Messages.BootStrappingResponse msg) {
        //TODO implement
    }

    private void onLeave(Messages.LeaveMsg msg) {
        //TODO implement (what to do if you are the only node left?)
    }

    private void onCrash(Messages.CrashMsg msg) {
        //TODO implement
    }

    private void onRecovery(Messages.RecoveryMsg msg) {
        //TODO implement
    }

    private void onUpdateValue(Messages.UpdateValueMsg msg) {
        //TODO implement
        System.out.println("[UPDATE ITEM] Key: " + msg.key +", Value: " + msg.value);

        //how to understand which node posesses which values
        Map<Integer,ActorRef> nodeList = getNextNNodes(msg.key);

        for (Map.Entry<Integer,ActorRef> entry: nodeList.entrySet()) {
            if (entry.getKey() == this.id) {
                update(msg.key,msg.value);
                continue;
            }
            ActorRef receiverNode = entry.getValue();
            receiverNode.tell(msg,getSelf());
        }
    }



    //client calls Get -> Node, Then Node calls a (different) Get on all replicas

    private void onGetValue (Messages.GetValueMsg msg) {
        //TODO implement
        System.out.println("[REQUESTED VALUE] Key: " + msg.key);

        //cannot be negative just for starting the logic
        VersionedValue requestedValue = new VersionedValue(null, -1);
        
        if(this.isCoordinator) {
            Map<Integer,ActorRef> nodeList = getNextNNodes(msg.key);
            
            List<VersionedValue> valuesCollected = new ArrayList<>();

            for (Map.Entry<Integer,ActorRef> entry: nodeList.entrySet()) {
                if (entry.getKey() == this.id) {
                    valuesCollected.add(this.dataStore.get(msg.key));
                    continue;
                }
                entry.getValue().tell(msg, getSelf());
            }

            //after collecting all data
            for (VersionedValue value: valuesCollected) {
                requestedValue =  value.getVersion() > requestedValue.getVersion() ? value : requestedValue; 
            }
        } else {
            requestedValue = get(msg.key);
        }

        Messages.GetValueResponseMsg response = new Messages.GetValueResponseMsg(msg.key, requestedValue);
        sender().tell(response, self());
    }
    
    //--akka--
    static public Props props(int id, boolean isCoordinator) {
        return Props.create(StorageNode.class, () -> new StorageNode(id, isCoordinator));
    }

    @Override
    public Receive createReceive() {
        //TODO check if something missing
        return receiveBuilder()
            .match(Messages.JoinMsg.class, this::onJoin)
            .match(Messages.AskAvailableNodes.class, this::onAskAvailableNodes)
            .match(Messages.BootStrappingResponse.class, this::onBootstrappingResponse)
            .match(Messages.LeaveMsg.class, this::onLeave)
            .match(Messages.CrashMsg.class, this::onCrash)
            .match(Messages.RecoveryMsg.class, this::onRecovery)
            .match(Messages.UpdateValueMsg.class, this::onUpdateValue)
            .match(Messages.GetValueMsg.class, this::onGetValue)
            .build();
    }    
}
