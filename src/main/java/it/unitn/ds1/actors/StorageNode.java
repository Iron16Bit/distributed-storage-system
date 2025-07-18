package it.unitn.ds1.actors;

import java.lang.Runtime.Version;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.utils.VersionedValue;

/* 
 * Action to be implemented:
 * Announcing to whole system:
 *  - Join: get value + message in broadcast
 *  - Quit: message in broadcast + drop values to all other nodes
 * Handle crash state (receiving of message) + Recovery
 * Hanlde request for read and write (quorum)
 */


interface DataService {
  VersionedValue updateWithCurrentValue(int key, String value, VersionedValue currentValue);
  VersionedValue get(int key);
}

public class StorageNode extends AbstractActor implements DataService {
    
    private final Integer id;
    private ActorRef nextNode = null;
    private boolean isAlive = true;
    private boolean isCrashed = false;

    private Map<Integer,VersionedValue> dataStore = new HashMap<>();
    private TreeMap<Integer, ActorRef> nodeRegistry = new TreeMap<>();
    
    private Map<String, GetOperation> pendingGet = new HashMap<>();
    private Map<String, UpdateOperation> pendingUpdate = new HashMap<>();
    private int operationCounter = 0;

    private static class GetOperation {
        final int key;
        final ActorRef client;
        final int requiredQuorum;
        final List<VersionedValue> responses;
        final long startTime;
        final String updateValue; // Add this field
        final boolean isForUpdate; // Add this flag

        // Constructor for regular get operations
        GetOperation(int key, ActorRef client, int quorum) {
            this.key = key;
            this.client = client;
            this.requiredQuorum = quorum;
            this.responses = new ArrayList<>();
            this.startTime = System.currentTimeMillis();
            this.updateValue = null;
            this.isForUpdate = false;
        }

        // Constructor for read-before-update operations
        GetOperation(int key, ActorRef client, int quorum, String updateValue) {
            this.key = key;
            this.client = client;
            this.requiredQuorum = quorum;
            this.responses = new ArrayList<>();
            this.startTime = System.currentTimeMillis();
            this.updateValue = updateValue;
            this.isForUpdate = true;
        }
    }


    private static class UpdateOperation {
        final int key;
        final ActorRef client;
        final List<VersionedValue> responses;
        final long startTime;

        UpdateOperation(int key, ActorRef client) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
            this.startTime = System.currentTimeMillis();
        }
    }


    //--Constructor--
    //TODO coordinator needs to be handled differently
    public StorageNode(int id) {
        this.id = id;
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

    public void setNodeRegistry(TreeMap<Integer,ActorRef> nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }


    private List<ActorRef> getNextNNodes(int key) {
        List<ActorRef> result = new ArrayList<>();
        
        if (key < 0 || this.nodeRegistry.isEmpty()) {
            return result;
        }

        // For single node, return self
        if (this.nodeRegistry.size() == 1) {
            result.add(this.nodeRegistry.firstEntry().getValue());
            return result;
        }

        // Get all node IDs in sorted order
        List<Integer> nodeIds = new ArrayList<>(this.nodeRegistry.keySet());
        
        // Find the first node with ID >= key (consistent hashing)
        int startIndex = 0;
        for (int i = 0; i < nodeIds.size(); i++) {
            if (nodeIds.get(i) >= key) {
                startIndex = i;
                break;
            }
        }

        // Add REPLICATION_FACTOR nodes starting from startIndex (with wrap-around)
        for (int i = 0; i < DataStoreManager.REPLICATION_FACTOR && i < nodeIds.size(); i++) {
            int index = (startIndex + i) % nodeIds.size();
            Integer nodeId = nodeIds.get(index);
            result.add(this.nodeRegistry.get(nodeId));
        }

        return result;
    } 


    @Override
    public VersionedValue updateWithCurrentValue(int key, String value, VersionedValue currentValue) {
        VersionedValue newVersionedValue;
        
        if (currentValue == null) {
            // Create new versioned value if it doesn't exist
            newVersionedValue = new VersionedValue(value, 1);
        } else {
            // Update existing value with incremented version
            newVersionedValue = new VersionedValue(value, currentValue.getVersion() + 1);
        }

        dataStore.put(key, newVersionedValue);
        return newVersionedValue;
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

    private void onClientUpdate(Messages.ClientUpdate msg) {
        System.out.println("[UPDATE ITEM] Key: " + msg.key +", Value: " + msg.value);

        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        List<ActorRef> nodeList = getNextNNodes(msg.key);
        int readQuorum = (nodeList.size() / 2) + 1;

        // Create a get operation that includes the update value
        GetOperation getOperation = new GetOperation(msg.key, sender(), readQuorum, msg.value);
        pendingGet.put(operationId, getOperation);

        // Send ReplicaGet to all replica nodes to read current values
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = this.dataStore.get(msg.key);
                getOperation.responses.add(localValue);
                System.out.println("[LOCAL READ FOR UPDATE] versioned value: " + localValue);
            } else {
                ref.tell(new Messages.ReplicaGet(msg.key, operationId), getSelf());
            }
        }

        checkGetOperation(operationId);
    }

    private void onReplicaUpdate(Messages.ReplicaUpdate msg) {
        updateWithCurrentValue(msg.key, msg.value, msg.currentValue);
        System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);
    }


    private void onClientGet(Messages.ClientGet msg) {
        //TODO implement
        System.out.println("[REQUESTED VALUE] Key: " + msg.key);

        // Generate unique operation ID
        String operationId = this.id + "-" + (++operationCounter);


        //Calculate read quorum (e.g., majority)
        //TODO make read quorum constant
        List<ActorRef> nodeList = getNextNNodes(msg.key);
        int readQuorum = (nodeList.size() / 2) + 1;

        // Create operation State
        GetOperation operation = new GetOperation(msg.key, sender(), readQuorum);
        pendingGet.put(operationId, operation);
    
        // Send ReplicaGet to all replica nodes (including self)
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                //Handle local read immediatly, if present
                VersionedValue localValue = this.dataStore.get(msg.key);
                operation.responses.add(localValue);
                System.out.println("[LOCAL GET] versioned value: " + localValue);
                // System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);
            } else {
                ref.tell(new Messages.ReplicaGet(msg.key, operationId), getSelf());
            }
        }

        // Check if quorum already achieved (in case of single node or local-only data)
        checkGetOperation(operationId);
    }
    
    
    private void onReplicaGet(Messages.ReplicaGet msg) {
        VersionedValue requestedValue = this.dataStore.get(msg.key);
        sender().tell(new Messages.GetResponse(msg.key, requestedValue, msg.operationId), getSelf());
        // System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);
    }

    private void onGetResponse(Messages.GetResponse msg) {
        // Find the corresponding get operation
        GetOperation operation = pendingGet.get(msg.operationId);

        if (operation == null) return; // Opeartion might have already completed or timed out
        
        // Add response to collected responses
        operation.responses.add(msg.value);
        
        // Check if we have enough responses for quorum
        checkGetOperation(msg.operationId);
    }
    
    private void checkGetOperation(String operationId) {
        GetOperation operation = pendingGet.get(operationId);
        
        if (operation == null) return;

        if (operation.isForUpdate) {
            if (operation.responses.size() >= operation.requiredQuorum) {
                VersionedValue currentValue = null;
                for (VersionedValue value: operation.responses) {
                    if (value != null && (currentValue == null || value.getVersion() > currentValue.getVersion())) {
                        currentValue = value;
                    }
                }

                Messages.UpdateResponse response;
                
                if (currentValue == null) {
                    response = new Messages.UpdateResponse(operation.key, new VersionedValue(operation.updateValue, 1), operationId);
                } else {
                    response = new Messages.UpdateResponse(operation.key, new VersionedValue(operation.updateValue, currentValue.getVersion() + 1), operationId);
                }

                operation.client.tell(response, getSelf());
                // This was a read-before-update, now perform the actual update
                performActualUpdate(operation.key, operation.updateValue, operationId, operation.client, currentValue);
                pendingGet.remove(operationId);
            }
        } else {
            if (operation.responses.size() >= operation.requiredQuorum) {
                VersionedValue latestValue = null;
                for (VersionedValue value: operation.responses) {
                    if (value != null && (latestValue == null || value.getVersion() > latestValue.getVersion())) {
                        latestValue = value;
                    }
                }
                // This was a regular get, send response to client
                Messages.GetResponse response = new Messages.GetResponse(operation.key, latestValue, operationId);
                operation.client.tell(response, getSelf());
                pendingGet.remove(operationId);
                System.out.println("[READ COMPLETED] Key: " + operation.key);
            }
        }
    }

    private void performActualUpdate(int key, String value, String operationId, ActorRef client, VersionedValue currentValue) {
        List<ActorRef> nodeList = getNextNNodes(key);

        UpdateOperation operation = new UpdateOperation(key, client);
        pendingUpdate.put(operationId, operation);

        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue versionedValue = updateWithCurrentValue(key, value, currentValue);
                operation.responses.add(versionedValue);
                System.out.println("[LOCAL UPDATE] versioned value: " + versionedValue);
                System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);
            } else {
                ref.tell(new Messages.ReplicaUpdate(key, value, operationId, currentValue), getSelf());
            }
        }
    }

    

    //TODO for testing
    private void onUpdateNodeRegistry(Messages.UpdateNodeRegistry msg) {
        this.nodeRegistry.clear();
        this.nodeRegistry.putAll(msg.nodeRegistry);
        // Keep self in registry
        this.nodeRegistry.put(this.id, getSelf());
        System.out.println("[NODE REGISTRY UPDATED] Node " + this.id + " now knows about " + this.nodeRegistry.size() + " nodes");
    }

    //--akka--
    static public Props props(int id) {
        return Props.create(StorageNode.class, () -> new StorageNode(id));
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
            .match(Messages.ClientUpdate.class, this::onClientUpdate)
            .match(Messages.ReplicaUpdate.class, this::onReplicaUpdate)
            .match(Messages.ClientGet.class, this::onClientGet)
            .match(Messages.ReplicaGet.class, this::onReplicaGet)
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.UpdateNodeRegistry.class, this::onUpdateNodeRegistry) // for testing
            .build();
    }    
}
