package it.unitn.ds1.actors;

import java.lang.Runtime.Version;
import java.lang.foreign.Linker.Option;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.types.GetType;
import it.unitn.ds1.types.OpType;
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
    private boolean isAlive = true;
    private boolean isCrashed = false;

    private Map<Integer,VersionedValue> dataStore = new HashMap<>();
    private SortedMap<Integer, ActorRef> nodeRegistry = new TreeMap<>();
    private Map<String, GetOperation> pendingGet = new HashMap<>();
    private Map<String, UpdateOperation> pendingUpdate = new HashMap<>();
    private int operationCounter = 0;

    // created through lack of efficient ideas
    private Map<Integer, Set<String>> keylocks = new HashMap<>();
    private Map<Integer, OpType> keyLockTypes = new HashMap<>();

    private static class GetOperation {
        final int key;
        final ActorRef client;
        final List<VersionedValue> responses;
        final long startTime;
        final String updateValue; // Add this field
        final GetType getType;

        // Constructor for regular get operations
        GetOperation(int key, ActorRef client, GetType getType) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
            this.startTime = System.currentTimeMillis();
            this.updateValue = null;
            this.getType = getType;
        }

        // Constructor for read-before-update operations
        GetOperation(int key, ActorRef client, String updateValue) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
            this.startTime = System.currentTimeMillis();
            this.updateValue = updateValue;
            this.getType = GetType.UPDATE;
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
    }

    //--Getters and Setters--
    public Integer getID() {
        return id;
    }

    public Map<Integer, VersionedValue> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<Integer, VersionedValue> dataStore) {
        this.dataStore = dataStore;
    }
    
    public SortedMap<Integer,ActorRef> getNodesAlive() {
        return nodeRegistry;
    }

    public void setNodeRegistry(SortedMap<Integer,ActorRef> nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    //works only on the same coordinator
    private boolean isLocked(int key, OpType op) {
        List<String> getOperationIdsList = new ArrayList<>(pendingGet.keySet());
        List<String> updateOperationIdsList = new ArrayList<>(pendingUpdate.keySet());
        String pattern = "-"+key+"-";
        return switch (op) {
            case GET -> {
                for (String operationsId: updateOperationIdsList) {
                    if (operationsId.contains(pattern)) yield true;
                }
                yield false;
            }
            case UPDATE -> {
                for(String operationsId: updateOperationIdsList) {
                    if (operationsId.contains(pattern)) yield true;
                }
                for(String operationsId: getOperationIdsList) {
                    if (operationsId.contains(pattern)) yield true;
                }
                yield false;
            }
            default -> {
                System.out.println("WTF MOMENT, operation not recognized");
                yield false;
            }
        };
    }
    
    private boolean acquireLock(int key, OpType op, String operationId) {
        Set<String> locks = keylocks.computeIfAbsent(key, k -> new HashSet<>());
        OpType currentLockType = keyLockTypes.get(key);

        if (locks.isEmpty()) {
            // No existing locks, acquire new one
            locks.add(operationId);
            keyLockTypes.put(key, op);
            return true;
        }

        if (currentLockType == OpType.GET && op == OpType.GET) {
            // Multiple reads allowed
            locks.add(operationId);
            return true;
        }

        // Write operation or mixed read / write => deny
        return false;
    }

    private void releaseLock(int key, String operationId) {
        Set<String> locks = keylocks.get(key);
        if (locks != null) {
            locks.remove(operationId);
            if (locks.isEmpty()) {
                keylocks.remove(key);
                keyLockTypes.remove(key);
            }
        }
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
        for (int i = 0; i < DataStoreManager.N && i < nodeIds.size(); i++) {
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
    private void onJoin(Messages.Join msg) {
        //TODO in case of timeout ask/change bootstrapping peer
        msg.bootstrappingPeer.tell(new Messages.RequestNodeRegistry(), getSelf());
    }

    private void onRequestNodeRegistry(Messages.RequestNodeRegistry msg) {
        sender().tell(new Messages.UpdateNodeRegistry(this.nodeRegistry, false), getSelf());
    }

    private void onRequestDataItems(Messages.RequestDataItems msg) {
        Map<Integer, VersionedValue> requestedDataItems = this.dataStore.entrySet().stream()
            .filter(entry -> entry.getKey() <= msg.askingID)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        sender().tell(new Messages.DataItemsResponse(requestedDataItems), getSelf());
    }

    private void onDataItemsReponse(Messages.DataItemsResponse msg) {
        for (Integer key: msg.dataItems.keySet()) {
            
            String operationId = this.id + "-" + key + "-" + (++this.operationCounter);

            GetOperation getOperation = new GetOperation(key, getSelf(), GetType.INIT);
            pendingGet.put(operationId, getOperation);
            
            List<ActorRef> nNodes = getNextNNodes(key);

            for (ActorRef ref: nNodes) {
                ref.tell(new Messages.ReplicaGet(key, operationId, GetType.INIT), getSelf());
            }
        }

        this.nodeRegistry.put(this.id, getSelf());
        System.out.println("I have inserted my self inside the nodeRegistry: id ->" + this.id);

        for (ActorRef ref: nodeRegistry.values()) {
            if (ref != getSelf()) ref.tell(new Messages.Announce(this.id), getSelf());
        }

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

        if(isLocked(msg.key, OpType.UPDATE) || isLocked(msg.key, OpType.GET)) {
            System.out.println("Coordinator noticed item is locked by a possible read/write");
            return;
        }

        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        List<ActorRef> nodeList = getNextNNodes(msg.key);

        // Create a get operation that includes the update value
        GetOperation getOperation = new GetOperation(msg.key, sender(), msg.value);
        pendingGet.put(operationId, getOperation);
        UpdateOperation updateOperation = new UpdateOperation(msg.key, sender());
        pendingUpdate.put(operationId, updateOperation);

        // Send ReplicaGet to all replica nodes to read current values
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = this.dataStore.get(msg.key);
                getOperation.responses.add(localValue);
                System.out.println("[LOCAL READ FOR UPDATE] versioned value: " + localValue);
            } else {
                ref.tell(new Messages.ReplicaGet(msg.key, operationId, GetType.UPDATE), getSelf());
            }
        }

        checkGetOperation(operationId);

    }

    private void onReplicaUpdate(Messages.ReplicaUpdate msg) {
        VersionedValue result = updateWithCurrentValue(msg.key, msg.value, msg.currentValue);
        System.out.println("[REMOTE NODE " + this.id + "] Datastore" + this.dataStore);
        
        // release lock
        releaseLock(msg.key, msg.operationId);
    }


    private void onClientGet(Messages.ClientGet msg) {
        //TODO implement
        System.out.println("[REQUESTED VALUE] Key: " + msg.key);

        if(isLocked(msg.key, OpType.UPDATE)) {
            System.out.println("Coordinator noticed item is locked by a possible write");
            return;
        }

        // Generate unique operation ID
        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);


        //Calculate read quorum (e.g., majority)
        //TODO make read quorum constant
        List<ActorRef> nodeList = getNextNNodes(msg.key);

        // Create operation State
        GetOperation operation = new GetOperation(msg.key, sender(), GetType.GET);
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
                ref.tell(new Messages.ReplicaGet(msg.key, operationId, GetType.GET), getSelf());
            }
        }

        // Check if quorum already achieved (in case of single node or local-only data)
        checkGetOperation(operationId);
    }
    
    
    private void onReplicaGet(Messages.ReplicaGet msg) {
        if (msg.getType == GetType.GET || msg.getType == GetType.INIT) {

            if (!acquireLock(msg.key, OpType.GET, msg.operationId)) {
                //maybe send message back
                System.err.println("[NODE" + this.id + "] Cannot execute get on data " + msg.key + " is locked");
                return;
            }
            

            VersionedValue requestedValue = this.dataStore.get(msg.key);
            sender().tell(new Messages.GetResponse(msg.key, requestedValue, msg.operationId), getSelf());
            // System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);

            // Release lock after sending response
            releaseLock(msg.key, msg.operationId);

        } else {
            if (!acquireLock(msg.key, OpType.UPDATE, msg.operationId)) {
                //maybe send message back
                System.err.println("[NODE" + this.id + "] Cannot execute get on data " + msg.key + " is locked");
                return;
            }
            

            VersionedValue requestedValue = this.dataStore.get(msg.key);
            sender().tell(new Messages.GetResponse(msg.key, requestedValue, msg.operationId), getSelf());
            // System.out.println("[NODE " + this.id + "] Datastore" + this.dataStore);

            // Release lock after sending response
            releaseLock(msg.key, msg.operationId);
        }
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

        switch (operation.getType) {
            case UPDATE -> {
                if (operation.responses.size() >= DataStoreManager.W) {
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
                    pendingGet.remove(operationId);
                    performActualUpdate(operation.key, operation.updateValue, operationId, operation.client, currentValue);
                }
            }
            case GET -> {
                if (operation.responses.size() >= DataStoreManager.R) {
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
            case INIT -> {
                if (operation.responses.size() == DataStoreManager.N) {
                    VersionedValue latestValue = null;
                    for (VersionedValue value: operation.responses) {
                        if (value != null && (latestValue == null || value.getVersion() > latestValue.getVersion())) {
                            latestValue = value;
                        }
                    }
                    
                    this.dataStore.put(operation.key, latestValue);
                    pendingGet.remove(operationId);
                    System.out.println("[NODE " + this.id + "] Inserted Value: " + latestValue);
                }
            }
        }
    }

    private void performActualUpdate(int key, String value, String operationId, ActorRef client, VersionedValue currentValue) {
        List<ActorRef> nodeList = getNextNNodes(key);

        UpdateOperation operation = pendingUpdate.get(operationId);

        if (operation == null) {
            System.err.println("UpdateOperation not found for operationId: " + operationId);
            return;
        }

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

        pendingUpdate.remove(operationId);
    }

    

    //TODO for testing
    private void onUpdateNodeRegistry(Messages.UpdateNodeRegistry msg) {
        this.nodeRegistry.clear();
        this.nodeRegistry.putAll(msg.nodeRegistry);
        // Keep self in registry
        if(msg.isInit) {
            this.nodeRegistry.put(this.id, getSelf());
        } else {
            SortedMap<Integer, ActorRef> higherNodes = this.nodeRegistry.tailMap(this.id);
            ActorRef neighbor;
            if (higherNodes.isEmpty()) {
                neighbor = this.nodeRegistry.firstEntry().getValue();
            } else {
                neighbor = higherNodes.firstEntry().getValue();
            }
            neighbor.tell(new Messages.RequestDataItems(this.id), getSelf());
        }

        System.out.println("[NODE REGISTRY UPDATED] Node " + this.id + " now knows about " + this.nodeRegistry.size() + " nodes");
    }

    private void onAnnouce(Messages.Announce msg) {
        this.nodeRegistry.put(msg.announcingId, sender());
        Map<Integer, VersionedValue> requestedDataItems = this.dataStore.entrySet().stream()
            .filter(entry -> entry.getKey() <= msg.announcingId)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for(Integer key: requestedDataItems.keySet()) {
            List<ActorRef> shouldPosess = getNextNNodes(key);
            if (!shouldPosess.contains(getSelf())) {
                VersionedValue itemToRemove = this.dataStore.get(key); 
                System.out.println("[NODE " + this.id + "] Due to join operation of NODE: " + msg.announcingId + ", dropped data item: (key -> " + key + ") " + itemToRemove);
                this.dataStore.remove(key);
            }
        }
    }

    //--akka--
    static public Props props(int id) {
        return Props.create(StorageNode.class, () -> new StorageNode(id));
    }

    @Override
    public Receive createReceive() {
        //TODO check if something missing
        return receiveBuilder()
            .match(Messages.Join.class, this::onJoin)
            .match(Messages.RequestNodeRegistry.class, this::onRequestNodeRegistry)
            .match(Messages.RequestDataItems.class, this::onRequestDataItems)
            .match(Messages.DataItemsResponse.class, this::onDataItemsReponse)
            .match(Messages.Announce.class, this::onAnnouce)
            .match(Messages.UpdateNodeRegistry.class, this::onUpdateNodeRegistry) // for testing
            .match(Messages.LeaveMsg.class, this::onLeave)
            .match(Messages.CrashMsg.class, this::onCrash)
            .match(Messages.RecoveryMsg.class, this::onRecovery)
            .match(Messages.ClientUpdate.class, this::onClientUpdate)
            .match(Messages.ReplicaUpdate.class, this::onReplicaUpdate)
            .match(Messages.ClientGet.class, this::onClientGet)
            .match(Messages.ReplicaGet.class, this::onReplicaGet)
            .match(Messages.GetResponse.class, this::onGetResponse)
            .build();
    }    
}
