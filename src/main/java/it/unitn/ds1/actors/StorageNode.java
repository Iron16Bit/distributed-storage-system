package it.unitn.ds1.actors;

import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.types.GetType;
import it.unitn.ds1.types.OpType;
import it.unitn.ds1.types.UpdateType;
import it.unitn.ds1.utils.TimeoutDelay;
import it.unitn.ds1.utils.VersionedValue;
import it.unitn.ds1.utils.OperationDelays.OperationType;

interface DataService {
  VersionedValue updateWithCurrentValue(int key, String value, VersionedValue currentValue);
  VersionedValue get(int key);
}

public class StorageNode extends AbstractActor implements DataService {
    
    private final Integer id;
    private final Random random;

    private SortedMap<Integer,VersionedValue> dataStore = new TreeMap<>();
    private SortedMap<Integer, ActorRef> nodeRegistry = new TreeMap<>();
    private Map<String, GetOperation> pendingGet = new HashMap<>();
    private Map<String, UpdateOperation> pendingUpdate = new HashMap<>();
    private int operationCounter = 0;

    // created through lack of efficient ideas
    private Map<Integer, Set<String>> keylocks = new HashMap<>();
    private Map<Integer, OpType> keyLockTypes = new HashMap<>();


    private LeaveOperation leaveOperation;

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

    private static class LeaveOperation {
        final Set<ActorRef> neededAcks;
        final Set<ActorRef> receivedAcks = new HashSet<>();

        LeaveOperation(Set<ActorRef> neededAcks) {
            this.neededAcks = neededAcks;
        }
    }


    //--Constructor--
    //TODO coordinator needs to be handled differently
    public StorageNode(int id) {
        this.id = id;
        this.random = new Random(id);
    }

    //--Getters and Setters--
    public Integer getID() {
        return id;
    }

    public SortedMap<Integer, VersionedValue> getDataStore() {
        return dataStore;
    }

    public void setDataStore(SortedMap<Integer, VersionedValue> dataStore) {
        this.dataStore = dataStore;
    }
    
    public SortedMap<Integer,ActorRef> getNodesAlive() {
        return nodeRegistry;
    }

    public void setNodeRegistry(SortedMap<Integer,ActorRef> nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    private void scheduleMessage(ActorRef receiver, ActorRef sender, Object msg) {
        int randomDelay = 50 + random.nextInt(Messages.DELAY);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(randomDelay, TimeUnit.MILLISECONDS),  
            receiver,
            msg, // the message to send
            getContext().system().dispatcher(), 
            sender
        );
    }

    private void scheduleTimeoutMessage(ActorRef ref, Messages.Timeout msg) {            
        getContext().system().scheduler().scheduleOnce(
            Duration.create(TimeoutDelay.getDelayForOperation(msg.opType), TimeUnit.MILLISECONDS),  
            ref,
            msg, // the message to send
            getContext().system().dispatcher(), 
            ref
        );
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
    
    // Replica level
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

    private boolean releaseLock(int key, String operationId) {
        Set<String> locks = keylocks.get(key);
        if (locks != null) {
            locks.remove(operationId);
            if (locks.isEmpty()) {
                keylocks.remove(key);
                keyLockTypes.remove(key);
            }
            return true;
        }
        return false;
    }

    private List<ActorRef> getNextNNodes(int key, SortedMap<Integer,ActorRef> nodeRegistry) {
        List<ActorRef> result = new ArrayList<>();
        
        if (key < 0 || nodeRegistry.isEmpty()) {
            return result;
        }

        // For single node, return self
        if (nodeRegistry.size() == 1) {
            result.add(nodeRegistry.firstEntry().getValue());
            return result;
        }

        // Get all node IDs in sorted order
        List<Integer> nodeIds = new ArrayList<>(nodeRegistry.keySet());
        
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
            result.add(nodeRegistry.get(nodeId));
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
    private void onTimeout(Messages.Timeout msg) {
        
        switch(msg.opType) {
            case CLIENT_GET -> {
                // Removing the operation from the pending ones
                // We only need to remove the operation from the coordinator,
                // beacause the only lock that remains active is there
                if (pendingGet.containsKey(msg.operationId)) {
                    ActorRef client = pendingGet.remove(msg.operationId).client;
                    System.out.printf("[NODE %d] Timeout on GET operation for key (%d)\n", this.id, msg.key);

                    // Notify the client about the timeout
                    scheduleMessage(client, getSelf(), new Messages.Error(msg.key, msg.operationId, OperationType.CLIENT_GET));
                }
            }
            case CLIENT_UPDATE -> {
                // The timeout in the CLIENT_UPDATE operation can only occur when:
                // - Read quorum is not achieved during read-before-update phase
                // - Write quorum is not achieved during update phase
                if (pendingGet.containsKey(msg.operationId) && pendingUpdate.containsKey(msg.operationId)) {
                    // This is a coordinator timeout - clean up pending operations
                    UpdateOperation updateOperation = pendingUpdate.remove(msg.operationId);
                    pendingGet.remove(msg.operationId);
                    
                    System.out.printf("[NODE (Coordinator) %d] Timeout on UPDATE operation for key (%d)\n", this.id, msg.key);
                    
                    // Send timeout notification to all replica nodes to release their locks
                    for (ActorRef ref: getNextNNodes(updateOperation.key, this.nodeRegistry)) {
                        if (ref != getSelf()) {
                            scheduleMessage(ref, getSelf(), new Messages.Timeout(msg.operationId, OperationType.CLIENT_UPDATE, msg.key));
                        }
                    }

                    releaseLock(msg.key, msg.operationId);
                    
                    // Notify the client about the timeout
                    scheduleMessage(updateOperation.client, getSelf(), new Messages.Error(msg.key, msg.operationId, OperationType.CLIENT_UPDATE));
                    
                } else if (releaseLock(msg.key, msg.operationId)) {
                    // This is a replica timeout - just release the lock
                    System.out.printf("[NODE (Replica) %d] Timeout on UPDATE operation for key (%d) - releasing lock\n", this.id, msg.key);
                }
            }
            case JOIN -> {
                //Assumption coordinator gives always active bootstrapping node
                /* Timeout when:
                 * - Nodes that need to be contacted for data items are not active
                 *  - ABORT operation
                 */
                if (this.nodeRegistry.isEmpty())
                    System.out.printf("[NODE %d] Timeout on JOIN (cannot contact neighbor)!\n", this.id);
                else 
                    System.out.printf("[NODE %d] (IGNORATO) timeout per l'operazione JOIN\n", this.id);
            }
            case LEAVE -> {
                //TODO create a ACK message by the node that takes your item, in order to guarantee that you can safely leave
                /* 
                 * If leave operation requires the repartition of data to nodes that are not active (not ACK'ed)
                 * - ABORT operation
                 */
                if (!this.nodeRegistry.isEmpty())
                    System.out.printf("[NODE %d] E io mi offendo e rimango qui!\n", this.id);
                else 
                    System.out.printf("[NODE %d] (IGNORATO) timeout per l'operazione LEAVE\n", this.id);
            }
            default -> {

            }
        }
    }

    private void onJoin(Messages.Join msg) {

        if (!this.nodeRegistry.isEmpty()) {
            System.out.printf("[NODE %d] Cannot JOIN already inside\n", this.id);
            return;
        }
        //TODO in case of timeout ask/change bootstrapping peer
        scheduleMessage(msg.bootstrappingPeer, getSelf(), new Messages.RequestNodeRegistry(UpdateType.JOIN));

        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.JOIN, -1));
    }

    private void onRequestNodeRegistry(Messages.RequestNodeRegistry msg) {
        scheduleMessage(sender(), getSelf(), new Messages.UpdateNodeRegistry(this.nodeRegistry, msg.type));
    }

    private void onRequestDataItems(Messages.RequestDataItems msg) {
        
        // Filter requested Data items for the joining Storage Node
        Map<Integer, VersionedValue> requestedDataItems;
        
        requestedDataItems = new TreeMap<>(this.dataStore);

        scheduleMessage(sender(), getSelf(), new Messages.DataItemsResponse(requestedDataItems, msg.type));
    }

    private void onDataItemsReponse(Messages.DataItemsResponse msg) {

        SortedMap<Integer, ActorRef> futureMap = new TreeMap<>(this.nodeRegistry);
        futureMap.put(this.id, getSelf());

        for (Integer key: msg.dataItems.keySet()) {

            List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
            
            if (msg.type == UpdateType.JOIN) {
                
                if (!getNextNNodes(key, futureMap).contains(getSelf())) continue;

                String operationId = this.id + "-" + key + "-" + (++this.operationCounter);
    
                GetOperation getOperation = new GetOperation(key, getSelf(), GetType.INIT);
                pendingGet.put(operationId, getOperation);
                
                for (ActorRef ref: nNodes) {
                    scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(key, operationId, GetType.INIT));
                }
            } else {
                // Value is added if it's now requested and not present inside the personal dataStore.
                if (nNodes.contains(getSelf()) && !this.dataStore.containsKey(key)) {
                    System.out.printf("[NODE %d] Due to Recovery added (Key -> %d, value -> %s)\n", this.id, key, msg.dataItems.get(key));
                    this.dataStore.put(key, msg.dataItems.get((key)));
                }
            }
        }

        // On Announce was here rip 2025-2025
    }

    // Debug message to print the contents of a node's data store
    private void onDebugPrintDataStore(Messages.DebugPrintDataStore msg) {
        System.out.println("[NODE " + this.id + "] DataStore: " + this.dataStore);
    }

    private void onLeave(Messages.Leave msg) {

        SortedMap<Integer, ActorRef> futureRegistry = new TreeMap<>(this.nodeRegistry);
        futureRegistry.remove(this.id);

        Set<ActorRef> neededAcks = new HashSet<>();
        for (Integer key: this.dataStore.keySet()) {
            neededAcks.addAll(getNextNNodes(key, futureRegistry));
        }

        // System.out.println("Needed Acks: " + neededAcks);
        this.leaveOperation = new LeaveOperation(neededAcks);
        
        for (ActorRef ref: neededAcks) {
            scheduleMessage(ref, getSelf(), new Messages.NotifyLeave());
        }
        
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.LEAVE, -1));
    }

    private void onNotifyLeave(Messages.NotifyLeave msg) {
        scheduleMessage(sender(), getSelf(), new Messages.LeaveACK());
    }
    
    private void onLeaveACK(Messages.LeaveACK msg) {
        this.leaveOperation.receivedAcks.add(sender());

        // We have received all ACK's so we can safely leave and tell other nodes to collect data
        if (this.leaveOperation.receivedAcks.equals(this.leaveOperation.neededAcks)) {
            // System.out.println("Received Acks: " + this.leaveOperation.receivedAcks);
            for (ActorRef ref: this.leaveOperation.receivedAcks) {
                scheduleMessage(ref, getSelf(), new Messages.RepartitionData(this.id, this.dataStore));
            }
            this.nodeRegistry.clear();
            System.out.println("[NODE" + this.id +"] Announces leave");
        }
    }

    private void onRepartitionData(Messages.RepartitionData msg) {
        
        // Remove leaving node from the nodeRegistry
        this.nodeRegistry.remove(msg.leavingId);

        for (Integer key: msg.items.keySet()) {
            List<ActorRef> newOwners = getNextNNodes(key, this.nodeRegistry);

            if (newOwners.contains(getSelf())) {
                VersionedValue givenValue =  msg.items.get(key);
                VersionedValue posessedValue = this.dataStore.get(key);

                if (posessedValue == null || posessedValue.getVersion() < givenValue.getVersion()) {
                    this.dataStore.put(key,givenValue);
                    System.out.println("[NODE" + this.id +"] Due to Node " + msg.leavingId 
                        + ", Inserts (key -> " + key +", value -> " + givenValue + ")");
                }
            }
        }
    }


    private void onCrash(Messages.Crash msg) {
        getContext().become(crashed());
        System.out.println("[NODE " + this.id + "] CRASHED!");
    }

    private void onRecovery(Messages.Recovery msg) {
        getContext().become(createReceive());
        System.out.printf("[NODE %d] Recovering\n", this.id);
        scheduleMessage(msg.recoveryNode, getSelf(), new Messages.RequestNodeRegistry(UpdateType.RECOVERY));
    }

    private void onClientUpdate(Messages.ClientUpdate msg) {
        System.out.println("[UPDATE ITEM] Key: " + msg.key +", Value: " + msg.value);
        
        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        if(isLocked(msg.key, OpType.UPDATE) || isLocked(msg.key, OpType.GET) && acquireLock(msg.key, OpType.UPDATE, operationId)) {
            System.out.println("Coordinator noticed item is locked by a possible read/write");
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_UPDATE));
            return;
        }

        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);

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
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, GetType.UPDATE));
            }
        }
        
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_UPDATE, msg.key));

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
        
        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        if(isLocked(msg.key, OpType.UPDATE) && acquireLock(msg.key, OpType.GET, operationId)) {
            System.out.println("Coordinator noticed item is locked by a possible write");
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_GET));
            return;
        }

        // Generate unique operation ID


        //Calculate read quorum (e.g., majority)
        //TODO make read quorum constant
        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);

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
                // Lock released here because we are sure that the localValue will not be changed by subsequent UPDATE
                releaseLock(msg.key, operationId);
            } else {
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, GetType.GET));
            }
        }

        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_GET, msg.key));

        // Check if quorum already achieved (in case of single node or local-only data)
        checkGetOperation(operationId);
    }
    
    
    private void onReplicaGet(Messages.ReplicaGet msg) {
        // Determine the appropriate lock type
        OpType lockType = (msg.getType == GetType.UPDATE) ? OpType.UPDATE : OpType.GET;
        
        if (!acquireLock(msg.key, lockType, msg.operationId)) {
            System.err.println("[NODE " + this.id + "] Cannot execute get on data " + msg.key + " is locked");
            return;
        }

        VersionedValue requestedValue = this.dataStore.get(msg.key);
        scheduleMessage(sender(), getSelf(), new Messages.GetResponse(msg.key, requestedValue, msg.operationId));
        
        // For regular GET and INIT operations, release lock immediately
        // For UPDATE operations (read-before-update), keep the lock until update completes
        if (msg.getType == GetType.GET || msg.getType == GetType.INIT) {
            releaseLock(msg.key, msg.operationId);
        }
        // Note: For UPDATE operations, the lock will be released in onReplicaUpdate
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

                    scheduleMessage(operation.client, getSelf(), response);
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
                    scheduleMessage(operation.client, getSelf(), response);
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
                    // System.out.println("[NODE " + this.id + "] Inserted Value: " + latestValue);
                    if (!this.nodeRegistry.containsKey(this.id)) {
                        System.out.printf("[NODE %d] Inserted Value: %s, is this value correct? %b\n", this.id, latestValue, getNextNNodes(operation.key, this.nodeRegistry).contains(getSelf()));
                        this.nodeRegistry.put(this.id, getSelf());
                        System.out.println("I have inserted my self inside the nodeRegistry: id ->" + this.id);
                    }
            
                    for (ActorRef ref: nodeRegistry.values()) {
                        if (ref != getSelf()) {
                            scheduleMessage(ref, getSelf(), new Messages.Announce(this.id));
                        } 
                    }
                }
            }
        }
    }

    private void performActualUpdate(int key, String value, String operationId, ActorRef client, VersionedValue currentValue) {
        List<ActorRef> nodeList = getNextNNodes(key, this.nodeRegistry);

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
                scheduleMessage(ref, getSelf(), new Messages.ReplicaUpdate(key, value, operationId, currentValue));
            }
        }

        pendingUpdate.remove(operationId);
        releaseLock(key, operationId);
    }

    

    //TODO for testing
    private void onUpdateNodeRegistry(Messages.UpdateNodeRegistry msg) {
        
        this.nodeRegistry.clear();
        this.nodeRegistry.putAll(msg.nodeRegistry);

        switch(msg.type) {
            case INIT -> this.nodeRegistry.put(this.id, getSelf());
            case RECOVERY -> {
                // stritctly less than key, meaning this node is not included
                SortedMap<Integer, ActorRef> lowerNodes = this.nodeRegistry.headMap(this.id);
                
                // Getting the previous (left) neighbor of the recovering node
                // TODO if contacting node is crashed choose another one
                ActorRef neighbor;
                int index;
                if (lowerNodes.isEmpty()) {
                    index = this.nodeRegistry.lastEntry().getKey();
                    neighbor = this.nodeRegistry.lastEntry().getValue();
                } else {
                    index = lowerNodes.lastEntry().getKey();
                    neighbor = lowerNodes.lastEntry().getValue();
                }

                System.out.printf("[NODE %d] Asking Data Items to NODE %d\n", this.id, index);

                scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, UpdateType.RECOVERY));

                this.dataStore.entrySet().removeIf(entry -> {
                    Integer key = entry.getKey();
                    VersionedValue value = entry.getValue();
                    List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
                    
                    if (!nNodes.contains(getSelf())) {
                        System.out.printf("[NODE %d] Due to Recovery dropped (Key -> %d, value -> %s)\n", 
                                        this.id, key, value);
                        return true; // Remove this entry
                    }
                    return false; // Keep this entry
                });
            }
            case JOIN -> {
                SortedMap<Integer, ActorRef> higherNodes = this.nodeRegistry.tailMap(this.id);
                ActorRef neighbor;


                // TODO if contacting node is crashed choose another one
                int neighborId;
                if (higherNodes.isEmpty()) {
                    neighbor = this.nodeRegistry.firstEntry().getValue();
                    neighborId = this.nodeRegistry.firstKey();
                } else {
                    neighbor = higherNodes.firstEntry().getValue();
                    neighborId = higherNodes.firstKey();
                }
                System.out.printf("[NODE %d] Contact Neighbor %d to request data items\n", this.id, neighborId);
                scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, UpdateType.JOIN));
                
            }
            default -> System.out.println("BOH");
        }

        System.out.println("[NODE REGISTRY UPDATED] Node " + this.id + " now knows about " + this.nodeRegistry.size() + " nodes");
    }

    private void onAnnouce(Messages.Announce msg) {
        this.nodeRegistry.put(msg.announcingId, sender());
        Map<Integer, VersionedValue> requestedDataItems = new TreeMap<>(this.dataStore);

        for(Integer key: requestedDataItems.keySet()) {
            List<ActorRef> shouldPosess = getNextNNodes(key, this.nodeRegistry);
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
            .match(Messages.Leave.class, this::onLeave)
            .match(Messages.NotifyLeave.class, this::onNotifyLeave)
            .match(Messages.LeaveACK.class, this::onLeaveACK)
            .match(Messages.RepartitionData.class, this::onRepartitionData)
            .match(Messages.Crash.class, this::onCrash)
            .match(Messages.Recovery.class, this::onRecovery)
            .match(Messages.ClientUpdate.class, this::onClientUpdate)
            .match(Messages.ReplicaUpdate.class, this::onReplicaUpdate)
            .match(Messages.ClientGet.class, this::onClientGet)
            .match(Messages.ReplicaGet.class, this::onReplicaGet)
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.Timeout.class, this::onTimeout)
            .match(Messages.DebugPrintDataStore.class, this::onDebugPrintDataStore) // For debugging
            .build();
    }
    
    
    public Receive crashed() {
      return receiveBuilder()
              .match(Messages.Recovery.class, this::onRecovery)
              .matchAny(msg -> {})
              .build();
    }
}
