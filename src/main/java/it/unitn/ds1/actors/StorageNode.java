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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

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

    private static final Logger logger = LoggerFactory.getLogger(StorageNode.class);


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
                if (pendingGet.containsKey(msg.operationId)) {
                    ActorRef client = pendingGet.remove(msg.operationId).client;
                    logger.warn("Node {} - Timeout on GET operation for key {}", this.id, msg.key);

                    scheduleMessage(client, getSelf(), new Messages.Error(msg.key, msg.operationId, OperationType.CLIENT_GET));
                }
            }
            case CLIENT_UPDATE -> {
                if (pendingGet.containsKey(msg.operationId) && pendingUpdate.containsKey(msg.operationId)) {
                    UpdateOperation updateOperation = pendingUpdate.remove(msg.operationId);
                    pendingGet.remove(msg.operationId);
                    
                    logger.warn("Node {} (Coordinator) - Timeout on UPDATE operation for key {}", this.id, msg.key);
                    
                    for (ActorRef ref: getNextNNodes(updateOperation.key, this.nodeRegistry)) {
                        if (ref != getSelf()) {
                            scheduleMessage(ref, getSelf(), new Messages.Timeout(msg.operationId, OperationType.CLIENT_UPDATE, msg.key));
                        }
                    }

                    releaseLock(msg.key, msg.operationId);
                    scheduleMessage(updateOperation.client, getSelf(), new Messages.Error(msg.key, msg.operationId, OperationType.CLIENT_UPDATE));
                    
                } else if (releaseLock(msg.key, msg.operationId)) {
                    logger.debug("Node {} (Replica) - Timeout on UPDATE operation for key {} - releasing lock", this.id, msg.key);
                }
            }
            case JOIN -> {
                if (this.nodeRegistry.isEmpty()) {
                    logger.error("Node {} - JOIN timeout: cannot contact neighbor", this.id);
                } else {
                    logger.debug("Node {} - JOIN timeout ignored (already connected)", this.id);
                }
            }
            case LEAVE -> {
                if (!this.nodeRegistry.isEmpty()) {
                    logger.warn("Node {} - LEAVE timeout: staying in cluster", this.id);
                } else {
                    logger.debug("Node {} - LEAVE timeout ignored", this.id);
                }
            }
            default -> {
                logger.debug("Node {} - Unhandled timeout for operation type {}", this.id, msg.opType);
            }
        }
    }

    private void onJoin(Messages.Join msg) {
        if (!this.nodeRegistry.isEmpty()) {
            logger.warn("Node {} - Cannot JOIN: already inside cluster", this.id);
            return;
        }
        
        logger.info("Node {} - Initiating JOIN operation", this.id);
        scheduleMessage(msg.bootstrappingPeer, getSelf(), new Messages.RequestNodeRegistry(UpdateType.JOIN));
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.JOIN, -1));
    }

    private void onRequestNodeRegistry(Messages.RequestNodeRegistry msg) {
        logger.debug("Node {} - Received node registry request of type {}", this.id, msg.type);
        scheduleMessage(sender(), getSelf(), new Messages.UpdateNodeRegistry(this.nodeRegistry, msg.type));
    }

    private void onRequestDataItems(Messages.RequestDataItems msg) {
        logger.debug("Node {} - Received data items request from node {}", this.id, msg.askingID);
        Map<Integer, VersionedValue> requestedDataItems = new TreeMap<>(this.dataStore);
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
                
                logger.debug("Node {} - Requesting initial data for key {} during JOIN", this.id, key);
                for (ActorRef ref: nNodes) {
                    scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(key, operationId, GetType.INIT));
                }
            } else {
                if (nNodes.contains(getSelf()) && !this.dataStore.containsKey(key)) {
                    logger.info("Node {} - Recovery: added key {} with value {}", this.id, key, msg.dataItems.get(key));
                    this.dataStore.put(key, msg.dataItems.get((key)));
                }
            }
        }
    }

    // Debug message to print the contents of a node's data store
    private void onDebugPrintDataStore(Messages.DebugPrintDataStore msg) {
        logger.info("Node {} DataStore: {}", this.id, this.dataStore);
    }


    private void onLeave(Messages.Leave msg) {
        logger.info("Node {} - Initiating LEAVE operation", this.id);
        
        SortedMap<Integer, ActorRef> futureRegistry = new TreeMap<>(this.nodeRegistry);
        futureRegistry.remove(this.id);

        Set<ActorRef> neededAcks = new HashSet<>();
        for (Integer key: this.dataStore.keySet()) {
            neededAcks.addAll(getNextNNodes(key, futureRegistry));
        }

        logger.debug("Node {} - Waiting for {} ACKs to leave", this.id, neededAcks.size());
        this.leaveOperation = new LeaveOperation(neededAcks);
        
        for (ActorRef ref: neededAcks) {
            scheduleMessage(ref, getSelf(), new Messages.NotifyLeave());
        }
        
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.LEAVE, -1));
    }

    private void onNotifyLeave(Messages.NotifyLeave msg) {
        logger.debug("Node {} - Received leave notification, sending ACK", this.id);
        scheduleMessage(sender(), getSelf(), new Messages.LeaveACK());
    }
    
    private void onLeaveACK(Messages.LeaveACK msg) {
        this.leaveOperation.receivedAcks.add(sender());

        if (this.leaveOperation.receivedAcks.equals(this.leaveOperation.neededAcks)) {
            logger.info("Node {} - All ACKs received, proceeding with leave", this.id);
            for (ActorRef ref: this.leaveOperation.receivedAcks) {
                scheduleMessage(ref, getSelf(), new Messages.RepartitionData(this.id, this.dataStore));
            }
            this.nodeRegistry.clear();
            logger.info("Node {} - Successfully left the cluster", this.id);
        }
    }

    private void onRepartitionData(Messages.RepartitionData msg) {
        logger.info("Node {} - Processing data repartition from leaving node {}", this.id, msg.leavingId);
        this.nodeRegistry.remove(msg.leavingId);

        int itemsReceived = 0;
        for (Integer key: msg.items.keySet()) {
            List<ActorRef> newOwners = getNextNNodes(key, this.nodeRegistry);

            if (newOwners.contains(getSelf())) {
                VersionedValue givenValue = msg.items.get(key);
                VersionedValue posessedValue = this.dataStore.get(key);

                if (posessedValue == null || posessedValue.getVersion() < givenValue.getVersion()) {
                    this.dataStore.put(key, givenValue);
                    itemsReceived++;
                    logger.debug("Node {} - Received key {} with value {} from leaving node {}", 
                                    this.id, key, givenValue, msg.leavingId);
                }
            }
        }
        
        if (itemsReceived > 0) {
            logger.info("Node {} - Successfully received {} data items from leaving node {}", 
                       this.id, itemsReceived, msg.leavingId);
        }
    }


    private void onCrash(Messages.Crash msg) {
        getContext().become(crashed());
        logger.error("Node {} - CRASHED!", this.id);
    }

    private void onRecovery(Messages.Recovery msg) {
        getContext().become(createReceive());
        logger.info("Node {} - Recovering from crash", this.id);
        scheduleMessage(msg.recoveryNode, getSelf(), new Messages.RequestNodeRegistry(UpdateType.RECOVERY));
    }

    private void onClientUpdate(Messages.ClientUpdate msg) {
        logger.info("Node {} - UPDATE request: key={}, value={}", this.id, msg.key, msg.value);
        
        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        // Check coordinator-level locks first
        if(isLocked(msg.key, OpType.UPDATE) || isLocked(msg.key, OpType.GET)) {
            logger.warn("Node {} - UPDATE rejected: key {} is locked at coordinato level", this.id, msg.key);
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_UPDATE));
            return;
        }

        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);
        
        // Only acquire replica-level lock if this node is part of the replicas
        if (nodeList.contains(getSelf())) {
            if (!acquireLock(msg.key, OpType.UPDATE, operationId)) {
                logger.warn("Node {} - UPDATE rejected: key {} is locked at replica level", this.id, msg.key);
                scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_UPDATE));
                return;
            }
        }

        GetOperation getOperation = new GetOperation(msg.key, sender(), msg.value);
        pendingGet.put(operationId, getOperation);
        UpdateOperation updateOperation = new UpdateOperation(msg.key, sender());
        pendingUpdate.put(operationId, updateOperation);

        logger.debug("Node {} - Starting read-before-update for key {}", this.id, msg.key);
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = this.dataStore.get(msg.key);
                getOperation.responses.add(localValue);
                logger.debug("Node {} - Local read for update: key={}, value={}", this.id, msg.key, localValue);
            } else {
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, GetType.UPDATE));
            }
        }
        
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_UPDATE, msg.key));
        checkGetOperation(operationId);
    }

    private void onReplicaUpdate(Messages.ReplicaUpdate msg) {
        VersionedValue result = updateWithCurrentValue(msg.key, msg.value, msg.currentValue);
        logger.debug("Node {} - Replica update completed: key={}, new_value={}", this.id, msg.key, result);
        releaseLock(msg.key, msg.operationId);
    }


    private void onClientGet(Messages.ClientGet msg) {
        logger.info("Node {} - GET request: key={}", this.id, msg.key);
        
        String operationId = this.id + "-" + msg.key + "-" + (++operationCounter);

        if(isLocked(msg.key, OpType.UPDATE)) {
            logger.warn("Node {} - GET rejected: key {} is locked for update at coordinator level", this.id, msg.key);
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_GET));
            return;
        }

        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);
        
        // Only acquire replica-level lock if this node is part of the replicas
        if (nodeList.contains(getSelf())) {
            if (!acquireLock(msg.key, OpType.GET, operationId)) {
                logger.warn("Node {} - GET rejected: key {} is locked at replica level", this.id, msg.key);
                scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_GET));
                return;
            }
        }

        GetOperation operation = new GetOperation(msg.key, sender(), GetType.GET);
        pendingGet.put(operationId, operation);
    
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = this.dataStore.get(msg.key);
                operation.responses.add(localValue);
                logger.debug("Node {} - Local GET: key={}, value={}", this.id, msg.key, localValue);
                releaseLock(msg.key, operationId);
            } else {
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, GetType.GET));
            }
        }

        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_GET, msg.key));
        checkGetOperation(operationId);
    }
    
    
    private void onReplicaGet(Messages.ReplicaGet msg) {
        OpType lockType = (msg.getType == GetType.UPDATE) ? OpType.UPDATE : OpType.GET;
        
        if (!acquireLock(msg.key, lockType, msg.operationId)) {
            logger.warn("Node {} - Cannot execute {} on key {}: locked", this.id, msg.getType, msg.key);
            return;
        }

        VersionedValue requestedValue = this.dataStore.get(msg.key);
        logger.debug("Node {} - Replica GET: key={}, value={}, type={}", this.id, msg.key, requestedValue, msg.getType);
        scheduleMessage(sender(), getSelf(), new Messages.GetResponse(msg.key, requestedValue, msg.operationId));
        
        if (msg.getType == GetType.GET || msg.getType == GetType.INIT) {
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

                    scheduleMessage(operation.client, getSelf(), response);
                    pendingGet.remove(operationId);
                    logger.info("Node {} - UPDATE completed: key={}, new_version={}", this.id, operation.key, 
                              response.versionedValue.getVersion());
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
                    
                    Messages.GetResponse response = new Messages.GetResponse(operation.key, latestValue, operationId);
                    scheduleMessage(operation.client, getSelf(), response);
                    pendingGet.remove(operationId);
                    logger.info("Node {} - GET completed: key={}, value={}", this.id, operation.key, latestValue);
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
                    
                    if (!this.nodeRegistry.containsKey(this.id)) {
                        logger.info("Node {} - JOIN completed: inserted {} data items", this.id, this.dataStore.size());
                        this.nodeRegistry.put(this.id, getSelf());
                        
                        for (ActorRef ref: nodeRegistry.values()) {
                            if (ref != getSelf()) {
                                scheduleMessage(ref, getSelf(), new Messages.Announce(this.id));
                            } 
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
            logger.error("Node {} - UpdateOperation not found for operationId: {}", this.id, operationId);
            return;
        }

        logger.debug("Node {} - Executing actual update for key {}", this.id, key);
        for (ActorRef ref: nodeList) {
            if (ref == getSelf()) {
                VersionedValue versionedValue = updateWithCurrentValue(key, value, currentValue);
                operation.responses.add(versionedValue);
                logger.debug("Node {} - Local update completed: key={}, value={}", this.id, key, versionedValue);
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
            case INIT -> {
                this.nodeRegistry.put(this.id, getSelf());
                logger.info("Node {} - Node registry initialized with {} nodes", this.id, this.nodeRegistry.size());
            }
            case RECOVERY -> {
                SortedMap<Integer, ActorRef> lowerNodes = this.nodeRegistry.headMap(this.id);
                
                ActorRef neighbor;
                int neighborId;
                if (lowerNodes.isEmpty()) {
                    neighborId = this.nodeRegistry.lastEntry().getKey();
                    neighbor = this.nodeRegistry.lastEntry().getValue();
                } else {
                    neighborId = lowerNodes.lastEntry().getKey();
                    neighbor = lowerNodes.lastEntry().getValue();
                }

                logger.info("Node {} - Recovery: requesting data items from node {}", this.id, neighborId);
                scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, UpdateType.RECOVERY));

                int droppedItems = 0;
                this.dataStore.entrySet().removeIf(entry -> {
                    Integer key = entry.getKey();
                    VersionedValue value = entry.getValue();
                    List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
                    
                    if (!nNodes.contains(getSelf())) {
                        logger.debug("Node {} - Recovery: dropped key {} (value: {})", this.id, key, value);
                        return true;
                    }
                    return false;
                });
                
                if (droppedItems > 0) {
                    logger.info("Node {} - Recovery: dropped {} obsolete data items", this.id, droppedItems);
                }
            }
            case JOIN -> {
                SortedMap<Integer, ActorRef> higherNodes = this.nodeRegistry.tailMap(this.id);
                ActorRef neighbor;
                int neighborId;
                
                if (higherNodes.isEmpty()) {
                    neighbor = this.nodeRegistry.firstEntry().getValue();
                    neighborId = this.nodeRegistry.firstKey();
                } else {
                    neighbor = higherNodes.firstEntry().getValue();
                    neighborId = higherNodes.firstKey();
                }
                
                logger.info("Node {} - JOIN: requesting data items from node {}", this.id, neighborId);
                scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, UpdateType.JOIN));
            }
            default -> logger.warn("Node {} - Unknown registry update type: {}", this.id, msg.type);
        }

        logger.info("Node {} - Registry updated: now tracking {} nodes", this.id, this.nodeRegistry.size());
    }

    private void onAnnouce(Messages.Announce msg) {
        logger.info("Node {} - Node {} announced join", this.id, msg.announcingId);
        this.nodeRegistry.put(msg.announcingId, sender());
        Map<Integer, VersionedValue> requestedDataItems = new TreeMap<>(this.dataStore);

        int itemsDropped = 0;
        for(Integer key: requestedDataItems.keySet()) {
            List<ActorRef> shouldPosess = getNextNNodes(key, this.nodeRegistry);
            if (!shouldPosess.contains(getSelf())) {
                VersionedValue itemToRemove = this.dataStore.get(key); 
                this.dataStore.remove(key);
                itemsDropped++;
                logger.debug("Node {} - Dropped key {} (value: {}) due to node {} join", 
                                this.id, key, itemToRemove, msg.announcingId);
            }
        }
        
        if (itemsDropped > 0) {
            logger.info("Node {} - Dropped {} data items due to node {} join", this.id, itemsDropped, msg.announcingId);
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
