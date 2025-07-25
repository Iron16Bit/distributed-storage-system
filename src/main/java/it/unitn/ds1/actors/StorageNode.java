package it.unitn.ds1.actors;

import scala.concurrent.duration.Duration;

import java.rmi.server.Operation;
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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.TimeoutDelay;
import it.unitn.ds1.utils.VersionedValue;

/**
 * Storage node implementation for the distributed key-value storage system.
 * Handles data storage, replication, and coordination of distributed operations.
 */
public class StorageNode extends AbstractActor {
    
    private static final Logger logger = LoggerFactory.getLogger(StorageNode.class);
    
    // Node identification and randomization
    private final Integer id;
    private final Random random;

    // Core data structures
    private final SortedMap<Integer, VersionedValue> dataStore = new TreeMap<>();
    private final SortedMap<Integer, ActorRef> nodeRegistry = new TreeMap<>();
    
    // Operation tracking
    private final Map<String, GetOperation> pendingGet = new HashMap<>();
    private final Map<String, UpdateOperation> pendingUpdate = new HashMap<>();
    private int operationCounter = 0;

    // Locking mechanism
    private final Map<Integer, Set<String>> keylocks = new HashMap<>();
    private final Map<Integer, OperationType> keyLockTypes = new HashMap<>();

    // Leave operation state
    private LeaveOperation leaveOperation;

    // DataStore Manager
    private final DataStoreManager dataStoreManager = DataStoreManager.getInstance();
    
    // ===== NESTED CLASSES =====
    
    private static class GetOperation {
        final int key;
        final ActorRef client;
        final List<VersionedValue> responses;
        final String updateValue;
        final OperationType operationType;

        GetOperation(int key, ActorRef client, OperationType operationType) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
            this.updateValue = null;
            this.operationType = operationType;
        }

        GetOperation(int key, ActorRef client, String updateValue) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
            this.updateValue = updateValue;
            this.operationType = OperationType.CLIENT_UPDATE;
        }
    }

    private static class UpdateOperation {
        final int key;
        final ActorRef client;
        final List<VersionedValue> responses;

        UpdateOperation(int key, ActorRef client) {
            this.key = key;
            this.client = client;
            this.responses = new ArrayList<>();
        }
    }

    private static class LeaveOperation {
        final Set<ActorRef> neededAcks;
        final Set<ActorRef> receivedAcks = new HashSet<>();

        LeaveOperation(Set<ActorRef> neededAcks) {
            this.neededAcks = neededAcks;
        }
    }

    // ===== CONSTRUCTOR =====
    
    public StorageNode(int id) {
        this.id = id;
        this.random = new Random(id);
    }

    // ===== AKKA LIFECYCLE =====
    
    public static Props props(int id) {
        return Props.create(StorageNode.class, () -> new StorageNode(id));
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getContext().become(initialSpawn());
    }

    // ===== BEHAVIOR DEFINITIONS =====
    
    public Receive initialSpawn() {
        return receiveBuilder()
            .match(Messages.Join.class, this::onJoin)
            .match(Messages.UpdateNodeRegistry.class, this::onUpdateNodeRegistry)
            .matchAny(msg -> logger.debug("Node {} - Ignoring message in initialSpawn state", this.id))
            .build();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Messages.RequestNodeRegistry.class, this::onRequestNodeRegistry)
            .match(Messages.RequestDataItems.class, this::onRequestDataItems)
            .match(Messages.DataItemsResponse.class, this::onDataItemsResponse)
            .match(Messages.Announce.class, this::onAnnounce)
            .match(Messages.UpdateNodeRegistry.class, this::onUpdateNodeRegistry)
            .match(Messages.Leave.class, this::onLeave)
            .match(Messages.NotifyLeave.class, this::onNotifyLeave)
            .match(Messages.LeaveACK.class, this::onLeaveACK)
            .match(Messages.RepartitionData.class, this::onRepartitionData)
            .match(Messages.Crash.class, this::onCrash)
            .match(Messages.ClientUpdate.class, this::onClientUpdate)
            .match(Messages.ReplicaUpdate.class, this::onReplicaUpdate)
            .match(Messages.ClientGet.class, this::onClientGet)
            .match(Messages.ReplicaGet.class, this::onReplicaGet)
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.Timeout.class, this::onTimeout)
            .match(Messages.DebugPrintDataStore.class, this::onDebugPrintDataStore)
            .matchAny(msg -> logger.warn("Node {} - Unhandled message: {}", this.id, msg.getClass().getSimpleName()))
            .build();
    }

    public Receive crashed() {
        return receiveBuilder()
            .match(Messages.Recovery.class, this::onRecovery)
            .matchAny(msg -> logger.debug("Node {} - Ignoring message while crashed", this.id))
            .build();
    }

    // ===== UTILITY METHODS =====
    
    private void scheduleMessage(ActorRef receiver, ActorRef sender, Object msg) {
        int randomDelay = 50 + random.nextInt(Messages.DELAY);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(randomDelay, TimeUnit.MILLISECONDS),
            receiver, msg,
            getContext().system().dispatcher(), sender
        );
    }

    private void scheduleTimeoutMessage(ActorRef ref, Messages.Timeout msg) {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(TimeoutDelay.getDelayForOperation(msg.operationType), TimeUnit.MILLISECONDS),
            ref, msg,
            getContext().system().dispatcher(), ref
        );
    }

    // ===== CONSISTENT HASHING =====
    
    private List<ActorRef> getNextNNodes(int key, SortedMap<Integer, ActorRef> nodeRegistry) {
        List<ActorRef> result = new ArrayList<>();
        
        if (key < 0 || nodeRegistry.isEmpty()) {
            return result;
        }

        if (nodeRegistry.size() == 1) {
            result.add(nodeRegistry.firstEntry().getValue());
            return result;
        }

        List<Integer> nodeIds = new ArrayList<>(nodeRegistry.keySet());
        
        // Find the first node with ID >= key (consistent hashing)
        int startIndex = 0;
        for (int i = 0; i < nodeIds.size(); i++) {
            if (nodeIds.get(i) >= key) {
                startIndex = i;
                break;
            }
        }

        // Add N nodes starting from startIndex (with wrap-around)
        // logger.info("DataStoreManager: {}", this.dataStoreManager);
        for (int i = 0; i < this.dataStoreManager.N && i < nodeIds.size(); i++) {
            int index = (startIndex + i) % nodeIds.size();
            Integer nodeId = nodeIds.get(index);
            result.add(nodeRegistry.get(nodeId));
        }

        return result;
    }

    // ===== LOCKING MECHANISMS =====
    
    /**
     * Check if a key is locked at the coordinator level
     */
    private boolean isLocked(int key, OperationType operationType) {
        String pattern = "-" + key + "-";
        
        return switch (operationType) {
            case CLIENT_GET -> pendingUpdate.keySet().stream()
                    .anyMatch(operationId -> operationId.contains(pattern));
            
            case CLIENT_UPDATE -> pendingUpdate.keySet().stream()
                    .anyMatch(operationId -> operationId.contains(pattern)) ||
                    pendingGet.keySet().stream()
                    .anyMatch(operationId -> operationId.contains(pattern));
            default -> {
                logger.error("Node {} - checking for lock with wrong type", this.id);
                yield false;
            }
        };
    }
    
    /**
     * Acquire a lock at the replica level
     */
    private boolean acquireLock(int key, OperationType operationType, String operationId) {
        Set<String> locks = keylocks.computeIfAbsent(key, k -> new HashSet<>());
        OperationType currentLockType = keyLockTypes.get(key);

        if (locks.isEmpty()) {
            locks.add(operationId);
            keyLockTypes.put(key, operationType);
            return true;
        }

        if (currentLockType == OperationType.CLIENT_GET && operationType == OperationType.CLIENT_GET) {
            locks.add(operationId);
            return true;
        }

        return false; // Write operation or mixed read/write => deny
    }
    
    /**
     * Release a lock at the replica level
     */
    private boolean releaseLock(int key, String operationId) {
        Set<String> locks = keylocks.get(key);
        logger.debug("Node {} - releaseLock for operation id {}, current state of locks {}", this.id, operationId, this.keylocks);
        if (locks != null) {
            locks.remove(operationId);
            if (locks.isEmpty()) {
                keylocks.remove(key);
                keyLockTypes.remove(key);
            }
            logger.debug("Node {} - after removal of operation id {}, current state of locks {}", this.id, operationId, this.keylocks);
            return true;
        }
        return false;
    }

    // ===== DATA OPERATIONS =====
    
    private VersionedValue updateWithCurrentValue(int key, String value, VersionedValue currentValue) {
        VersionedValue newVersionedValue = (currentValue == null) 
            ? new VersionedValue(value, 1)
            : new VersionedValue(value, currentValue.getVersion() + 1);

        dataStore.put(key, newVersionedValue);
        return newVersionedValue;
    }

    private VersionedValue get(int key) {
        return dataStore.get(key);
    }

    // ===== MESSAGE HANDLERS - LIFECYCLE =====
    
    private void onJoin(Messages.Join msg) {
        getContext().become(createReceive());
        logger.info("Node {} - Initiating JOIN operation", this.id);
        scheduleMessage(msg.bootstrappingPeer, getSelf(), new Messages.RequestNodeRegistry(OperationType.JOIN));
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.JOIN, -1));
    }

    private void onLeave(Messages.Leave msg) {
        logger.info("Node {} - Initiating LEAVE operation", this.id);
        
        SortedMap<Integer, ActorRef> futureRegistry = new TreeMap<>(this.nodeRegistry);
        futureRegistry.remove(this.id);

        // Cannot Leave if it violates constraints on Replication Factor
        if (futureRegistry.size() < dataStoreManager.N) {
            logger.error("Node {} - Abort LEAVE operation -> violating Replication Factor!", this.id);
            return;
        }

        Set<ActorRef> neededAcks = new HashSet<>();
        for (Integer key : this.dataStore.keySet()) {
            neededAcks.addAll(getNextNNodes(key, futureRegistry));
        }

        logger.debug("Node {} - Waiting for {} ACKs to leave", this.id, neededAcks.size());
        this.leaveOperation = new LeaveOperation(neededAcks);
        
        if (neededAcks.isEmpty()) {
            logger.info("Node {} - No ACKs needed, proceeding with leave", this.id);
            for (ActorRef ref : this.nodeRegistry.values()) {
                scheduleMessage(ref, getSelf(), new Messages.RepartitionData(this.id, this.dataStore));
            }
            this.nodeRegistry.clear();
            getContext().become(initialSpawn());
            logger.info("Node {} - Successfully left the cluster", this.id);
            // Here the program should exit!
        } else {
            for (ActorRef ref : neededAcks) {
                scheduleMessage(ref, getSelf(), new Messages.NotifyLeave());
            }
            
            scheduleTimeoutMessage(getSelf(), new Messages.Timeout(null, OperationType.LEAVE, -1));
        }
    }

    private void onNotifyLeave(Messages.NotifyLeave msg) {
        logger.debug("Node {} - Received leave notification, sending ACK", this.id);
        scheduleMessage(sender(), getSelf(), new Messages.LeaveACK());
    }
    
    private void onLeaveACK(Messages.LeaveACK msg) {
        if (leaveOperation == null) return;
        
        leaveOperation.receivedAcks.add(sender());

        if (leaveOperation.receivedAcks.equals(leaveOperation.neededAcks)) {
            logger.info("Node {} - All ACKs received, proceeding with leave", this.id);
            for (ActorRef ref : this.nodeRegistry.values()) {
                scheduleMessage(ref, getSelf(), new Messages.RepartitionData(this.id, this.dataStore));
            }
            this.nodeRegistry.clear();
            getContext().become(initialSpawn());
            logger.info("Node {} - Successfully left the cluster", this.id);
        }
    }

    private void onCrash(Messages.Crash msg) {
        getContext().become(crashed());
        logger.error("Node {} - CRASHED!", this.id);
    }

    private void onRecovery(Messages.Recovery msg) {
        getContext().become(createReceive());
        logger.info("Node {} - Recovering from crash", this.id);
        scheduleMessage(msg.recoveryNode, getSelf(), new Messages.RequestNodeRegistry(OperationType.RECOVERY));
    }

    // ===== MESSAGE HANDLERS - REGISTRY MANAGEMENT =====
    
    private void onRequestNodeRegistry(Messages.RequestNodeRegistry msg) {
        logger.debug("Node {} - Received node registry request of type {}", this.id, msg.operationType);
        scheduleMessage(sender(), getSelf(), new Messages.UpdateNodeRegistry(this.nodeRegistry, msg.operationType));
    }

    private void onUpdateNodeRegistry(Messages.UpdateNodeRegistry msg) {
        
        // Can't have a dupliKate
        if (msg.nodeRegistry.containsKey(this.id) && msg.operationType == OperationType.JOIN) {
            logger.error("Node {}({}) - ABORT JOIN -> ID already in use by Node {}({})!", this.id,this.getSelf().path().name(),this.id, msg.nodeRegistry.get(this.id).path().name());
            getContext().become(initialSpawn());
            return;
        }
        
        this.nodeRegistry.clear();
        this.nodeRegistry.putAll(msg.nodeRegistry);

        switch (msg.operationType) {
            case INIT -> handleInitUpdate();
            case RECOVERY -> handleRecoveryUpdate();
            case JOIN -> handleJoinUpdate();
            default -> logger.warn("Node {} - Unknown registry update type: {}", this.id, msg.operationType);
        }

        logger.info("Node {} - Registry updated: now tracking {} nodes", this.id, this.nodeRegistry.size());
    }

    private void handleInitUpdate() {
        this.nodeRegistry.put(this.id, getSelf());
        logger.info("Node {} - Node registry initialized with {} nodes", this.id, this.nodeRegistry.size());
        getContext().become(createReceive());
    }

    private void handleRecoveryUpdate() {
        ActorRef neighbor = findNeighborForRecovery();
        if (neighbor != null) {
            logger.info("Node {} - Recovery: requesting data items from neighbor", this.id);
            scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, OperationType.RECOVERY));
        }
        cleanupObsoleteData();
    }

    private void handleJoinUpdate() {
        ActorRef neighbor = findNeighborForJoin();
        if (neighbor != null) {
            logger.info("Node {} - JOIN: requesting data items from neighbor", this.id);
            scheduleMessage(neighbor, getSelf(), new Messages.RequestDataItems(this.id, OperationType.JOIN));
        }
    }

    private ActorRef findNeighborForRecovery() {
        SortedMap<Integer, ActorRef> lowerNodes = this.nodeRegistry.headMap(this.id);
        return lowerNodes.isEmpty() 
            ? this.nodeRegistry.lastEntry().getValue()
            : lowerNodes.lastEntry().getValue();
    }

    private ActorRef findNeighborForJoin() {
        SortedMap<Integer, ActorRef> higherNodes = this.nodeRegistry.tailMap(this.id);
        return higherNodes.isEmpty()
            ? this.nodeRegistry.firstEntry().getValue()
            : higherNodes.firstEntry().getValue();
    }

    private void cleanupObsoleteData() {
        int droppedItems = 0;
        var iterator = this.dataStore.entrySet().iterator();
        
        while (iterator.hasNext()) {
            var entry = iterator.next();
            Integer key = entry.getKey();
            VersionedValue value = entry.getValue();
            List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
            
            if (!nNodes.contains(getSelf())) {
                iterator.remove();
                droppedItems++;
                logger.debug("Node {} - Recovery: dropped key {} (value: {})", this.id, key, value);
            }
        }
        
        if (droppedItems > 0) {
            logger.info("Node {} - Recovery: dropped {} obsolete data items", this.id, droppedItems);
        }
    }

    private void onAnnounce(Messages.Announce msg) {
        logger.info("Node {} - Node {} announced join", this.id, msg.announcingId);
        this.nodeRegistry.put(msg.announcingId, sender());
        
        int itemsDropped = cleanupDataAfterJoin();
        
        if (itemsDropped > 0) {
            logger.info("Node {} - Dropped {} data items due to node {} join", this.id, itemsDropped, msg.announcingId);
        }
    }

    private int cleanupDataAfterJoin() {
        Map<Integer, VersionedValue> itemsToCheck = new TreeMap<>(this.dataStore);
        int itemsDropped = 0;
        
        for (Integer key : itemsToCheck.keySet()) {
            List<ActorRef> shouldPossess = getNextNNodes(key, this.nodeRegistry);
            if (!shouldPossess.contains(getSelf())) {
                VersionedValue itemToRemove = this.dataStore.remove(key);
                itemsDropped++;
                logger.debug("Node {} - Dropped key {} (value: {}) due to topology change", 
                           this.id, key, itemToRemove);
            }
        }
        
        return itemsDropped;
    }

    // ===== MESSAGE HANDLERS - DATA OPERATIONS =====
    
    private void onRequestDataItems(Messages.RequestDataItems msg) {
        logger.debug("Node {} - Received data items request from node {}", this.id, msg.askingID);
        Map<Integer, VersionedValue> requestedDataItems = new TreeMap<>(this.dataStore);
        scheduleMessage(sender(), getSelf(), new Messages.DataItemsResponse(requestedDataItems, msg.operationType));
    }

    private void onDataItemsResponse(Messages.DataItemsResponse msg) {
        SortedMap<Integer, ActorRef> futureMap = new TreeMap<>(this.nodeRegistry);
        futureMap.put(this.id, getSelf());

        boolean hasAskedItems = false;

        for (Integer key : msg.dataItems.keySet()) {
            if (msg.operationType == OperationType.JOIN && getNextNNodes(key, futureMap).contains(getSelf())) {
                handleJoinDataItem(key, futureMap);
                hasAskedItems = true;
            } else {
                handleRecoveryDataItem(key, msg.dataItems.get(key));
            }
        }

        // Need this because if no data is required for the joining node
        // we did not add it to its nodeRegistry.
        if (msg.operationType == OperationType.JOIN) {
            this.nodeRegistry.put(this.id, getSelf());
            if (!hasAskedItems) {
                for (ActorRef ref : nodeRegistry.values()) {
                    if (ref != getSelf()) {
                        scheduleMessage(ref, getSelf(), new Messages.Announce(this.id));
                    } 
                }
            }
        }
    }

    private void handleJoinDataItem(Integer key, SortedMap<Integer, ActorRef> futureMap) {
        if (!getNextNNodes(key, futureMap).contains(getSelf())) {
            return;
        }

        String operationId = this.id + "-" + key + "-" + (++this.operationCounter);
        GetOperation getOperation = new GetOperation(key, getSelf(), OperationType.JOIN);
        pendingGet.put(operationId, getOperation);
        
        logger.debug("Node {} - Requesting initial data for key {} during JOIN", this.id, key);
        List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
        for (ActorRef ref : nNodes) {
            scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(key, operationId, OperationType.JOIN));
        }
    }

    private void handleRecoveryDataItem(Integer key, VersionedValue value) {
        List<ActorRef> nNodes = getNextNNodes(key, this.nodeRegistry);
        if (nNodes.contains(getSelf()) && !this.dataStore.containsKey(key)) {
            logger.info("Node {} - Recovery: added key {} with value {}", this.id, key, value);
            this.dataStore.put(key, value);
        }
    }

    private void onRepartitionData(Messages.RepartitionData msg) {
        logger.info("Node {} - Processing data repartition from leaving node {}", this.id, msg.leavingId);
        this.nodeRegistry.remove(msg.leavingId);

        int itemsReceived = 0;
        for (Map.Entry<Integer, VersionedValue> entry : msg.items.entrySet()) {
            Integer key = entry.getKey();
            VersionedValue givenValue = entry.getValue();
            
            List<ActorRef> newOwners = getNextNNodes(key, this.nodeRegistry);

            if (newOwners.contains(getSelf())) {
                VersionedValue possessedValue = get(key);

                if (possessedValue == null || possessedValue.getVersion() < givenValue.getVersion()) {
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

    // ===== MESSAGE HANDLERS - CLIENT OPERATIONS =====
    
    private void onClientUpdate(Messages.ClientUpdate msg) {
        logger.info("Node {} - UPDATE request: key={}, value={}", this.id, msg.key, msg.value);

        String operationId = generateOperationId(msg.key);
        
        if (msg.key < 0) {
            logger.error("Node {} - ABORT UPDATE -> keys cannot be negative!", this.id);
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_UPDATE));
            return;
        }

        if (isOperationBlocked(msg.key, OperationType.CLIENT_UPDATE, operationId)) {
            return;
        }

        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);
        
        if (!tryAcquireReplicaLock(msg.key, OperationType.CLIENT_UPDATE, operationId, nodeList)) {
            return;
        }

        startReadBeforeUpdate(msg, operationId, nodeList);
    }

    private void onClientGet(Messages.ClientGet msg) {
        logger.info("Node {} - GET request: key={}", this.id, msg.key);
        
        String operationId = generateOperationId(msg.key);

        if (msg.key < 0) {
            logger.error("Node {} - ABORT GET -> keys cannot be negative!", this.id);
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_GET));
            return;
        }

        if (isLocked(msg.key, OperationType.CLIENT_GET)) {
            logger.warn("Node {} - GET rejected: key {} is locked for update at coordinator level", this.id, msg.key);
            scheduleMessage(sender(), getSelf(), new Messages.Error(msg.key, operationId, OperationType.CLIENT_GET));
            return;
        }

        List<ActorRef> nodeList = getNextNNodes(msg.key, this.nodeRegistry);

        
        if (!tryAcquireReplicaLock(msg.key, OperationType.CLIENT_GET, operationId, nodeList)) {
            return;
        }

        startGetOperation(msg, operationId, nodeList);
    }

    private String generateOperationId(int key) {
        return this.id + "-" + key + "-" + (++operationCounter);
    }

    private boolean isOperationBlocked(int key, OperationType operationType, String operationId) {
        if (isLocked(key, operationType)) {
            logger.warn("Node {} - {} rejected: key {} is locked at coordinator level", 
                       this.id, operationType, key);
            scheduleMessage(sender(), getSelf(), new Messages.Error(key, operationId, operationType));
            return true;
        }
        return false;
    }

    private boolean tryAcquireReplicaLock(int key, OperationType operationType, String operationId, List<ActorRef> nodeList) {
        if (nodeList.contains(getSelf()) && !acquireLock(key, operationType, operationId)) {
            logger.warn("Node {} - {} rejected: key {} is locked at replica level", this.id, operationType, key);
            logger.debug("Node {} - Replica Lock State [locks -> {}, types -> {}]", this.id, this.keylocks, this.keyLockTypes);
            scheduleMessage(sender(), getSelf(), new Messages.Error(key, operationId, operationType));
            return false;
        }
        return true;
    }

    private void startReadBeforeUpdate(Messages.ClientUpdate msg, String operationId, List<ActorRef> nodeList) {
        GetOperation getOperation = new GetOperation(msg.key, sender(), msg.value);
        pendingGet.put(operationId, getOperation);
        UpdateOperation updateOperation = new UpdateOperation(msg.key, sender());
        pendingUpdate.put(operationId, updateOperation);

        logger.debug("Node {} - Starting read-before-update for key {}", this.id, msg.key);
        for (ActorRef ref : nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = get(msg.key);
                getOperation.responses.add(localValue);
                logger.debug("Node {} - Local read for update: key={}, value={}", this.id, msg.key, localValue);
            } else {
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, OperationType.CLIENT_UPDATE));
            }
        }

        // This sleep operation ensures that messages are FIFO as the constraint
        // We encounter a problem where a replica got a update message before the initial read,
        // leading to a lock that could not be removed. While the actual update was still performed.
        try {
            Thread.sleep(Messages.DELAY);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.error("Node {} - could not execute delay after update!", this.id);
            e.printStackTrace();
        }
        
        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_UPDATE, msg.key));
        checkGetOperation(operationId);
    }

    private void startGetOperation(Messages.ClientGet msg, String operationId, List<ActorRef> nodeList) {
        GetOperation operation = new GetOperation(msg.key, sender(), OperationType.CLIENT_GET);
        pendingGet.put(operationId, operation);
    
        for (ActorRef ref : nodeList) {
            if (ref == getSelf()) {
                VersionedValue localValue = get(msg.key);
                operation.responses.add(localValue);
                logger.debug("Node {} - Local GET: key={}, value={}", this.id, msg.key, localValue);
                releaseLock(msg.key, operationId);
            } else {
                scheduleMessage(ref, getSelf(), new Messages.ReplicaGet(msg.key, operationId, OperationType.CLIENT_GET));
            }
        }

        scheduleTimeoutMessage(getSelf(), new Messages.Timeout(operationId, OperationType.CLIENT_GET, msg.key));
        checkGetOperation(operationId);
    }

    // ===== MESSAGE HANDLERS - REPLICA OPERATIONS =====
    
    private void onReplicaGet(Messages.ReplicaGet msg) {

        OperationType operationType = msg.operationType == OperationType.JOIN ? OperationType.CLIENT_GET : msg.operationType; 
        
        if (!acquireLock(msg.key, operationType, msg.operationId)) {
            logger.warn("Node {} - Cannot execute {} on key {}: locked at replica level", this.id, operationType, msg.key);
            logger.debug("Node {} - Replica Lock State [locks -> {}, types -> {}]", this.id, this.keylocks, this.keyLockTypes);
            return;
        }

        VersionedValue requestedValue = get(msg.key);
        logger.debug("Node {} - Replica GET: key={}, value={}, type={}", 
                    this.id, msg.key, requestedValue, msg.operationType);
        

        // Already contains the INIT type for the ternary operator above
        if (msg.operationType == OperationType.CLIENT_GET || msg.operationType == OperationType.JOIN) {
            releaseLock(msg.key, msg.operationId);
        }

        scheduleMessage(sender(), getSelf(), new Messages.GetResponse(msg.key, requestedValue, msg.operationId));
    }

    private void onReplicaUpdate(Messages.ReplicaUpdate msg) {
        VersionedValue result = updateWithCurrentValue(msg.key, msg.value, msg.currentValue);
        releaseLock(msg.key, msg.operationId);
        logger.debug("Node {} - Replica update completed: key={}, new_value={} - lock released", 
                    this.id, msg.key, result);
    }

    private void onGetResponse(Messages.GetResponse msg) {
        GetOperation operation = pendingGet.get(msg.operationId);
        if (operation == null) return; // Operation might have already completed or timed out
        
        operation.responses.add(msg.value);
        checkGetOperation(msg.operationId);
    }
    
    // ===== OPERATION COMPLETION LOGIC =====
    
    private void checkGetOperation(String operationId) {
        GetOperation operation = pendingGet.get(operationId);
        if (operation == null) return;

        switch (operation.operationType) {
            case CLIENT_UPDATE -> handleUpdateCompletion(operation, operationId);
            case CLIENT_GET -> handleGetCompletion(operation, operationId);
            case JOIN -> handleJoinCompletion(operation, operationId);
            default -> logger.error("Node {} - tried a GET in order to complete operation {}", this.id, operation.operationType);
        }
    }

    private void handleUpdateCompletion(GetOperation operation, String operationId) {
        if (operation.responses.size() >= dataStoreManager.W) {
            VersionedValue currentValue = findLatestValue(operation.responses);
            
            VersionedValue newValue = (currentValue == null) 
                ? new VersionedValue(operation.updateValue, 1)
                : new VersionedValue(operation.updateValue, currentValue.getVersion() + 1);

            Messages.UpdateResponse response = new Messages.UpdateResponse(operation.key, newValue, operationId);
            scheduleMessage(operation.client, getSelf(), response);
            pendingGet.remove(operationId);
            
            logger.info("Node {} - UPDATE completed: key={}, new_version={}", 
                       this.id, operation.key, newValue.getVersion());
            performActualUpdate(operation.key, operation.updateValue, operationId, operation.client, currentValue);
        }
    }

    private void handleGetCompletion(GetOperation operation, String operationId) {
        if (operation.responses.size() >= dataStoreManager.R) {
            VersionedValue latestValue = findLatestValue(operation.responses);
            
            Messages.GetResponse response = new Messages.GetResponse(operation.key, latestValue, operationId);
            scheduleMessage(operation.client, getSelf(), response);
            pendingGet.remove(operationId);
            logger.info("Node {} - GET completed: key={}, value={}", this.id, operation.key, latestValue);
        }
    }

    private void handleJoinCompletion(GetOperation operation, String operationId) {
        if (operation.responses.size() == dataStoreManager.N) {
            VersionedValue latestValue = findLatestValue(operation.responses);
            
            this.dataStore.put(operation.key, latestValue);
            pendingGet.remove(operationId);
            
            logger.debug("Node {} - JOIN: acquired key {} with value {}", this.id, operation.key, latestValue);
            if (!this.nodeRegistry.containsKey(this.id)) {
                this.nodeRegistry.put(this.id, getSelf());
                
                for (ActorRef ref : nodeRegistry.values()) {
                    if (ref != getSelf()) {
                        scheduleMessage(ref, getSelf(), new Messages.Announce(this.id));
                    } 
                }
            }
        }
    }

    private VersionedValue findLatestValue(List<VersionedValue> values) {
        VersionedValue latest = null;
        for (VersionedValue value : values) {
            if (value != null && (latest == null || value.getVersion() > latest.getVersion())) {
                latest = value;
            }
        }
        return latest;
    }

    private void performActualUpdate(int key, String value, String operationId, ActorRef client, VersionedValue currentValue) {
        List<ActorRef> nodeList = getNextNNodes(key, this.nodeRegistry);
        UpdateOperation operation = pendingUpdate.get(operationId);

        if (operation == null) {
            logger.error("Node {} - UpdateOperation not found for operationId: {}", this.id, operationId);
            return;
        }

        logger.debug("Node {} - Executing actual update for key {}", this.id, key);
        for (ActorRef ref : nodeList) {
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

    // ===== MESSAGE HANDLERS - ERROR HANDLING =====
    
    private void onTimeout(Messages.Timeout msg) {
        switch (msg.operationType) {
            case CLIENT_GET -> handleGetTimeout(msg);
            case CLIENT_UPDATE -> handleUpdateTimeout(msg);
            case JOIN -> handleJoinTimeout();
            case LEAVE -> handleLeaveTimeout();
            default -> logger.debug("Node {} - Unhandled timeout for operation type {}", this.id, msg.operationType);
        }
    }

    private void handleGetTimeout(Messages.Timeout msg) {
        if (pendingGet.containsKey(msg.operationId)) {
            ActorRef client = pendingGet.remove(msg.operationId).client;
            logger.warn("Node {} - Timeout on GET operation for key {}", this.id, msg.key);
            scheduleMessage(client, getSelf(), new Messages.Error(msg.key, msg.operationId, OperationType.CLIENT_GET));
        }
    }

    private void handleUpdateTimeout(Messages.Timeout msg) {
        if (pendingGet.containsKey(msg.operationId) && pendingUpdate.containsKey(msg.operationId)) {
            UpdateOperation updateOperation = pendingUpdate.remove(msg.operationId);
            pendingGet.remove(msg.operationId);
            
            logger.warn("Node {} (Coordinator) - Timeout on UPDATE operation for key {}", this.id, msg.key);
            
            // Notify replica nodes about timeout
            for (ActorRef ref : getNextNNodes(updateOperation.key, this.nodeRegistry)) {
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

    private void handleJoinTimeout() {
        if (this.nodeRegistry.isEmpty()) {
            getContext().become(initialSpawn());
            logger.error("Node {} - JOIN timeout: cannot contact neighbor", this.id);
        } else {
            logger.debug("Node {} - JOIN timeout ignored (already connected)", this.id);
            logger.debug("Node {} - Node Registry {}", this.id, this.nodeRegistry);
        }
    }

    private void handleLeaveTimeout() {
        if (!this.nodeRegistry.isEmpty()) {
            logger.warn("Node {} - LEAVE timeout: staying in cluster", this.id);
        } else {
            logger.debug("Node {} - LEAVE timeout ignored", this.id);
        }
    }

    // ===== DEBUG UTILITIES =====
    
    private void onDebugPrintDataStore(Messages.DebugPrintDataStore msg) {
        logger.info("Node {} DataStore: {}", this.id, this.dataStore);
    }
}