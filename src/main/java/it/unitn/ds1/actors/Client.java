package it.unitn.ds1.actors;

import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.AbstractActor;
import akka.actor.Props;

import it.unitn.ds1.Messages;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.TimeoutDelay;
import it.unitn.ds1.utils.VersionedValue;

public class Client extends AbstractActor {
    
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    
    private final Map<Integer, VersionedValue> dataStore = new HashMap<>();
    private final Map<String, PendingOperation> pendingOperations = new HashMap<>();
    private int counter = 0;

    // Clean operation tracking
    private static class PendingOperation {
        final int key;
        final OperationType type;
        final long timestamp;
        
        PendingOperation(int key, OperationType type) {
            this.key = key;
            this.type = type;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private String generateOperationId(OperationType type) {
        return type.name() + "-" + (++counter);
    }

    private void onInitiateGet(Messages.InitiateGet msg) {
        String operationId = generateOperationId(OperationType.CLIENT_GET);
        pendingOperations.put(operationId, new PendingOperation(msg.key, OperationType.CLIENT_GET));
        
        msg.coordinator.tell(new Messages.ClientGet(msg.key), getSelf());
        scheduleTimeout(operationId, OperationType.CLIENT_GET, msg.key);
        
        logger.debug("Client initiated GET operation {} on key {}", operationId, msg.key);
    }

    private void onInitiateUpdate(Messages.InitiateUpdate msg) {
        String operationId = generateOperationId(OperationType.CLIENT_UPDATE);
        pendingOperations.put(operationId, new PendingOperation(msg.key, OperationType.CLIENT_UPDATE));
        
        msg.coordinator.tell(new Messages.ClientUpdate(msg.key, msg.value), getSelf());
        scheduleTimeout(operationId, OperationType.CLIENT_UPDATE, msg.key);
        
        logger.debug("Client initiated UPDATE operation {} on key {}", operationId, msg.key);
    }

    private void onGetResponse(Messages.GetResponse msg) {
        clearPendingOperationsForKey(msg.key);
        
        if (msg.value == null) {
            logger.info("GET REJECTED - Key {} not found", msg.key);
            return;
        }

        updateLocalDataStore(msg.key, msg.value);
    }

    private void onUpdateResponse(Messages.UpdateResponse msg) {
        clearPendingOperationsForKey(msg.key);
        
        if (msg.versionedValue == null) {
            logger.error("UPDATE REJECTED - Key {} received null value", msg.key);
            return;
        }

        updateLocalDataStore(msg.key, msg.versionedValue);
    }

    private void onTimeout(Messages.Timeout msg) {
        PendingOperation operation = pendingOperations.remove(msg.operationId);
        
        if (operation != null) {
            logger.error("Client Operation {}, Id {}, timed out after {}ms due to coordinator unresponsiveness", 
                        msg.operationType, msg.operationId, 
                        System.currentTimeMillis() - operation.timestamp);
            
            // Optional: Implement retry logic here
            // handleOperationTimeout(operation, msg.operationId);
        } else {
            logger.debug("Client timeout evaded, Operation {} - Id {} already completed", 
                        msg.operationType, msg.operationId);
        }
    }

    private void onError(Messages.Error msg) {
        clearPendingOperationsForKey(msg.key);
        logger.error("ERROR - {}: Key {}", msg.operationType, msg.key);
    }

    // Helper methods for cleaner code
    private void clearPendingOperationsForKey(int key) {
        pendingOperations.entrySet().removeIf(entry -> entry.getValue().key == key);
    }

    private void updateLocalDataStore(int key, VersionedValue newValue) {
        VersionedValue localValue = dataStore.get(key);

        if (localValue == null || localValue.getVersion() <= newValue.getVersion()) {
            logger.info("DATA UPDATED - Key: {}, Value: {}", key, newValue);
            dataStore.put(key, newValue);
        } else {
            logger.info("DATA REJECTED - Key {} has newer version (local: v{}, received: v{})", 
                    key, localValue.getVersion(), newValue.getVersion());
        }
    }

    private void handleOperationTimeout(PendingOperation operation, String operationId) {
        // Optional: Implement retry logic or other timeout handling
        logger.warn("Operation timeout for key {} - consider retry or coordinator failover", operation.key);
    }

    private void scheduleTimeout(String operationId, OperationType operationType, int key) {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(TimeoutDelay.getDelayForOperation(operationType) + 500, TimeUnit.MILLISECONDS),
            getSelf(),
            new Messages.Timeout(operationId, operationType, key),
            getContext().system().dispatcher(), 
            getSelf()
        );
    }

    public static Props props() {
        return Props.create(Client.class, Client::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Messages.InitiateGet.class, this::onInitiateGet)
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.InitiateUpdate.class, this::onInitiateUpdate)
            .match(Messages.UpdateResponse.class, this::onUpdateResponse)
            .match(Messages.Timeout.class, this::onTimeout)
            .match(Messages.Error.class, this::onError)
            .build();
    }
}