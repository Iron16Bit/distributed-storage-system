package it.unitn.ds1.actors;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.ds1.Messages;
import it.unitn.ds1.utils.VersionedValue;

public class Client extends AbstractActor {
    
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    
    private final Map<Integer, VersionedValue> dataStore = new HashMap<>();

    // Getters and Setters
    public Map<Integer, VersionedValue> getDataStore() {
        return new HashMap<>(dataStore);
    }

    public void setDataStore(Map<Integer, VersionedValue> dataStore) {
        this.dataStore.clear();
        this.dataStore.putAll(dataStore);
    }

    // Message Handlers with simplified logging
    private void onGetResponse(Messages.GetResponse msg) {
        if (msg.value == null) {
            logger.info("GET REJECTED - Key {} not found", msg.key);
            return;
        }

        VersionedValue localValue = dataStore.get(msg.key);

        if (localValue == null) {
            logger.info("GET ACCEPTED - Key: {}, Value: {}", msg.key, msg.value);
            dataStore.put(msg.key, msg.value);
            return;
        }
        
        if (localValue.getVersion() > msg.value.getVersion()) {
            logger.info("GET REJECTED - Key {} has older version (local: v{}, received: v{})", 
                       msg.key, localValue.getVersion(), msg.value.getVersion());
            return;
        }
        
        logger.info("GET ACCEPTED - Key: {}, Value: {}", msg.key, msg.value);
        dataStore.put(msg.key, msg.value);
    }

    private void onUpdateResponse(Messages.UpdateResponse msg) {
        if (msg.versionedValue == null) {
            logger.error("UPDATE REJECTED - Key {} received null value", msg.key);
            return;
        }

        VersionedValue localValue = dataStore.get(msg.key);
        
        if (localValue == null) {
            logger.info("UPDATE ACCEPTED - Key: {}, Value: {}", msg.key, msg.versionedValue);
            dataStore.put(msg.key, msg.versionedValue);
            return;
        }

        if (localValue.getVersion() > msg.versionedValue.getVersion()) {
            logger.warn("UPDATE REJECTED - Key {} inconsistent (local: v{}, received: v{})", 
                       msg.key, localValue.getVersion(), msg.versionedValue.getVersion());
            return;
        }
        
        logger.info("UPDATE ACCEPTED - Key: {}, Value: {}", msg.key, msg.versionedValue);
        dataStore.put(msg.key, msg.versionedValue);
    }

    private void onError(Messages.Error msg) {
        logger.error("ERROR - {}: Key {}", msg.operationType, msg.key);
    }

    public static Props props() {
        return Props.create(Client.class, Client::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.UpdateResponse.class, this::onUpdateResponse)
            .match(Messages.Error.class, this::onError)
            .build();
    }
}