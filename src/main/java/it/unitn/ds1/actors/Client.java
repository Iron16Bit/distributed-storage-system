package it.unitn.ds1.actors;

import java.util.HashMap;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.ds1.Messages;
import it.unitn.ds1.utils.VersionedValue;




//this class implements client functionalities
public class Client extends AbstractActor {
    
    // private Integer coordinatorID; decided randomly from main (for testing we can force it)
    
    private Map<Integer, VersionedValue> dataStore = new HashMap<>();

    //--Getters and Setters--
    public Map<Integer, VersionedValue> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<Integer, VersionedValue> dataStore) {
        this.dataStore = dataStore;
    }

    //--Messages--
    public void onGetResponse(Messages.GetResponse msg) {
        //Check if the response is null -> no item found or other possible errors.
        if (msg.value == null) {
            System.out.println("[REJECT]  Item (key " + msg.key + ") cannot be found ");
            return;
        }
    
        VersionedValue localValue = this.dataStore.get(msg.key);

        // If clienmt doesn't currenlty have the item, accept the reponse
        if (localValue == null) {
            System.out.println("[ACCEPT] Read Item: Key->" + msg.key + ", Value->" + msg.value);
            dataStore.put(msg.key, msg.value);
            return;
        }
        
        // If client has a newer version, reject the response
        if (localValue.getVersion() > msg.value.getVersion()) {
            System.out.println("[REJECT] Item (key " + msg.key + ") Read older version");
            return;
        }
        System.out.println("[ACCEPT] Read Item: Key->" + msg.key + ",value->" + msg.value);
        dataStore.put(msg.key, msg.value);
    }

    public void onUpdateResponse(Messages.UpdateResponse msg) {
        if (msg.versionedValue == null) {
            System.out.println("[REJECT]  Item (key " + msg.key + ") possible WTF moment ");
            return;
        }

        VersionedValue localValue = this.dataStore.get(msg.key);
        // If clienmt doesn't currenlty have the item, accept the reponse
        if (localValue == null) {
            System.out.println("[ACCEPT] Updated Item: Key->" + msg.key + ", Versioned Value->" + msg.versionedValue);
            dataStore.put(msg.key, msg.versionedValue);
            return;
        }

        // If client has a newer version, reject the response
        if (localValue.getVersion() > msg.versionedValue.getVersion()) {
            System.out.println("[REJECT] Item (key " + msg.key + ") write inconsistent (older than actual value)");
            return;
        }
        System.out.println("[ACCEPT] Updated Item: Key->" + msg.key + ",Versioned Value->" + msg.versionedValue);
        dataStore.put(msg.key, msg.versionedValue);

    }

    static public Props props() {
        return Props.create(Client.class, () -> new Client());
    }

    @Override
    public Receive createReceive() {
        //TODO check if something missing
        return receiveBuilder()
            .match(Messages.GetResponse.class, this::onGetResponse)
            .match(Messages.UpdateResponse.class, this::onUpdateResponse)
            .build();
    }  


}
