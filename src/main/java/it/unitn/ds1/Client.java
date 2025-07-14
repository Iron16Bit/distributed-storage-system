package it.unitn.ds1;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;


//this class implements client functionalities
public class Client {
    
    // private Integer coordinatorID; decided randomly from main (for testing we can force it)
    
    private ActorRef coordinator;
    private Map<Integer, VersionedValue> dataStore = new HashMap<>();
    
    //--Constructor--
    public Client(ActorRef coordinator) {
        this.coordinator = coordinator;
    }

    //--Getters and Setters--
    public Map<Integer, VersionedValue> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<Integer, VersionedValue> dataStore) {
        this.dataStore = dataStore;
    }

}
