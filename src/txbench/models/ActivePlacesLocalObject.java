package txbench.models;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import apgas.util.PlaceLocalObject;
import static apgas.Constructs.places;
import static apgas.Constructs.here;

public class ActivePlacesLocalObject extends PlaceLocalObject {
    private List<Place> activePlaces;
    private int logicalId = -1;
    private Place nextPlace = null;
    
    public static class SlaveChange {
        public boolean changed;
        public Place newPlace;
        public SlaveChange (boolean ch, Place np) {
            this.changed = ch;
            this.newPlace = np;
        }
    }
    
    public ActivePlacesLocalObject(int spare) {
        int activeSize = places().size() - spare;
        activePlaces = new ArrayList<Place>(activeSize);
        for (int i = 0; i < activeSize; i++) {
            activePlaces.add(places().get(i));
        }
        if (here().id < activeSize){
            logicalId = here().id;
            nextPlace = activePlaces.get((logicalId+1)%activePlaces.size()); 
        }
        printActivePlaces();
    }
    
    public synchronized void replace(int deadLogicalId, Place spare) {
        activePlaces.remove(deadLogicalId);
        activePlaces.add(deadLogicalId, spare);
        printActivePlaces();
    }
    
    
    public synchronized void allocate(int logicalId) throws Exception {
        if (this.logicalId != -1)
            throw new Exception("Failed to allocate " + here() + " already allocated for logicalId " + this.logicalId);
        this.logicalId = logicalId;
        activePlaces.remove(logicalId);
        activePlaces.add(logicalId, here());
        nextPlace = activePlaces.get((logicalId+1)%activePlaces.size()); 
    }
    
    public synchronized Place nextPlace() {
        return activePlaces.get((logicalId+1)%activePlaces.size());
    }
   
    public synchronized Place prevPlace() {
        return activePlaces.get((logicalId-1+activePlaces.size())%activePlaces.size());
    }
    
    public synchronized List<Place> getActivePlaces() {
        return activePlaces;
    }
    
    public synchronized SlaveChange nextPlaceChange() {
        Place curNextPlace = activePlaces.get((logicalId+1)%activePlaces.size());
        if (curNextPlace.id == nextPlace.id) {
            return new SlaveChange(false, null);
        }
        else {
            nextPlace = curNextPlace;
            return new SlaveChange(true, curNextPlace);
        }
    }
    
    public synchronized int getLogicalId() {
        return logicalId;
    }
    
    public void printActivePlaces() {
        String str = "";
        for (int i = 0; i < activePlaces.size(); i++) {
            str += activePlaces.get(i) + " ";
        }
        System.err.println(here() + "  activePlaces{" + str + "} ");
    }
}
