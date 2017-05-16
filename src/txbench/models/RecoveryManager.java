package txbench.models;

import static apgas.Constructs.here;

import apgas.Place;

public class RecoveryManager {

    int lastAllocatedPlace = -1;
    private ActivePlacesLocalObject plh;
    public RecoveryManager(ActivePlacesLocalObject plh) {
        this.plh = plh;
    }
    
    public synchronized void reportPlaceFailure(Place p) {
        if (p.id > 0) {
            System.err.println("Observing failure of " + p + " from " + here());
            /*
            List<Place> activePlaces = plh.getActivePlaces();
            
            int deadLogicalId = activePlaces.indexOf(p);
            int size = activePlaces.size();
            if (lastAllocatedPlace == -1)
                lastAllocatedPlace = activePlaces.get(size -1).id;
            
            if (lastAllocatedPlace == places().size()-1) {
                System.err.println(here() + "  no spare places available to recover " + p + " ...");
                java.lang.Runtime.getRuntime().halt(1);
            }
            
            lastAllocatedPlace++;
            Place spare = places().get(lastAllocatedPlace);
            
            finish(() -> {
                for (Place pl: places()) {
                    if (p.id == pl.id)
                        continue;
                    asyncAt(pl, () -> {
                        plh.replace(deadLogicalId, spare);
                    });
                }
            });
            */
        }
    }
}
