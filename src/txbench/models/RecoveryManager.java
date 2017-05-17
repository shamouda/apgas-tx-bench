package txbench.models;

import static apgas.Constructs.*;

import java.util.List;

import apgas.Place;

public class RecoveryManager {

    public final static boolean DEBUG = System.getProperty("txbench.debug") != null
            && System.getProperty("txbench.debug").equals("true");
    
    private int lastActivePlace = -1;
    private ActivePlacesLocalObject plh;

    public RecoveryManager(ActivePlacesLocalObject plh) {
        this.plh = plh;

        List<Place> activePlaces = plh.getActivePlaces();
        int size = activePlaces.size();
        this.lastActivePlace = activePlaces.get(size - 1).id;
    }

    public synchronized void replaceDeadPlace(Place dead) {
        try {
            if (dead.id > 0) {
                System.err.println("Observing failure of " + dead + " from " + here());

                List<Place> activePlaces = plh.getActivePlaces();
                int deadLogicalId = activePlaces.indexOf(dead);
                int lastPlace = places().get(places().size() - 1).id;

                if (lastActivePlace + 1 > lastPlace) {
                    System.err.println(here() + "  no spare places available to recover " + dead + " ...");
                    return;
                }
                Place spare = new Place(lastActivePlace + 1);
                System.err.println("spare place " + spare + " is replacing place " + dead);
                lastActivePlace++;

                ActivePlacesLocalObject plhtemp = plh; // do not serialize this
                at(spare, () -> {
                    plhtemp.allocate(deadLogicalId);
                });

                System.err.println("allocating " + spare + " succeeded ...");

                finish(() -> {
                    for (Place pl : places()) {
                        if (spare.id == pl.id)
                            continue;
                        if (DEBUG)
                            System.err.println("handshaking with place " + pl);
                        asyncAt(pl, () -> {
                            plhtemp.replace(deadLogicalId, spare);
                        });
                    }
                });
            }
        } catch (Exception ex) {
            System.err.println("Recovery of " + dead + " failed with exception [" + ex.getMessage() + "] ");
            ex.printStackTrace();
        }
    }
}
