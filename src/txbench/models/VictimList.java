package txbench.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

public class VictimList implements Serializable {
    private HashMap<Integer,Long> placeKillTimes; // <<place,killTime>>
    
    public VictimList() {
        placeKillTimes = new HashMap<Integer,Long>();
    }
    
    public void addVictim (int place, long time) {
        placeKillTimes.put(place, time);
    }
    
    public long getKillTimeInSeconds(int place) {
        Long killTime = placeKillTimes.get(place);
        if (killTime == null || killTime == 0)
            return -1;
        return killTime;
    }
    
    public String toString() {
        String str = "";
        Iterator<Integer> iter = placeKillTimes.keySet().iterator();
        while (iter.hasNext()) {
            int place = iter.next();
            long time = placeKillTimes.get(place);
            str += "Place(" +place + "):" + time + " sec,";
        }
        return str;
    }
}
