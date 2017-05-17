package txbench.models;

import apgas.util.PlaceLocalObject;

/**
 * A container for the throughputs of the place's threads
 * */
public class PlaceThroughput extends PlaceLocalObject {
    /** the place logical id**/
    public long logicalPlaceId;
    
    /** the throughput of the individual threads **/
    public ThreadThroughput[] thrds;
    
    /** the throughput of the next place before it died **/
    public PlaceThroughput nextPlaceThroughput;
    
    /** the time when the next place died  **/
    public long nextPlaceDeathTimeNS = -1;
    
    /** whether the place ever started to produce transactions  **/
    public boolean started = false;
    
    /** whether the place has been added to replace a dead place  **/
    public boolean recovered = false;
   
    public PlaceThroughput() {
    }
    
    public PlaceThroughput(long logicalPlaceId, int threads) {
        this.logicalPlaceId = logicalPlaceId;
        thrds = new ThreadThroughput[threads];
        for (int i = 0; i < threads; i++) {
            thrds[i] = new ThreadThroughput( logicalPlaceId, i);
        }
    }
    
    public PlaceThroughput(long logicalPlaceId, ThreadThroughput[] thrds) {
        this.logicalPlaceId = logicalPlaceId;
        this.thrds = thrds;
    }
    
    public void reset() {
        for (int i = 0; i < thrds.length; i++) {
            thrds[i] = new ThreadThroughput( logicalPlaceId, i);
        }
        nextPlaceThroughput = null;
        nextPlaceDeathTimeNS = -1;

        started = false;
        recovered = false;
    }
    
    public void reinit(long logicalPlaceId, ThreadThroughput[] thrds) {
        this.logicalPlaceId = logicalPlaceId;
        this.thrds = thrds;
        recovered = true;
    }
    
    public String toString() {
        String str = "PlaceThroughput[Place"+logicalPlaceId+"] ";
        for (int i = 0; i < thrds.length; i++) {
            str += "{" + thrds[i] + "} ";
        }
        return str;
    }
    
    public void shiftElapsedTime(long timeNS) {
        for (int i = 0; i < thrds.length; i++) {
            thrds[i].elapsedTimeNS += timeNS;
        }
    }
    
    public void setElapsedTime(long timeNS) {
        for (int i = 0; i < thrds.length; i++) {
            thrds[i].elapsedTimeNS = timeNS;
        }
    }
    
    public long mergeCounts() {
        long sumCount = 0;
        for (int i = 0; i < thrds.length; i++) {
            sumCount+= thrds[i].txCount;
        }
        return sumCount;
    }
    
    public long mergeTimes() {
        long sumTimes = 0;
        for (int i = 0; i < thrds.length; i++) {
            sumTimes += thrds[i].elapsedTimeNS;
        }
        return sumTimes;
    }
}