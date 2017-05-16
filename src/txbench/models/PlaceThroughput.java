package txbench.models;

import apgas.util.PlaceLocalObject;

public class PlaceThroughput extends PlaceLocalObject {
    public int threads;
    public ProducerThroughput[] thrds;
    public PlaceThroughput rightPlaceThroughput;
    public long rightPlaceDeathTimeNS = -1;
    public long virtualPlaceId;

    public boolean started = false;
    public boolean recovered = false;

    public long reducedTime;
    public long reducedTxCount;

    public PlaceThroughput() {
        
    }
    
    public PlaceThroughput(long virtualPlaceId, int threads) {
        this.threads = threads;
        this.virtualPlaceId = virtualPlaceId;
        thrds = new ProducerThroughput[threads];
        for (int i = 0; i < threads; i++) {
            thrds[i] = new ProducerThroughput( virtualPlaceId, i);
        }
    }
    
    public PlaceThroughput(long virtualPlaceId, ProducerThroughput[] thrds) {
        this.threads = thrds.length;
        this.virtualPlaceId = virtualPlaceId;
        this.thrds = thrds;
    }
    
    public void reset() {
        for (int i = 0; i < threads; i++) {
            thrds[i] = new ProducerThroughput( virtualPlaceId, i);
        }
        rightPlaceThroughput = null;
        rightPlaceDeathTimeNS = -1;

        started = false;
        recovered = false;

        reducedTime = 0;
        reducedTxCount = 0;
    }
    
    public void reinit(PlaceThroughput other) {
        virtualPlaceId = other.virtualPlaceId;
        thrds = other.thrds;
        recovered = true;
    }
    
    public String toString() {
        String str = "PlaceThroughput[Place"+virtualPlaceId+"] ";
        for (int i = 0; i < threads; i++) {
            str += "{" + thrds[i] + "} ";
        }
        return str;
    }
    
    public void shiftElapsedTime(long timeNS) {
        for (int i = 0; i < threads; i++) {
            thrds[i].elapsedTimeNS += timeNS;
        }
    }
    
    public void setElapsedTime(long timeNS) {
        for (int i = 0; i < threads; i++) {
            thrds[i].elapsedTimeNS = timeNS;
        }
    }
    
    public long mergeCounts() {
        long sumCount = 0;
        for (int i = 0; i < threads; i++) {
            sumCount+= thrds[i].txCount;
        }
        return sumCount;
    }
    
    public long mergeTimes() {
        long sumTimes = 0;
        for (int i = 0; i < threads; i++) {
            sumTimes += thrds[i].elapsedTimeNS;
        }
        return sumTimes;
    }
}