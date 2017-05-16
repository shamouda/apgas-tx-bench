package txbench.models;

import java.io.Serializable;

public class ProducerThroughput implements Serializable {
    public long elapsedTimeNS;
    public long txCount;
    public long placeId;
    public long threadId;

    public ProducerThroughput(long placeId, long threadId, long elapsedTimeNS, long txCount) {
        this.elapsedTimeNS = elapsedTimeNS;
        this.txCount = txCount;
        this.placeId = placeId;
        this.threadId = threadId;
    }

    public ProducerThroughput(long placeId, long threadId){
        this.placeId = placeId;
        this.threadId = threadId;
    }

    public String toString() {
        return placeId + "x" + threadId + ": elapsedTime=" + elapsedTimeNS / 1e9 + " seconds  txCount= " + txCount;
    }
}
