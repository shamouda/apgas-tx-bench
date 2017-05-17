package txbench.models;

import java.io.Serializable;

/**
 * A container for a thread throughput information
 * */
public class ThreadThroughput implements Serializable {
    /** time elapsed in processing the transactions*/
    public long elapsedTimeNS;
    
    /** number of completed transactions*/
    public long txCount;
    
    /** place id */
    public long placeId;
    
    /** local thread id */
    public long threadId;

    public ThreadThroughput(long placeId, long threadId, long elapsedTimeNS, long txCount) {
        this.elapsedTimeNS = elapsedTimeNS;
        this.txCount = txCount;
        this.placeId = placeId;
        this.threadId = threadId;
    }

    public ThreadThroughput(long placeId, long threadId){
        this.placeId = placeId;
        this.threadId = threadId;
    }

    public String toString() {
        return placeId + "x" + threadId + ": elapsedTime=" + elapsedTimeNS / 1e9 + " seconds  txCount= " + txCount;
    }
}
