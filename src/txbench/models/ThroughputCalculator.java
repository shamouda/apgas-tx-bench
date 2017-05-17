package txbench.models;

/**
 * Estimates a throughput of a single iteration 
 */
public class ThroughputCalculator {
    private long totalTxCount;
    private long totalTxTimeNS;
    private int p;
    private int t;
    private int h;
    private int o;

    public ThroughputCalculator(int p, int t, int h, int o) {
        this.p = p;
        this.t = t;
        this.h = h;
        this.o = o;

    }

    public void addCount(long localCount) {
        totalTxCount += localCount;
    }

    public void addNSTime(long localTimeNS) {
        totalTxTimeNS += localTimeNS;
    }

    public double operationsPerMillisecond() {
        long allOperations = totalTxCount * h * o;
        int producers = p * t;
        return ((double) allOperations) / (totalTxTimeNS / 1e6) * producers;
    }

    public long getCount() {
        return totalTxCount;
    }

    public long getTime() {
        return totalTxTimeNS;
    }

}
