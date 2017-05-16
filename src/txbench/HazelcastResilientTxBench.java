package txbench;

import static apgas.Constructs.async;
import static apgas.Constructs.asyncAt;
import static apgas.Constructs.at;
import static apgas.Constructs.finish;
import static apgas.Constructs.here;
import static apgas.Constructs.places;
import static apgas.Constructs.uncountedAsyncAt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalTaskContext;

import apgas.Configuration;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.util.GlobalRef;
import apgas.util.PlaceLocalObject;

import txbench.models.ActivePlacesLocalObject;
import txbench.models.ActivePlacesLocalObject.SlaveChange;
import txbench.models.OptionsParser;
import txbench.models.ParticipantOperations;
import txbench.models.ParticipantOperations.KeyInfo;
import txbench.models.PlaceThroughput;
import txbench.models.ProducerThroughput;
import txbench.models.RecoveryManager;
import txbench.models.STMBenchFailed;
import txbench.models.STMBenchParameters;
import txbench.models.ThroughputCalculator;
import txbench.models.Utils;
import txbench.models.VictimList;

/**
 * A benchmark program to measure Hazelcast's transaction throughput in a
 * distributed asynchronous environment. Activities within each place generate
 * transactions on APGAS's backing store, which is a Hazelcast store. The
 * benchmark can be configured to kill places at specific times to evaluate the
 * throughput in failure scenarios.
 * 
 * We report the throughput in terms of operations per milliseconds. An
 * operation is either a read operation (map.get) or a write operation (map.put)
 */
public class HazelcastResilientTxBench {

    public static void main(String[] args) {
        try {
            if (System.getProperty(Configuration.APGAS_PLACES) == null) {
                System.setProperty(Configuration.APGAS_PLACES, "5");
            }
            System.setProperty(Configuration.APGAS_THREADS, "2");
            boolean RESILIENT = System.getProperty(Configuration.APGAS_RESILIENT) != null
                    && System.getProperty(Configuration.APGAS_RESILIENT).equals("true");

            OptionsParser opts = new OptionsParser(args);

            // s= spare places
            int s = opts.get("s", 1);

            // p = number of places creating transactions
            int p = opts.get("p", places().size() - s);

            // r = range of possible keys
            long r = opts.get("r", 32 * 1024L);

            // u = percentage of update operation
            float u = opts.get("u", 0.0F);

            // n= number of benchmarking iterations
            int n = opts.get("n", 5);

            // t= number of threads creating transactions per place
            int t = opts.get("t", 1 /*Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS))*/);

            // w= warm up duration in milliseconds
            long w = opts.get("w", 5000L);

            // d= benchmarking iteration duration in milliseconds
            long d = opts.get("d", 10000L);

            // h= number of transaction participants
            int h = opts.get("h", 2);

            // o= number of operations per transaction participant
            int o = opts.get("o", 2);

            // g= interval of progress reporting per producer
            int g = opts.get("g", -1);

            // v= victims configurations in the form:
            // "place:iteration:time_seconds,place:iteration:time_seconds"
            String victims = opts.get("v", "2:1:1,3:2:1,4:3:1");

            HashMap<Integer, VictimList> victimProfiles = Utils.parseVictims(victims);

            // a PlaceThroughput object at each place to record throughput
            // related statistics
            final PlaceThroughput throughputPLH = PlaceLocalObject.make(places(),
                    () -> new PlaceThroughput(here().id, t));

            // a ActivePlacesLocalObject object at each place to maintain the
            // list of active places
            final ActivePlacesLocalObject activePlacesMgr = PlaceLocalObject.make(places(),
                    () -> new ActivePlacesLocalObject(s));

            RecoveryManager recoveryManager = new RecoveryManager(activePlacesMgr);
            if (!victims.equals("")) {
                System.out.println();
                System.setProperty(Configuration.APGAS_RESILIENT, "true");
                RESILIENT = true;
                System.out.println("Setting the place failure handler ...");
                GlobalRuntime.getRuntime().setPlaceFailureHandler(recoveryManager::reportPlaceFailure);
            }

            new STMBenchParameters(p, t, r, u, n, w, d, h, o, g, s, victimProfiles).printRunConfigurations();

            long startWarmup = System.currentTimeMillis();
            if (w == -1) {
                System.out.println("no warmpup");
            } else {
                System.out.println("warmup started");
                runIteration(RESILIENT, p, t, w, r, u, h, o, g, throughputPLH, activePlacesMgr, null, null);
                resetStatistics(p, throughputPLH);
                System.out.println("warmup completed, warmup elapsed time ["
                        + (System.currentTimeMillis() - startWarmup) + "]  ms ");
            }

            for (int iter = 1; iter <= n; iter++) {
                long startIter = System.currentTimeMillis();
                VictimList iterVictims = victimProfiles.get(iter);
                runIteration(RESILIENT, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr, iterVictims, null);
                System.out.println("iteration:" + iter + " completed, iteration elapsedTime ["
                        + (System.currentTimeMillis() - startIter) + "]  ms ");

                printThroughput(p, t, h, o, throughputPLH, iter);
                resetStatistics(p, throughputPLH);
            }

            System.out.println("+++ HazelcastResilientTxBench Succeeded +++");

        } catch (Exception e) {
            System.out.println("!!! HazelcastResilientTxBench Failed !!!");
            e.printStackTrace();
        }
    }

    /**
     * Executes a warm up or a benchmarking iteration
     **/
    public static void runIteration(boolean RESILIENT, int p, int t, long d, long r, float u, int h, int o, int g,
            PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr, VictimList victims,
            PlaceThroughput recoveryThroughput) {
        finish(() -> {
            for (int i = 0; i < p; i++) {
                Place pl = places().get(i);
                startPlaceAsync(RESILIENT, pl, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr, victims,
                        recoveryThroughput);
            }
        });
    }

    /**
     * Creates producer threads at a given place 'pl'. Also creates a suicide
     * activity to kill this place at the proper time if it is a victim.
     */
    private static void startPlaceAsync(boolean RESILIENT, Place pl, int p, int t, long d, long r, float u, int h,
            int o, int g, PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr, VictimList victims,
            PlaceThroughput recoveryThroughput) {

        asyncAt(pl, () -> {
            // If this place is replacing a dead place, initialize its
            // throughput values using the dead place's suicide note
            if (recoveryThroughput != null) {
                System.out.println(here() + " initializing the place throughput with " + recoveryThroughput);
                throughputPLH.reinit(recoveryThroughput);
            }

            // create 't' asynchronous producers
            for (int thrd = 1; thrd <= t; thrd++) {
                final int producerId = thrd - 1;
                async(() -> {
                    produce(RESILIENT, producerId, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr);
                });
            }

            if (RESILIENT && victims != null) {
                long killTimeSec = victims.getKillTimeInSeconds(here().id);
                if (killTimeSec != -1) {
                    uncountedAsyncAt(here(), () -> {
                        long startedNS = System.nanoTime();
                        long deadline = System.currentTimeMillis() + 1000 * killTimeSec;
                        System.out.println(
                                "Hammer kill timer at " + here() + " starting sleep for " + killTimeSec + " secs   deadlineMS " + deadline);
                        while (deadline > System.currentTimeMillis()) {
                            long sleepTime = deadline - System.currentTimeMillis();
                            Thread.sleep(sleepTime);
                        }

                        // send my throughput values to the left neighbor
                        Place prev = activePlacesMgr.prevPlace();
                        PlaceThroughput myThroughput = throughputPLH;
                        myThroughput.setElapsedTime(System.nanoTime() - startedNS);

                        final ProducerThroughput[] victimThreads = myThroughput.thrds;
                        final long victimLogicalId = myThroughput.virtualPlaceId;
                        Place victim = here();
                        at(prev, () -> {
                            throughputPLH.rightPlaceThroughput = new PlaceThroughput(victimLogicalId, victimThreads);
                            throughputPLH.rightPlaceDeathTimeNS = System.nanoTime();
                            System.out.println(here() + " Received suicide note from " + victim + " throughputValues: "
                                    + throughputPLH.rightPlaceThroughput);
                        });

                        System.out.println(here() + " Good bye ...");
                        java.lang.Runtime.getRuntime().halt(1);
                    });
                }
            }
        });
    }

    /**
     * The core method that generates the transactions. Each producer records
     * the number of completed transactions and the exact elapsed time; the
     * exact time may deviate slightly from the user requested time 'd'.
     * 
     * When the next place gets replaced with a new place due to a failure, the
     * first producer in the current place activates the added place by starting
     * producer threads there.
     */
    public static void produce(boolean RESILIENT, int producerId, int p, int t, long d, long r, float u, int h, int o,
            int g, PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr) throws STMBenchFailed {
        HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
        int logicalPlaceId = activePlacesMgr.getLogicalId();
        Random rand = new Random((here().id + 1) * producerId);
        ProducerThroughput myThroughput = throughputPLH.thrds[producerId];
        long timeNS = myThroughput.elapsedTimeNS;

        TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.TWO_PHASE);

        // generate new transactions until the duration 'd' elapses
        while (timeNS < d * 1e6) {

            // generate random operations at random participants
            int[] participants = Utils.nextTransactionParticipants(rand, p, h, logicalPlaceId);
            ArrayList<ParticipantOperations> operations = Utils.nextRandomOperations(rand, p, participants, r, u, o);

            boolean successful = true;

            // retry a transaction until it succeeds or the duration elapses
            while (timeNS < d * 1e6) {
                long start = System.nanoTime();
                try {
                    
                    hz.executeTransaction(options, (TransactionalTaskContext context) -> {
                        final TransactionalMap<String, Long> map = context.getMap("map");
                        for (int m = 0; m < h; m++) {
                            ParticipantOperations myOperations = operations.get(m);
                            int dest = myOperations.dest;
                            KeyInfo[] keys = myOperations.keys;
                            for (int x = 0; x < o; x++) {
                                String key = hazelcastKey(dest, keys[x].key);
                                boolean read = keys[x].read;
                                long value = keys[x].value;
                                if (read) {
                                    map.get(key);
                                } else {
                                    map.put(key, value);
                                }
                            }
                        }
                        return null;
                    });

                    if (!successful)
                        System.out.println(here() + ": retry transaction succeeded ");

                    successful = true;
                    break;
                } catch (Exception f) {
                    System.out.println(here() + ": retry transaction due to exception [" + f.getMessage() + "] ");
                    successful = false;
                } finally {
                    timeNS += System.nanoTime() - start;
                }
            }
            myThroughput.elapsedTimeNS = timeNS;
            if (successful)
                myThroughput.txCount++;

            if (g != -1 && myThroughput.txCount % g == 0)
                System.out.println(here() + " Progress " + here().id + "x" + producerId + ":" + myThroughput.txCount);

            // If a new place is added next to this place create producer
            // threads there
            SlaveChange slaveChange = activePlacesMgr.nextPlaceChange();
            if (RESILIENT && producerId == 0 && slaveChange.changed) {
                Place nextPlace = slaveChange.newPlace;
                if (throughputPLH.rightPlaceDeathTimeNS == -1)
                    throw new STMBenchFailed(here() + " assertion error, did not receive a suicide note ...");
                PlaceThroughput oldThroughput = throughputPLH.rightPlaceThroughput;
                long recoveryTime = System.nanoTime() - throughputPLH.rightPlaceDeathTimeNS;
                oldThroughput.shiftElapsedTime(recoveryTime);
                System.out.println(here() + " Calculated recovery time = " + (recoveryTime / 1e9) + " seconds");
                startPlaceAsync(RESILIENT, nextPlace, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr, null,
                        oldThroughput);
            }
        }
    }

    private static void printThroughput(int p, int t, int h, int o, PlaceThroughput localThroughput, int iteration) {
        final GlobalRef<ThroughputCalculator> throughputCalcGr = new GlobalRef<ThroughputCalculator>(
                new ThroughputCalculator(p, t, h, o));
        finish(() -> {
            for (int i = 0; i < p; i++) {
                asyncAt(places().get(i), () -> {
                    Long localTxCounts = localThroughput.mergeCounts();
                    Long localElapsedTime = localThroughput.mergeTimes();
                    asyncAt(throughputCalcGr.home(), () -> {
                        ThroughputCalculator throughput = throughputCalcGr.get();
                        synchronized (throughput) {
                            throughput.addCount(localTxCounts);
                            throughput.addNSTime(localElapsedTime);
                        }
                    });

                });
            }
        });

        double operationsPerMS = throughputCalcGr.get().operationsPerMillisecond();
        System.out.println("iteration:" + iteration + ":throughput(op/MS):" + operationsPerMS);
        System.out.println("========================================================================");

    }

    private static void resetStatistics(int p, PlaceThroughput throughputPLH) {
        finish(() -> {
            for (int i = 0; i < p; i++) {
                asyncAt(places().get(i), () -> throughputPLH.reset());
            }
        });
    }

    private static String hazelcastKey(int logicalPlaceId, String key) {
        return (logicalPlaceId << 32) + "_" + key;
    }

}
