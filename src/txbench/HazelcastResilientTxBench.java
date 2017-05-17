package txbench;

import static apgas.Constructs.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalTaskContext;

import apgas.Configuration;
import apgas.DeadPlacesException;
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
import txbench.models.ThreadThroughput;
import txbench.models.RecoveryManager;
import txbench.models.TxBenchFailed;
import txbench.models.TxBenchParameters;
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
    public final static boolean DEBUG = System.getProperty("txbench.debug") != null
            && System.getProperty("txbench.debug").equals("true");

    public static void main(String[] args) {
        try {
            boolean RESILIENT = System.getProperty(Configuration.APGAS_RESILIENT) != null
                    && System.getProperty(Configuration.APGAS_RESILIENT).equals("true");
            OptionsParser opts = new OptionsParser(args);

            // s= spare places
            int s = opts.get("s", 2);

            // p= number of places creating transactions
            int p = opts.get("p", places().size() - s);

            // r= range of possible keys
            long r = opts.get("r", 32 * 1024L);

            // u= percentage of update operation
            float u = opts.get("u", 0.5F);

            // n= number of benchmarking iterations
            int n = opts.get("n", 5);

            // t= number of threads creating transactions per place
            int t = opts.get("t", Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS)));

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
            // "place:iteration:time_seconds,place:iteration:time_seconds" eg.
            // "2:1:1,4:2:2"
            String victims = opts.get("v", "");

            if (!victims.equals("") && !RESILIENT) {
                throw new TxBenchFailed("Wrong configurations, having victims requires setting -Dapgas.resilient=true");
            }

            HashMap<Integer, VictimList> victimProfiles = Utils.parseVictims(victims);

            // a PlaceThroughput object at each place to record throughput
            // related statistics
            final PlaceThroughput localThroughput = PlaceLocalObject.make(places(),
                    () -> new PlaceThroughput(here().id, t));

            // a ActivePlacesLocalObject object at each place to maintain the
            // list of active places
            final ActivePlacesLocalObject localActivePlaces = PlaceLocalObject.make(places(),
                    () -> new ActivePlacesLocalObject(s));

            RecoveryManager recoveryManager = new RecoveryManager(localActivePlaces);
            if (!victims.equals("")) {
                GlobalRuntime.getRuntime().setPlaceFailureHandler(recoveryManager::replaceDeadPlace);
            }

            new TxBenchParameters(p, t, r, u, n, w, d, h, o, g, s, victimProfiles).printRunConfigurations();

            long startWarmup = System.currentTimeMillis();
            if (w == -1) {
                System.out.println("no warmpup");
            } else {
                System.out.println("warmup started");
                runIteration(RESILIENT, p, t, w, r, u, h, o, g, localThroughput, localActivePlaces, null, null);
                resetStatistics(p, localThroughput);
                System.out.println("warmup completed, warmup elapsed time ["
                        + (System.currentTimeMillis() - startWarmup) + "]  ms \n");
            }

            for (int iter = 1; iter <= n; iter++) {
                long startIter = System.currentTimeMillis();
                VictimList iterVictims = victimProfiles.get(iter);

                System.out.println("iteration:" + iter + " started");
                runIteration(RESILIENT, p, t, d, r, u, h, o, g, localThroughput, localActivePlaces, iterVictims, null);
                System.out.println("iteration:" + iter + " completed, iteration elapsedTime ["
                        + (System.currentTimeMillis() - startIter) + "]  ms");

                printThroughput(iter, p, t, h, o, localThroughput, localActivePlaces);
                resetStatistics(p, localThroughput);
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
            PlaceThroughput localThroughput, ActivePlacesLocalObject localActivePlaces, VictimList victims,
            PlaceThroughput recoveryThroughput) {
        try {
            finish(() -> {
                for (int i = 0; i < p; i++) {
                    Place pl = places().get(i);
                    startPlaceAsync(RESILIENT, pl, p, t, d, r, u, h, o, g, localThroughput, localActivePlaces, victims,
                            null);
                }
            });
        } catch (DeadPlacesException dpe) {
            if (!RESILIENT)
                throw dpe;
        }
    }

    /**
     * Creates producer threads at a given place 'pl'. Also creates a suicide
     * activity to kill this place at the proper time if it is a victim.
     */
    private static void startPlaceAsync(boolean RESILIENT, Place pl, int p, int t, long d, long r, float u, int h,
            int o, int g, PlaceThroughput localThroughput, ActivePlacesLocalObject localActivePlaces,
            VictimList victims, ThreadThroughput[] oldThroughput) {

        asyncAt(pl, () -> {
            int logicalPlaceId = localActivePlaces.getLogicalId();
            // If this place is replacing a dead place, initialize its
            // throughput values using the dead place's suicide note
            if (oldThroughput != null) {
                localThroughput.reinit(logicalPlaceId, oldThroughput);
                System.out.println(here() + " initializing the place throughput with " + localThroughput);
            }

            // create 't' asynchronous producers
            for (int thrd = 1; thrd <= t; thrd++) {
                final int producerId = thrd - 1;
                async(() -> {
                    produce(RESILIENT, producerId, p, t, d, r, u, h, o, g, localThroughput, localActivePlaces);
                });
            }

            if (RESILIENT && victims != null) {
                long killTimeSec = victims.getKillTimeInSeconds(here().id);
                if (killTimeSec != -1) {
                    uncountedAsyncAt(here(), () -> {
                        long startedNS = System.nanoTime();
                        long deadline = System.currentTimeMillis() + 1000 * killTimeSec;
                        System.out.println(
                                "Hammer kill timer at " + here() + " starting sleep for " + killTimeSec + " secs");
                        while (deadline > System.currentTimeMillis()) {
                            long sleepTime = deadline - System.currentTimeMillis();
                            Thread.sleep(sleepTime);
                        }

                        // send my throughput values to the left neighbor
                        Place prev = localActivePlaces.prevPlace();
                        PlaceThroughput myThroughput = localThroughput;
                        myThroughput.setElapsedTime(System.nanoTime() - startedNS);

                        final ThreadThroughput[] victimThreads = myThroughput.thrds;
                        final long victimLogicalId = myThroughput.logicalPlaceId;
                        Place victim = here();
                        at(prev, () -> {
                            localThroughput.nextPlaceThroughput = new PlaceThroughput(victimLogicalId, victimThreads);
                            localThroughput.nextPlaceDeathTimeNS = System.nanoTime();
                            System.out.println(here() + " Received suicide note from " + victim + " throughputValues: "
                                    + localThroughput.nextPlaceThroughput);
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
            int g, PlaceThroughput localThroughput, ActivePlacesLocalObject localActivePlaces) throws TxBenchFailed {
        localThroughput.started = true;
        if (localThroughput.recovered) {
            System.out.println(here() + " added place started to produce transactions " + localThroughput);
        }

        HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
        int logicalPlaceId = localActivePlaces.getLogicalId();
        Random rand = new Random((here().id + 1) * producerId);
        ThreadThroughput myThroughput = localThroughput.thrds[producerId];
        long timeNS = myThroughput.elapsedTimeNS;

        TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.TWO_PHASE)
                .setTimeout(1, TimeUnit.SECONDS);

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

                    // if (!successful)
                    // System.out.println(here() + ": retry transaction
                    // succeeded ");

                    successful = true;
                    break;
                } catch (Exception f) {
                    // System.out.println(here() + ": retry transaction due to
                    // exception [" + f.getMessage() + "] ");
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
            SlaveChange slaveChange = localActivePlaces.nextPlaceChange();
            if (RESILIENT && producerId == 0 && slaveChange.changed) {
                Place nextPlace = slaveChange.newPlace;
                System.out.println(here() + " discovered a place change nextPlace is " + nextPlace);
                if (localThroughput.nextPlaceDeathTimeNS == -1)
                    throw new TxBenchFailed(here() + " assertion error, did not receive a suicide note ...");
                PlaceThroughput oldThroughput = localThroughput.nextPlaceThroughput;
                long recoveryTime = System.nanoTime() - localThroughput.nextPlaceDeathTimeNS;

                oldThroughput.shiftElapsedTime(recoveryTime);

                System.out.println(here() + " Calculated recovery time = " + (recoveryTime / 1e9) + " seconds");
                startPlaceAsync(RESILIENT, nextPlace, p, t, d, r, u, h, o, g, localThroughput, localActivePlaces, null,
                        oldThroughput.thrds);
            }
        }
    }

    private static void printThroughput(int iteration, int p, int t, int h, int o, PlaceThroughput localThroughput,
            ActivePlacesLocalObject localActivePlaces) {
        final GlobalRef<ThroughputCalculator> throughputCalcGr = new GlobalRef<ThroughputCalculator>(
                new ThroughputCalculator(p, t, h, o));
        finish(() -> {
            List<Place> activePlaces = localActivePlaces.getActivePlaces();
            for (int i = 0; i < p; i++) {
                asyncAt(activePlaces.get(i), () -> {

                    if (!localThroughput.started)
                        throw new TxBenchFailed(here() + " never started ...");
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

    private static void resetStatistics(int p, PlaceThroughput localThroughput) {
        finish(() -> {
            for (int i = 0; i < p; i++) {
                asyncAt(places().get(i), () -> localThroughput.reset());
            }
        });
    }

    private static String hazelcastKey(int logicalPlaceId, String key) {
        return (logicalPlaceId << 32) + "_" + key;
    }

}
