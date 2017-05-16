package txbench;

import static apgas.Constructs.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import txbench.models.PlaceThroughput;
import txbench.models.ProducerThroughput;
import txbench.models.RecoveryManager;
import txbench.models.STMBenchFailed;
import txbench.models.STMBenchParameters;
import txbench.models.ThroughputCalculator;
import txbench.models.VictimList;
import txbench.models.ParticipantOperations.KeyInfo;

public class HazelcastResilientTxBench {

    public static void main(String[] args) {
        try {

            if (System.getProperty(Configuration.APGAS_PLACES) == null) {
                System.setProperty(Configuration.APGAS_PLACES, "5");
            }
            System.setProperty(Configuration.APGAS_THREADS, "2");

            boolean RESILIENT = false; 
            // System.getProperty(Configuration.APGAS_RESILIENT) != null &&
            // System.getProperty(Configuration.APGAS_RESILIENT).equals("true");

            OptionsParser opts = new OptionsParser(args);

            long r = opts.get("r", 32 * 1024L);
            float u = opts.get("u", 0.0F);
            int n = opts.get("n", 5);
            int t = opts.get("t", 1/*Integer.parseInt(System.getProperty(Configuration.APGAS_THREADS))*/);
            long w = opts.get("w", 5000L);
            long d = opts.get("d", 5000L);
            int h = opts.get("h", 2);
            int o = opts.get("o", 2);
            int g = opts.get("g", -1);
            int s = opts.get("s", 0);
            String victims = opts.get("v", "2:1:1,3:2:1,4:3:1" ); // place:iteration:time,place:iteration:time
            
            HashMap<Integer, VictimList> victimProfiles = parseVictims(victims);

            int p = places().size();

           
            final PlaceThroughput throughputPLH = PlaceLocalObject.make(places(),
                    () -> new PlaceThroughput(here().id, t));
            final ActivePlacesLocalObject activePlacesMgr = PlaceLocalObject.make(places(),
                    () -> new ActivePlacesLocalObject(s));

            RecoveryManager recoveryManager = new RecoveryManager(activePlacesMgr);
            if (!victims.equals("")) {
                System.out.println();
                System.setProperty(Configuration.APGAS_RESILIENT, "true");
                RESILIENT = true;
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

            System.out.println("+++ STMBenchHazelcast Succeeded +++");

        } catch (Exception e) {
            System.out.println("!!! STMBenchHazelcast Failed !!!");
            e.printStackTrace();
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
        System.out.println("iteration:" + iteration + ":globalthroughput(op/MS):" + operationsPerMS);
        System.out.println("========================================================================");

    }

    private static void resetStatistics(int p, PlaceThroughput throughputPLH) {
        finish(() -> {
            for (int i = 0; i < p; i++) {
                asyncAt(places().get(i), () -> throughputPLH.reset());
            }
        });
    }

    public static void runIteration(boolean RESILIENT, int p, int t, long d, long r, float u, int h, int o, int g,
            PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr, VictimList victims,
            PlaceThroughput recoveryThroughput) {
        finish(() -> {
            for (int i = 0; i < p; i++) {
                Place pl = places().get(i);
                startPlace(RESILIENT, pl, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr, victims,
                        recoveryThroughput);
            }
        });
    }

    private static void startPlace(boolean RESILIENT, Place pl, int p, int t, long d, long r, float u, int h, int o,
            int g, PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr, VictimList victims,
            PlaceThroughput recoveryThroughput) {
        asyncAt(pl, () -> {

            if (recoveryThroughput != null) {
                System.out.println(here() + " reinitializing the place throughput with " + recoveryThroughput);
                throughputPLH.reinit(recoveryThroughput);
                System.out.println(here() + " current throughput " + throughputPLH);
            }

            for (int thrd = 1; thrd <= t; thrd++) {
                final int producerId = thrd - 1;
                async(() -> {
                    produce(RESILIENT, producerId, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr);
                });
            }

            if (RESILIENT && victims != null) {
                long killTime = victims.getKillTime(here().id);
                if (killTime != -1) {
                    uncountedAsyncAt(here(), () -> {
                        System.out.println("Hammer kill timer at " + here() + " starting sleep for " + killTime + " secs");
                        long startedNS = System.nanoTime();

                        long deadline = System.currentTimeMillis() + 1000 * killTime;
                        while (deadline > System.currentTimeMillis()) {
                            long sleepTime = deadline - System.currentTimeMillis();
                            Thread.sleep(sleepTime);
                        }

                        Place prev = activePlacesMgr.prevPlace();
                        PlaceThroughput myThroughput = throughputPLH;
                        myThroughput.setElapsedTime(System.nanoTime() - startedNS);

                        final ProducerThroughput[] victimThreads = myThroughput.thrds;
                        final long victimLogicalId = myThroughput.virtualPlaceId;
                        Place me = here();
                        at(prev, () -> {
                            throughputPLH.rightPlaceThroughput = new PlaceThroughput(victimLogicalId, victimThreads);
                            throughputPLH.rightPlaceDeathTimeNS = System.nanoTime();
                            System.out.println(here() + " Received suicide note from " + me + " throughputValues: "
                                    + throughputPLH.rightPlaceThroughput);
                        });

                        System.out.println(here() + " Good bye ...");
                        java.lang.Runtime.getRuntime().halt(1);
                    });
                }
            }
        });
    }

    public static void produce(boolean RESILIENT, int producerId, int p, int t, long d, long r, float u, int h, int o,
            int g, PlaceThroughput throughputPLH, ActivePlacesLocalObject activePlacesMgr) throws STMBenchFailed {
        HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName("apgas");
        int logicalPlaceId = activePlacesMgr.getLogicalId();
        System.out.println(here().id + "x" + producerId + " started ...");

        Random rand = new Random((here().id + 1) * producerId);
        ProducerThroughput myThroughput = throughputPLH.thrds[producerId];
        long timeNS = myThroughput.elapsedTimeNS; 
        while (timeNS < d * 1e6) {

            int[] participants = nextTransactionParticipants(rand, p, h, logicalPlaceId);
            ArrayList<ParticipantOperations> operations = nextRandomOperations(rand, p, participants, r, u, o);

            long start = System.nanoTime();
            TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.TWO_PHASE);

            boolean retry = false;
            while (timeNS < d * 1e6) {
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
                    if (retry)
                        System.out.println(here() + ": retry transaction succeeded ");
                    break;
                } catch (Exception f) {
                    System.out.println(here() + ": retry transaction due to exception [" + f.getMessage() + "] ");
                    retry = true;
                } finally {
                    timeNS += (System.nanoTime() - start);
                }
            }
            myThroughput.elapsedTimeNS = timeNS;
            myThroughput.txCount++;
            if (g != -1 && myThroughput.txCount % g == 0)
                System.out.println(here() + " Progress " + here().id + "x" + producerId + ":" + myThroughput.txCount);
/*
            SlaveChange slaveChange = activePlacesMgr.nextPlaceChange();
            if (RESILIENT && producerId == 0 && slaveChange.changed) {
                Place nextPlace = slaveChange.newPlace;
                if (throughputPLH.rightPlaceDeathTimeNS == -1)
                    throw new STMBenchFailed(here() + " assertion error, did not receive suicide note ...");
                PlaceThroughput oldThroughput = throughputPLH.rightPlaceThroughput;
                long recoveryTime = System.nanoTime() - throughputPLH.rightPlaceDeathTimeNS;
                oldThroughput.shiftElapsedTime(recoveryTime);
                System.out.println(here() + " Calculated recovery time = " + (recoveryTime / 1e9) + " seconds");
                startPlace(RESILIENT, nextPlace, p, t, d, r, u, h, o, g, throughputPLH, activePlacesMgr, null,
                        oldThroughput);
            }*/
        }

        System.out.println(here().id + "x" + producerId + " completed ...");

    }

    public static int[] nextTransactionParticipants(Random rand, int p, int h, int myPlaceIndex) {
        HashSet<Integer> selectedPlaces = new HashSet<Integer>();
        selectedPlaces.add(myPlaceIndex);
        while (selectedPlaces.size() < h) {
            selectedPlaces.add((int) (Math.abs(rand.nextLong()) % p));
        }

        int[] rail = new int[h];
        Iterator<Integer> iter = selectedPlaces.iterator();
        for (int c = 0; c < h; c++) {
            rail[c] = iter.next();
        }
        Arrays.sort(rail);
        return rail;
    }

    public static ArrayList<ParticipantOperations> nextRandomOperations(Random rand, int p, int[] participants, long r,
            float u, int o) {
        ArrayList<ParticipantOperations> list = new ArrayList<ParticipantOperations>();

        long keysPerPlace = r / p;

        for (int pl : participants) {
            KeyInfo[] keys = new KeyInfo[o];

            long baseKey = pl * keysPerPlace;
            HashSet<String> uniqueKeys = new HashSet<String>();
            for (int x = 0; x < o; x++) {
                boolean read = rand.nextFloat() > u;
                String k = "key" + (baseKey + (Math.abs(rand.nextLong() % keysPerPlace)));
                while (uniqueKeys.contains(k))
                    k = "key" + (baseKey + (Math.abs(rand.nextLong() % keysPerPlace)));
                uniqueKeys.add(k);
                long value = Math.abs(rand.nextLong()) % 1000;
                keys[x] = new KeyInfo(k, read, value);
            }
            list.add(new ParticipantOperations(pl, keys));
        }

        return list;
    }

    private static String hazelcastKey(int logicalPlaceId, String key) {
        return (logicalPlaceId << 32) + "_" + key;
    }

    private static HashMap<Integer, VictimList> parseVictims(String victimConfig) throws Exception {
        System.out.println("started parsing victims " + victimConfig);
        HashMap<Integer, VictimList> result = new HashMap<Integer, VictimList>();

        if (victimConfig == null || victimConfig.equals(""))
            return result;

        String[] victimTuples = victimConfig.split(",");
        for (String tuple : victimTuples) {
            String[] parts = tuple.split(":");
            if (parts.length < 3)
                throw new Exception("Wrong victim tuples format");

            Integer iteration = Integer.parseInt(parts[1]);
            VictimList v = result.get(iteration);
            if (v == null) {
                v = new VictimList();
                result.put(iteration, v);
            }
            v.addVictim(Integer.parseInt(parts[0]), Long.parseLong(parts[2]));
        }

        return result;
    }
}
