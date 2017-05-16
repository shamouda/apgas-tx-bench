package txbench.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import txbench.models.ParticipantOperations.KeyInfo;

public class Utils {
    /**
     * Select p-1 random places 
     **/
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

    /**
     * Select random keys and operations for each participant
     **/
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


    /**
     * Parse the victims configurations
     **/
    public static HashMap<Integer, VictimList> parseVictims(String victimConfig) throws Exception {
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
