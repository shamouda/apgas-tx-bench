package txbench.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

import apgas.Configuration;

public class TxBenchParameters implements Serializable {

    private static final long serialVersionUID = 43243254353461L;
    int p; // producing places
    int t; // producing threads per place
    long r; // the maximum number of keys
    float u; // the update percentage
    int n; // number of iterations
    long w; // warmpup iteration duration in milliseconds
    long d; // benchmarking iteration duration in milliseconds
    int h; // transaction participants count
    int o; // operations per participants
    int g; // progress reporting interval
    int s; // spare places
    HashMap<Integer, VictimList> victimProfiles; // victims configurations

    public TxBenchParameters(int p, int t, long r, float u, int n, long w, long d, int h, int o, int g, int s,
            HashMap<Integer, VictimList> victimProfiles) {
        this.r = r;
        this.u = u;
        this.n = n;
        this.p = p;
        this.t = t;
        this.w = w;
        this.d = d;
        this.h = h;
        this.o = o;
        this.g = g;
        this.s = s;
        this.victimProfiles = victimProfiles;
    }

    public void printRunConfigurations() {
        System.out.println("HazelcastResilientTxBench starting with the following parameters:");
        System.out.println(Configuration.APGAS_PLACES + "=" + System.getProperty(Configuration.APGAS_PLACES));
        System.out.println(Configuration.APGAS_THREADS + "=" + System.getProperty(Configuration.APGAS_THREADS));
        System.out.println(Configuration.APGAS_RESILIENT + "=" + System.getProperty(Configuration.APGAS_RESILIENT));
        System.out.println("r=" + r);
        System.out.println("u=" + u);
        System.out.println("n=" + n);
        System.out.println("p=" + p);
        System.out.println("t=" + t);
        System.out.println("w=" + w);
        System.out.println("d=" + d);
        System.out.println("h=" + h);
        System.out.println("o=" + o);
        System.out.println("g=" + g);
        System.out.println("s=" + s);
        if (victimProfiles != null) {
            Iterator<Integer> iter = victimProfiles.keySet().iterator();
            while (iter.hasNext()) {
                int iteration = iter.next();
                VictimList v = victimProfiles.get(iteration);
                System.out.println("Victims for iteration " + iteration + ": { " + v.toString() + " } ");
            }
        }
    }
}
