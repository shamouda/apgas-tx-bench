package txbench.models;

public class ParticipantOperations implements Comparable<ParticipantOperations> {
    
    public static class KeyInfo implements Comparable<KeyInfo> {
        public String key;
        public boolean read;
        public long value;
        
        public KeyInfo(String k, boolean r, long v) {
            this.key = k;
            this.read = r;
            this.value = v;
        }
        
        public int compareTo(KeyInfo that) {
            return key.compareTo(that.key);
        }
    }
    
    public int dest;
    public KeyInfo[] keys;
    
    public ParticipantOperations(int dest, KeyInfo[] keys) {
        this.dest = dest;
        this.keys = keys;
    }
    
    public String toString() {
        String str = "memberOperations:" /*+ dest*/ + ":";
        for (KeyInfo k : keys)
            str += "("+k.key+","+k.read+")" ;
        return str;
    }
    
    public int compareTo(ParticipantOperations that) {
        if (dest == that.dest)
            return 0;
        else if ( dest < that.dest)
            return -1;
        else
            return 1;
    }
}
