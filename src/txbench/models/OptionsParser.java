package txbench.models;
import java.util.HashMap;
import java.util.Iterator;

import apgas.Configuration;

public class OptionsParser {
    
    private HashMap<String, String> options = new HashMap<String,String>();
    public OptionsParser(String[] args) throws Exception {
        options = parse(args);
    }
    
    private static HashMap<String, String> parse(String[] args) throws Exception {
        HashMap<String, String> map = new HashMap<String,String>();
        String lastKey = null;
        String lastValue = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                lastKey = args[i].replace("-", ""); 
            }
            else if (lastKey != null) {
                lastValue = args[i];
                map.put(lastKey, lastValue);
                lastKey = null;
                lastValue = null;
            }
            else
                throw new Exception("Failed to parse parameters ...");
        }
        return map;
    }

    public String get(String key, String def) {
        String sVal = def;
        String ret = options.get(key);
        if (ret != null && !ret.equals("")) {
            sVal = ret;
        }
        return sVal;
    }
    
    public Long get(String key, Long def) {
        Long lVal = def;
        String ret = options.get(key);
        if (ret != null && !ret.equals("")) {
            lVal = Long.parseLong(ret);
        }
        return lVal;
    }
    
    public Float get(String key, Float def) {
        Float fVal = def;
        String ret = options.get(key);
        if (ret != null && !ret.equals("")) {
            fVal = Float.parseFloat(ret);
        }
        return fVal;
    }
    
    public Double get(String key, Double def) {
        Double dVal = def;
        String ret = options.get(key);
        if (ret != null && !ret.equals("")) {
            dVal = Double.parseDouble(ret);
        }
        return dVal;
    }
    
    public Integer get(String key, Integer def) {
        Integer iVal = def;
        String ret = options.get(key);
        if (ret != null && !ret.equals("")) {
            iVal = Integer.parseInt(ret);
        }
        return iVal;
    }
    
    
}
