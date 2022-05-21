package utils;

import java.util.HashMap;
import java.util.Map;

public class Month {
    public static final Map<Integer, String> staticMap = new HashMap<>();

    static {
        staticMap.put(0,"January");
        staticMap.put(1,"February");
        staticMap.put(2,"March");
        staticMap.put(3,"April");
        staticMap.put(4,"May");
        staticMap.put(5,"June");
        staticMap.put(6,"July");
        staticMap.put(7,"August");
        staticMap.put(8,"September");
        staticMap.put(9,"October");
        staticMap.put(10,"November");
        staticMap.put(11,"December");
    }
}
