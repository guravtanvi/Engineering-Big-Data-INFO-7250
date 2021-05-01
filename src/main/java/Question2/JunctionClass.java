package Question2;

import java.util.Map;
import java.util.HashMap;

public class JunctionClass {

    //Creating a static final Map (key-value) so that one copy of variable exists and can't be reinitialize
    public static final Map<String, String> JUNCTION_MAP;

    //Creating an immutable map using a static initializer
    static {
        JUNCTION_MAP = new HashMap<String, String>();
        JUNCTION_MAP.put("True", "Accidents Near Junction");
        JUNCTION_MAP.put("False", "Accidents Not Near Junction");
        JUNCTION_MAP.put("", "Unknown");

    }

    //Returns the mapped value for each key (true/false)
    public String getJunctionMethod(String junction) {
        return JUNCTION_MAP.get(junction);
    }

}
