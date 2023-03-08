package org.example;
import java.util.*;



class sortmapKey {

    // This map stores unsorted values
    static Map<String, Integer> map = new HashMap<>();

    // Function to sort map by Key
    public static void sortbykey()
    {
        // TreeMap to store values of HashMap
        TreeMap<String, Integer> sorted = new TreeMap<>();

        // Copy all data from hashMap into TreeMap
        sorted.putAll(map);

        // Display the TreeMap which is naturally sorted
        for (Map.Entry<String, Integer> entry : sorted.entrySet())
            System.out.println("Key = " + entry.getKey() +
                    ", Value = " + entry.getValue());
    }

    // Driver Code
    public static void main(String args[])
    {
        // putting values in the Map
        map.put("table3row1quantifier1", 80);
        map.put("table1row2quantifier7", 90);
        map.put("table2row1quantifier3", 80);
        map.put("table4row1quantifier3", 75);
        map.put("table3row6quantifier8", 40);

        // Calling the function to sortbyKey
        sortbykey();
    }
}