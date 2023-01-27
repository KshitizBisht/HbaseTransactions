package com.project.hbase;

import java.util.HashMap;
import java.util.Map.Entry;

public class testMap {
    public static void main(String[] args) {
        HashMap<String, String> newmap = new HashMap<String, String>();
        newmap.put("Hello", "Hello");
        for (Entry<String, String> set :
        newmap.entrySet()) {
            System.out.println(set.getKey() + " = "
            + set.getValue());
   }
    }
}
