package com.spark.dom;

import java.io.Serializable;
import java.util.ArrayList;

public class Order implements Serializable {
    public String user_id;
    public String initiator;
    public Long time;
    public ArrayList<Item> items;

    public String toString() {
        return "{ user_id: " + user_id + ", " +
                "initiator: " + initiator + ", " +
                "time: " + time.toString() + ", " +
                "items: " + items.toString() + " }";
    }
}
