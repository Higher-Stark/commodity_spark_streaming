package com.spark.dom;

public class Item implements Comparable<Item>{
    public String id;
    public int number;


    public String toString() {
        return "{ id: " + id + ", number: " + number + " }";
    }

    public int compareTo(Item other) {
        // overflow ?
        int my = Integer.valueOf(this.id);
        int it = Integer.valueOf(other.id);

        return my - it;
    }
}
