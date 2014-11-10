package com.hazelcast.stabilizer.tests.map.domain;

import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;

import java.io.Serializable;
import java.util.Random;

public class Item implements Serializable {

    public static Random rand = new Random();

    public String id;
    public String name;
    public double price;

    public Item(){
        init();
    }

    public void init(){
        id = StringUtils.generateString(8);
        name = StringUtils.generateString(10);
        price = rand.nextDouble();
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}
