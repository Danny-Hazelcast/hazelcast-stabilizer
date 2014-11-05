package com.hazelcast.stabilizer.tests.performance.domain;

import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Order implements Serializable {

    public String id;
    public Date orderDate;
    public Date deliveryDate;
    public List<Item> items;

    public Order(){
        init();
    }

    public void init(){
        id = StringUtils.generateString(8);
        orderDate = new Date();
        deliveryDate = new Date();

        items = new ArrayList<Item>(100);
        for(int i=0; i<100; i++){
            items.add(new Item());
        }
    }

    @Override
    public String toString() {
        return "Order{" +
                "id='" + id + '\'' +
                ", orderDate=" + orderDate +
                ", deliveryDate=" + deliveryDate +
                ", items=" + items +
                '}';
    }
}
