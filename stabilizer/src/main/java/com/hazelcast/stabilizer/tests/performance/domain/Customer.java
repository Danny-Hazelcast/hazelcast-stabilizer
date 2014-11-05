package com.hazelcast.stabilizer.tests.performance.domain;

import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Customer implements Serializable {

    public String id;
    public String name;

    public Address billingAddress;
    public Address deliveryAddress;

    public List<Order> orders;

    public Customer(){
       init();
    }

    public void init(){

        id = StringUtils.generateString(8);
        name = StringUtils.generateString(5);

        billingAddress = new Address();
        deliveryAddress = new Address();

        orders = new ArrayList<Order>(100);
        for(int i=0; i<100; i++){
            orders.add(new Order());
        }
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", billingAddress=" + billingAddress +
                ", deliveryAddress=" + deliveryAddress +
                ", orders=" + orders +
                '}';
    }
}
