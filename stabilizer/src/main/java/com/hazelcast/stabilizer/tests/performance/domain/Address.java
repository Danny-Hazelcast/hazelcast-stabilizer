package com.hazelcast.stabilizer.tests.performance.domain;

import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;

import java.util.Random;

public class Address {

    public static Random random = new Random();

    public String id;
    public int number;
    public String address;
    public String postCode;
    public String comments;

    public Address(){
        init();
    }

    public void init(){
        id = StringUtils.generateString(8);
        number = random.nextInt();
        address = StringUtils.generateString(48);
        postCode = StringUtils.generateString(6);
        comments = StringUtils.generateString(128);
    }

    @Override
    public String toString() {
        return "Address{" +
                "id='" + id + '\'' +
                ", number=" + number +
                ", address='" + address + '\'' +
                ", postCode='" + postCode + '\'' +
                ", comments='" + comments + '\'' +
                '}';
    }
}
