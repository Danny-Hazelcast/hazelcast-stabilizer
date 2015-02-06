package com.hazelcast.stabilizer.tests.sim;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.ConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by danny on 2/6/15.
 */
public class PutGetTest {


    public String clientHzFile = "";
    public HazelcastInstance client;

    public int totalMaps = 10;
    public int totalKeys = 10;
    public int threadCount = 2;



    public PutGetTest() throws IOException {

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();

        client = HazelcastClient.newHazelcastClient(clientConfig);
    }


    public void run(long time) throws InterruptedException {

        long start = System.currentTimeMillis();


        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        Set<Callable<Object>> callables = new HashSet<Callable<Object>>();
        for(int i=0; i<threadCount; i++) {
            callables.add(new MyTask());
        }
        executor.invokeAll(callables);
        System.out.println("end");
        executor.shutdown();

    }

    private class MyTask implements Callable {



        public Object call() throws Exception {



            for(int i=0; i<10; i++)
                System.out.println("hi"+Thread.currentThread());
            return this;
        }
    }





    public static void main(String[] args) throws Throwable {

        PutGetTest p = new PutGetTest();
        p.run(3);
    }

}
