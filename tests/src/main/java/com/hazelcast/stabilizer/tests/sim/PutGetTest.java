package com.hazelcast.stabilizer.tests.sim;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by danny on 2/6/15.
 */
public class PutGetTest {


    public String clientHzFile = "client-hazelcast.xml";
    public HazelcastInstance client;

    public String baseMapName="m";
    public int totalMaps = 10;
    public int totalKeys = 10;
    public int threadCount = 2;



    public PutGetTest() throws IOException {

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();

        client = HazelcastClient.newHazelcastClient(clientConfig);
    }


    public void run(long time) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        Set<Callable<Object>> callables = new HashSet<Callable<Object>>();
        for(int i=0; i<threadCount; i++) {
            callables.add(new MyTask());
        }
        executor.invokeAll(callables);
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("end");
        executor.shutdown();

    }

    private class MyTask implements Callable<Object> {

        Random random = new Random();

        public static final long MAXIMUM_LATENCY = 60 * 1000 * 1000; // 1 minute
        private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);


        public Object call() throws Exception {


            IMap map = client.getMap(baseMapName+random.nextInt(totalMaps));

            histogram.reset();
            histogram.setStartTimeStamp(System.currentTimeMillis());

            for(int i=0; i<5; i++){
                long startTime = System.nanoTime();

                Object o = map.get(random.nextInt(totalKeys));
                System.out.println(o);

                long endTime = System.nanoTime();
                histogram.recordValue(endTime - startTime);
            }

            histogram.setEndTimeStamp(System.currentTimeMillis());
            histogram.outputPercentileDistribution(System.out, 1.0);

            return null;
        }
    }

    public static void main(String[] args) throws Throwable {

        PutGetTest p = new PutGetTest();
        p.run(3);
    }
}
