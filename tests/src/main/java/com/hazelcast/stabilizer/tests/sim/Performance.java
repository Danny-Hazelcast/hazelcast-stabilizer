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

public class Performance {

    private static final long MAXIMUM_LATENCY = 1000 * 10; // 10 sec
    private static final int  SIG_DIGS = 4;
    private static final double outputScalingRatio = 1.0;

    public String clientHzFile = "client-hazelcast.xml";
    public HazelcastInstance client;

    public String baseMapName="m";
    public int totalMaps = 10;
    public int totalKeys = 10;
    public int threadCount = 4;

    private int durationSeconds=0;

    public ExecutorService executor;
    public Set<MyTask> tasks;

    public Performance() throws IOException {

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(clientHzFile);
        ClientConfig clientConfig = configBuilder.build();
        client = HazelcastClient.newHazelcastClient(clientConfig);

        executor = Executors.newFixedThreadPool(threadCount);

        tasks = new HashSet<MyTask>();
        for(int i=0; i<threadCount; i++) {
            tasks.add(new MyTask());
        }
    }

    public void run(int seconds) throws InterruptedException {
        durationSeconds = seconds;
        for(MyTask t: tasks) {
            t.setDuration(durationSeconds);
        }
        executor.invokeAll(tasks);
    }

    public void printResults(){

        Histogram histogram = new Histogram(MAXIMUM_LATENCY, SIG_DIGS);
        for(MyTask t: tasks) {
            histogram.add(t.histogram);

        }
        histogram.outputPercentileDistribution(System.out, outputScalingRatio);


        long totalops = histogram.getTotalCount();

        long start = histogram.getStartTimeStamp();
        long end = histogram.getEndTimeStamp();


        System.out.println("total operations="+totalops);
        System.out.println("duration sec ="+durationSeconds);
        System.out.println("opps / sec  ="+totalops / durationSeconds);


        /*
        long val = histogram.getValueAtPercentile(1.0);
        System.out.println("100% ="+val);

        AbstractHistogram.AllValues all = histogram.allValues();
        for( HistogramIterationValue v : all ){
            System.out.println(v.toString());
        }
        */
    }

    private class MyTask implements Callable<Object> {

        public final Histogram histogram = new Histogram(MAXIMUM_LATENCY, SIG_DIGS);
        public Random random = new Random();

        private int duration = 0;

        public void setDuration(int seconds){
            duration = seconds * 1000;
        }

        public Object call() throws Exception {

            histogram.reset();

            long startTime=System.currentTimeMillis();
            histogram.setStartTimeStamp(startTime);

            while( (System.currentTimeMillis()-startTime) < duration ){

                IMap map = client.getMap(baseMapName+random.nextInt(totalMaps));

                long start = System.currentTimeMillis();
                Object o = map.get(random.nextInt(totalKeys));
                long end = System.currentTimeMillis();

                histogram.recordValue(end - start);

                System.out.println("got = "+o);
            }

            histogram.setEndTimeStamp(System.currentTimeMillis());
            System.out.println("end");
            return null;
        }
    }

    public static void main(String[] args) throws Throwable {

        Performance p = new Performance();
        p.run(5);
        p.printResults();

        p.run(5);
        p.printResults();

    }
}
