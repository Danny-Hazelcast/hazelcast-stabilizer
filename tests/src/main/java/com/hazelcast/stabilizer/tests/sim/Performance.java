package com.hazelcast.stabilizer.tests.sim;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Performance {

    public File cvsDir = new File( System.getProperty("cvsDir", "/home/ec2-user/") );

    public String clientHzFile = System.getProperty("clientHzFile", "client-hazelcast.xml");
    public String baseMapName= System.getProperty("baseMapName", "m");
    public int totalMaps =  Integer.parseInt(System.getProperty("totalMaps", "10"));
    public int totalKeys = Integer.parseInt(System.getProperty("totalKeys", "10"));
    public int valueByteArraySize = Integer.parseInt(System.getProperty("valueByteArraySize", "10"));

    public double putProb=Double.parseDouble(System.getProperty("putProb", "1.0"));
    public double getProb=Double.parseDouble(System.getProperty("getProb", "0.0"));
    public double setProb=Double.parseDouble(System.getProperty("setProb", "0.0"));

    public int threadCount = Integer.parseInt(System.getProperty("threadCount", "8"));;
    public boolean sharedMetrics = Boolean.parseBoolean(System.getProperty("sharedMetrics", "true"));;

    private HazelcastInstance client;
    private ExecutorService executor;
    private Set<MyTask> tasks;
    private int durationSeconds=0;


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
        MetricRegistry metrics = new MetricRegistry();

        for(MyTask t: tasks) {
            t.setDuration(durationSeconds);

            if(sharedMetrics){
                t.metrics = metrics;
            }else{
                t.metrics = new MetricRegistry();
            }
        }
        executor.invokeAll(tasks);
    }

    public void printResults(){
        for(MyTask t : tasks){

            ConsoleReporter reporter = ConsoleReporter.forRegistry(t.metrics).build();
            CsvReporter csv = CsvReporter.forRegistry(t.metrics).build(cvsDir);

            csv.report();
            reporter.report();

            if(sharedMetrics){
                break;
            }
        }
    }

    private class MyTask implements Callable<Object> {

        public MetricRegistry metrics;
        private Timer getTimer;
        private Timer putTimer;
        private Timer setTimer;

        private Random random = new Random();
        private int duration = 0;
        private byte[] value = new byte[valueByteArraySize];

        public MyTask(){
            random.nextBytes(value);
        }

        public void setDuration(int seconds){
            duration = seconds * 1000;
        }

        public Object call() throws Exception {
            getTimer = metrics.timer("getTimer");
            putTimer = metrics.timer("putTimer");
            setTimer = metrics.timer("setTimer");

            long endTime = System.currentTimeMillis() + duration;
            while( System.currentTimeMillis() < endTime ){
                IMap map = client.getMap(baseMapName+random.nextInt(totalMaps));

                double chance = random.nextDouble();

                if((chance -= putProb)<=0) {
                    Timer.Context context =  putTimer.time();
                      Object o = map.put(random.nextInt(totalKeys), value);
                      //Thread.sleep(random.nextInt(10));
                    context.stop();

                }else if((chance-=getProb)<=0){
                    Timer.Context context =  getTimer.time();
                      Object o = map.get(random.nextInt(totalKeys));
                      //Thread.sleep(random.nextInt(10));
                    context.stop();

                }else{
                    Timer.Context context =  setTimer.time();
                      map.set(random.nextInt(totalKeys), value);
                      //Thread.sleep(random.nextInt(10));
                    context.stop();
                }
            }
            return null;
        }
    }

    public static void main(String[] args) throws Throwable {
        int jitWarmUpSec = Integer.parseInt(System.getProperty("jitWarmUpSec", "10"));
        int durationSec = Integer.parseInt(System.getProperty("durationSec", "30"));

        Performance p = new Performance();
        p.run(jitWarmUpSec);

        p.run(durationSec);
        p.printResults();
    }
}