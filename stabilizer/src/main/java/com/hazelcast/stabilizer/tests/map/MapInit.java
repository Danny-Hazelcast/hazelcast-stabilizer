package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.domain.*;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import org.HdrHistogram.IntHistogram;

import java.io.Serializable;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.printMemStats;
import static junit.framework.Assert.assertEquals;

public class MapInit {
    private final static ILogger log = Logger.getLogger(MapInit.class);
    private String basename = this.getClass().getCanonicalName();

    public int threadCount=10;
    public int memberCount = 1;
    public String mapName;
    public int stressKeys = 100;


    private int totalKeys;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap map;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(mapName);

        if(TestUtils.isMemberNode(targetInstance)){

            /*
            while ( targetInstance.getCluster().getMembers().size() != memberCount ) {
                Thread.sleep(1000);
            }
            log.info(basename + ": cluster == " + memberCount);
            */


            PartitionService partitionService = targetInstance.getPartitionService();
            final Set<Partition> partitionSet = partitionService.getPartitions();

            for (Partition partition : partitionSet) {

                while (partition.getOwner() == null) {
                    Thread.sleep(1000);
                }
            }


            log.info(basename+": "+partitionSet.size()+" partitions");

            totalKeys = partitionService.getPartitions().size() * 4;
            Member localMember = targetInstance.getCluster().getLocalMember();
            for(int i=0; i<totalKeys; i++){
                Partition partition = partitionService.getPartition(i);
                if (localMember.equals(partition.getOwner())) {
                    map.put(i, i);
                    log.info(basename+": setup Put key="+i);
                }
            }
        }
    }

    @Warmup(global = false)
    public void warmup() throws Exception {
        /*
        printMemStats(basename);

        PartitionService partitionService = targetInstance.getPartitionService();

        totalKeys = partitionService.getPartitions().size() * 4;
        for(int i=0; i<totalKeys; i++){
            map.put(i, i);
        }
        log.info(basename + ": After warmup map size=" + map.size());

        printMemStats(basename);
        */
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());

        Worker[] workers = new Worker[threadCount];

        for(int i=0; i<threadCount; i++){
            workers[i] = new Worker();
            spawner.spawn(workers[i]);
        }
        spawner.awaitCompletion();

    }

    private class Worker implements Runnable {
        Random random = new Random();
        Customer stressObj = new Customer();
        Counter count = new Counter();

        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt(totalKeys);
                Object obj = map.get(key);

                if(obj==null){
                    log.severe(basename+": key "+key+" == null");
                    log.severe(basename+": map size="+map.size());
                    log.severe(basename+": totalkeys="+totalKeys);
                    count.getNull++;
                }

                if(map.size()!=totalKeys){
                    log.severe(basename+": map size="+map.size()+" !");
                    count.mapSizeError++;
                }

                key = random.nextInt(totalKeys);
                IMap stressMap = targetInstance.getMap(basename+"stress");
                stressMap.put(key, stressObj);
            }
            targetInstance.getList(basename+"res").add(count);
        }
    }

    private static class Counter implements Serializable {
        public long getNull=0;
        public long mapSizeError=0;

        public void add(Counter c) {
            getNull+=c.getNull;
            mapSizeError+=c.mapSizeError;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "getNull=" + getNull +
                    ", mapSizeError=" + mapSizeError +
                    '}';
        }
    }


    @Verify(global = false)
    public void verify() throws Exception {
        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(mapName);
        log.info(basename+": "+mapConfig);
        log.info(basename+": verify map size="+map.size());

        log.info(basename+": Stress map size="+targetInstance.getMap(basename+"stress").size());


        IList<Counter> results = targetInstance.getList(basename + "res");
        Counter total = new Counter();
        for (Counter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " from " + results.size()+" worker Threads");

        assertEquals(basename + ": map (" + map.getName() + ") size ", totalKeys, map.size());

        assertEquals(basename + ": "+total, total.getNull, 0);
        assertEquals(basename + ": "+total, total.mapSizeError, 0);
    }
}