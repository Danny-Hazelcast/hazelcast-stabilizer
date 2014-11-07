package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.stabilizer.tests.map.domain.*;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import org.HdrHistogram.IntHistogram;

import java.util.Random;
import java.util.Set;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.printMemStats;
import static junit.framework.Assert.assertEquals;

public class MapInit {
    private final static ILogger log = Logger.getLogger(MapInit.class);
    private String basename = this.getClass().getCanonicalName();

    public int threadCount=3;
    public int totalKeys = 1000;
    public int memberCount = 1;
    public String mapName;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Integer, Customer> map;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(mapName);

        if(TestUtils.isMemberNode(targetInstance)){

            while ( targetInstance.getCluster().getMembers().size() != memberCount ) {
                Thread.sleep(1000);
            }
            log.info(basename + ": cluster == " + memberCount);

            Thread.sleep(1000*10);

            final PartitionService partitionService = targetInstance.getPartitionService();
            final Set<Partition> partitionSet = partitionService.getPartitions();
            for (Partition partition : partitionSet) {
                while (partition.getOwner() == null) {
                    Thread.sleep(1000);
                }
            }
            log.info(basename + ": all " + partitionSet.size() + " partitions assigned");



            printMemStats(basename);

            final Member localMember = targetInstance.getCluster().getLocalMember();
            for(int i=0; i<totalKeys; i++){
                Partition partition = partitionService.getPartition(i);
                if (localMember.equals(partition.getOwner())) {
                    map.put(i, new Customer());
                    log.info(basename+": setup Put key="+i);
                }
            }

            //while(map.size()!=totalKeys){
            //    Thread.sleep(1000);
            //}

            log.info(basename + ": After setup map size=" + map.size());

            printMemStats(basename);
        }
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

        for(int i=1; i<threadCount; i++){
            workers[0].putLatencyHisto.add(workers[i].putLatencyHisto);
            workers[0].getLatencyHisto.add(workers[i].getLatencyHisto);
        }

        targetInstance.getList(basename+"putHisto").add(workers[0].putLatencyHisto);
        targetInstance.getList(basename+"getHisto").add(workers[0].getLatencyHisto);

    }

    private class Worker implements Runnable {
        IntHistogram putLatencyHisto = new IntHistogram(1, 1000*30, 0);
        IntHistogram getLatencyHisto = new IntHistogram(1, 1000*30, 0);
        Random random = new Random();

        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt(totalKeys);


                long start = System.currentTimeMillis();
                Customer c = map.get(key);
                long stop = System.currentTimeMillis();
                getLatencyHisto.recordValue(stop - start);

                if(c==null){
                    log.severe(basename+": key "+key+" == null");
                    log.severe(basename+": map size="+map.size());
                    log.severe(basename+": totalkeys="+totalKeys);
                }
            }
        }
    }


    @Verify(global = false)
    public void verify() throws Exception {
        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(mapName);
        log.info(basename+": "+mapConfig);
        log.info(basename+": verify map size="+map.size());

        assertEquals(basename + ": map (" + map.getName() + ") size ", totalKeys, map.size());
    }
}