package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
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

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.printMemStats;
import static junit.framework.Assert.assertEquals;

public class MapInit {
    private final static ILogger log = Logger.getLogger(MapInit.class);
    private String basename = this.getClass().getCanonicalName();

    public int threadCount=3;
    public int totalKeys = 1000;

    public int stressKeys = 10;

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

            Thread.sleep(1000 * 10);

            /*
            PartitionService partitionService = targetInstance.getPartitionService();
            while(!partitionService.isClusterSafe()){
                Thread.sleep(1000);
            }
            log.info(basename + ": cluster is safe ");
            */

            PartitionService partitionService = targetInstance.getPartitionService();
            final Set<Partition> partitionSet = partitionService.getPartitions();
            for (Partition partition : partitionSet) {
                while (partition.getOwner() == null) {
                    Thread.sleep(1000);
                }
            }
            log.info(basename + ": all " + partitionSet.size() + " partitions assigned");
        }
    }

    @Warmup(global = false)
    public void warmup() throws Exception {
        printMemStats(basename);

        Customer c =  new Customer();

        PartitionService partitionService = targetInstance.getPartitionService();
        Member localMember = targetInstance.getCluster().getLocalMember();

        for(int i=0; i<totalKeys; i++){
            Partition partition = partitionService.getPartition(i);
            if (localMember.equals(partition.getOwner())) {
                map.put(i, c);
                //log.info(basename+": setup Put key="+i);
            }
        }
        log.info(basename + ": After setup map size=" + map.size());

        printMemStats(basename);
    }

    @Run
    public void run() {
        //IAtomicReference<Boolean> running = targetInstance.getAtomicReference("running");

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

        Customer stress = new Customer();

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

                if(map.size()!=totalKeys){
                    log.severe(basename+": map size="+map.size()+" !");
                }

                key = random.nextInt(stressKeys);
                IMap stressMap = targetInstance.getMap(basename+"stress");
                stressMap.put(key, stress);
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