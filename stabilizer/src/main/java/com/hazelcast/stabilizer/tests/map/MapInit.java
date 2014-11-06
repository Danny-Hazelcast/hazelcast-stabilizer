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
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.map.domain.*;
import com.hazelcast.stabilizer.tests.utils.TestUtils;

import java.util.Set;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.printMemStats;
import static junit.framework.Assert.assertEquals;

public class MapInit {
    private final static ILogger log = Logger.getLogger(MapInit.class);

    public String basename = this.getClass().getName();
    public int totalKeys = 1000;
    public int memberCount = 1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Integer, Customer> map;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        if(TestUtils.isMemberNode(targetInstance)){

            while ( targetInstance.getCluster().getMembers().size() != memberCount ) {
                Thread.sleep(1000);
            }
            log.info(basename + ": cluster == " + memberCount);

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
            log.info(basename + ": After setup map size=" + map.size());

            printMemStats(basename);
        }
    }


    @Verify(global = true)
    public void verify() throws Exception {
        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        log.info(basename+": "+mapConfig);
        log.info(basename+": verify map size="+map.size());

        assertEquals(basename + ": map (" + map.getName() + ") size ", totalKeys, map.size());
    }
}