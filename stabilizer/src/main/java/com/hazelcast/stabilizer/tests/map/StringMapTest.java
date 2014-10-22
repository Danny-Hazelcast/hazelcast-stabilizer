/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.isMemberNode;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.nextKeyOwnedBy;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class StringMapTest {
    private final static ILogger log = Logger.getLogger(StringMapTest.class);

    public int valueLength = 20000;
    public int localKeyCount = 1000;
    public int globalKeyCount = -1;

    public int memberCount = 3;
    public int logMs = 10000;
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap map;
    private long minUsed=Long.MAX_VALUE;
    private long maxUsed=Long.MIN_VALUE;

    @Setup
    public void setup(TestContext testContext) throws Exception {

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {

        while (targetInstance.getCluster().getMembers().size() != memberCount) {
            Thread.sleep(1000);
        }
        log.info(basename + ": "+memberCount+" member cluster formed");

        PartitionService partitionService = targetInstance.getPartitionService();
        for (Partition partition : partitionService.getPartitions()) {
            while (partition.getOwner() == null) {
                Thread.sleep(1000);
            }
        }
        log.info(basename + ": Partitions assigned");


        String value = StringUtils.generateString(valueLength);
        long key=0;
        if(globalKeyCount!=-1){
            while(map.size() < globalKeyCount){
                key = nextKeyOwnedBy(key, targetInstance);
                map.put(key, value);
            }
        }else{
            for(int i=0; i < localKeyCount; i++){
                key = nextKeyOwnedBy(key, targetInstance);
                map.put(key, value);
            }
        }

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        log.info(basename+": map size = "+map.size());
        log.info(basename+":"+" "+mapConfig);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        spawner.spawn(new Worker());
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        public void run() {
            while (!testContext.isStopped()) {
                sleepMs(logMs);
                printMemStats();
            }
        }
    }

    @Verify(global = false)
    public void localVerify() throws Exception {
        log.info(basename + ": map size = " + map.size());
        log.info(basename + ": max used = " + TestUtils.humanReadableByteCount(maxUsed, true));
        log.info(basename + ": min used = " + TestUtils.humanReadableByteCount(minUsed, true));
    }

    public void printMemStats() {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) max);

        long totalFree = max - used;

        if(used < minUsed){
            minUsed = used;
        }
        if(used > maxUsed){
            maxUsed = used;
        }

        log.info(basename + ": free = " + TestUtils.humanReadableByteCount(free, true));
        log.info(basename + ": total free = " + TestUtils.humanReadableByteCount(totalFree, true));
        log.info(basename + ": used = " + TestUtils.humanReadableByteCount(used, true));
        log.info(basename + ": max = " + TestUtils.humanReadableByteCount(max, true));
        log.info(basename + ": usedOfMax = " + usedOfMax + "%");
    }
}
