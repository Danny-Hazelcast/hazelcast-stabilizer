package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.nextKeyOwnedBy;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.isMemberNode;

import java.util.concurrent.TimeUnit;

public class MapHeapHogTest {

    private final static ILogger log = Logger.getLogger(MapHeapHogTest.class);

    public int threadCount = 3;
    public int memberCount = 4;
    public double approxHeapUsageFactor = 0.8;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    private long approxEntryBytesSize = 238;
    private IMap map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        map = targetInstance.getMap(basename);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        if (isMemberNode(targetInstance)) {
            while (targetInstance.getCluster().getMembers().size() != memberCount) {
                Thread.sleep(1000);
            }
            log.info(basename + ": "+memberCount+" member cluster formed");
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }


    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {

                if (getHeapUsageFactor() < approxHeapUsageFactor ) {
                    long totalFree = getTotalFree();
                    long maxLocalEntries = (long) ((totalFree / approxEntryBytesSize) * approxHeapUsageFactor);

                    maxLocalEntries = maxLocalEntries / threadCount;

                    long key = 0;
                    for (int i = 0; i < maxLocalEntries; i++) {
                        key = nextKeyOwnedBy(key, targetInstance);
                        map.put(key, key);
                        key++;
                    }

                    printMemStats();

                    if( getHeapUsageFactor() >= approxHeapUsageFactor ) {
                        log.info(basename +": Hit Target Usage Factor used = "+getHeapUsageFactor());
                    }
                }
            }
        }

        public double getHeapUsageFactor(){
            long free = Runtime.getRuntime().freeMemory();
            long total = Runtime.getRuntime().totalMemory();
            long used = total - free;
            long max = Runtime.getRuntime().maxMemory();
            return (double) used / (double) max;
        }

        public long getTotalFree(){
            long free = Runtime.getRuntime().freeMemory();
            long total = Runtime.getRuntime().totalMemory();
            long used = total - free;
            long max = Runtime.getRuntime().maxMemory();
            return  max - used;
        }
    }

    @Verify(global = false)
    public void localVerify() throws Exception {
        if (isMemberNode(targetInstance)) {
            printMemStats();
        }
    }

    public void printMemStats() {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) max);

        long totalFree = max - used;

        log.info(basename + ": free = " + TestUtils.humanReadableByteCount(free, true) + " = " + free);
        log.info(basename + ": total free = " + TestUtils.humanReadableByteCount(totalFree, true) + " = " + totalFree);
        log.info(basename + ": used = " + TestUtils.humanReadableByteCount(used, true) + " = " + used);
        log.info(basename + ": max = " + TestUtils.humanReadableByteCount(max, true) + " = " + max);
        log.info(basename + ": usedOfMax = " + usedOfMax + "%");
        log.info(basename + ": ==============");
    }
}
