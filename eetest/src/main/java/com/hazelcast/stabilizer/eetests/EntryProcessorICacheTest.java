package com.hazelcast.stabilizer.eetests;

import javax.cache.Cache;
import javax.cache.CacheManager;;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class EntryProcessorICacheTest {

    private final static ILogger log = Logger.getLogger(EntryProcessorICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private Cache<Integer, Long> cache;


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        CacheManager cacheManager;
        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        cache = cacheManager.getCache(basename);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0l);
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
        private final Random random = new Random();
        private final long[] increments = new long[keyCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long inc = random.nextInt(100);
                increments[key]+=inc;

                int delayMs = 0;
                if (maxProcessorDelayMs != 0) {
                    delayMs = minProcessorDelayMs + random.nextInt(maxProcessorDelayMs);
                }
                cache.invoke(key, new IncrementEntryProcessor(inc, delayMs));
            }
            //sleep to give time for the last EntryProcessor tasks to complete.
            sleepMs(2000 + maxProcessorDelayMs * 2);

            targetInstance.getList(basename).add(increments);
        }
    }

    @Verify
    public void verify() throws Exception {

        long[] amount = new long[keyCount];
        IList<long[]> resultsPerWorker = targetInstance.getList(basename);
        for (long[] incrments : resultsPerWorker) {
            for (int i=0 ; i<keyCount; i++) {
                amount[i] += incrments[i];
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (amount[k] != cache.get(k)) {
                failures++;
            }
        }
        assertEquals(basename+" Failures have been found", 0, failures);
    }


    private static class IncrementEntryProcessor implements EntryProcessor<Integer, Long, Object> , Serializable {
        private final long increment;
        private final long delayMs;

        private IncrementEntryProcessor(long increment, long delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(MutableEntry<Integer, Long> entry, Object... arguments) throws EntryProcessorException {
            delay();
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }

        private void delay() {
            if (delayMs != 0) {
                try {
                    sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        EntryProcessorICacheTest test = new EntryProcessorICacheTest();
        new TestRunner(test).run();
    }
}

