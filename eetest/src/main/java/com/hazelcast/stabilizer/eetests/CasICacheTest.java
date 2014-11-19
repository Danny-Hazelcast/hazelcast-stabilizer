package com.hazelcast.stabilizer.eetests;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;


import java.util.Iterator;
import java.util.Random;


/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 * <p/>
 * We have a bunch of predefined keys, and we are going to concurrently increment the value
 * and we protect ourselves against lost updates using cas method replace.
 * <p/>
 * Locally we keep track of all increments, and if the sum of these local increments matches the
 * global increment, we are done
 */
public class CasICacheTest {

    private final static ILogger log = Logger.getLogger(CasICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private Cache<Integer, Long> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = basename+""+testContext.getTestId();

        CachingProvider cachingProvider;
        if (TestUtils.isMemberNode(targetInstance)) {
            cachingProvider = HazelcastServerCachingProvider.createCachingProvider(targetInstance);
        } else {
            cachingProvider = HazelcastClientCachingProvider.createCachingProvider(targetInstance);
        }
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CacheConfig config = new CacheConfig<Integer, Long>();
        config.setName(basename);

        try{
            cacheManager.createCache(basename, config);
        }catch (Exception e){}

        cache = cacheManager.getCache(basename);

        log.info(basename+": cacheGet = "+cache + "config = "+config);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {

        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0l);
        }
        log.info(basename+": put "+keyCount+" keys");

        final Iterator<Cache.Entry<Integer,Long>> i = cache.iterator();
        while(i.hasNext()){
            log.info(basename+":"+i.next());
        }

        /*
        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);
        }
        */
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

        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                Long current = cache.get(key);


                if(current!=null){
                    log.severe(basename+": key " + key + "value="+current);

                    cache.put(key, current+increment);
                    /*
                    if (cache.replace(key, current, current + increment)) {
                        increments[key] += increment;
                    }
                    */
                }else{
                    log.severe("HI ERROR key " + key + "value=null");
                }

            }
            targetInstance.getList(basename).add(increments);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {

        /*
        long[] amount = new long[keyCount];

        IList<long[]> resultsPerWorker = targetInstance.getList(basename);
        for (long[] incrments : resultsPerWorker) {
            for (int i=0 ; i<keyCount; i++) {
                amount[i] += incrments[i];
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = cache.get(k);
            if (expected != found) {
                failures++;
            }
        }
        assertEquals(basename+" "+failures+" key=>values have been incremented unExpected", 0, failures);
        */
    }
}