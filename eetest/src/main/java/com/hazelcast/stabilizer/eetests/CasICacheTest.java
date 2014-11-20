package com.hazelcast.stabilizer.eetests;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.stabilizer.eetests.Utils.MemoryStatsUtil;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;


import java.util.Random;


public class CasICacheTest {

    private final static ILogger log = Logger.getLogger(CasICacheTest.class);

    public int threadCount = 3;
    public int keyCount = 1000;
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private Cache<Integer, Long> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        CachingProvider cachingProvider;
        if (TestUtils.isMemberNode(targetInstance)) {
            cachingProvider = HazelcastServerCachingProvider.createCachingProvider(targetInstance);
        } else {
            cachingProvider = HazelcastClientCachingProvider.createCachingProvider(targetInstance);
        }
        CacheManager cacheManager = cachingProvider.getCacheManager();

        cache = cacheManager.getCache(basename);

        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);

            CacheSimpleConfig cacheConfig = targetInstance.getConfig().getCacheConfig(basename);
            log.info(basename+": "+cacheConfig);
            log.info(basename+": "+cacheConfig.getInMemoryFormat());
        }
    }

    @Warmup(global = true)
    public void warmup() throws Exception {

        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0l);
        }
        log.info(basename+": put "+keyCount+" keys");

        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);
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

        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                Long current = cache.get(key);
                if(current!=null){
                    cache.replace(key, current, current + increment);
                }
            }
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);
        }
    }
}