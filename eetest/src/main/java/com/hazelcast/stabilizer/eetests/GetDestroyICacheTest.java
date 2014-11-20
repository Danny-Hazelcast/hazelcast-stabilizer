package com.hazelcast.stabilizer.eetests;


import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.stabilizer.eetests.Utils.MemoryStatsUtil;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Random;

/*
* we use this test to check for memory leaks in getting closing destroying caches
* */
public class GetDestroyICacheTest {

    private final static ILogger log = Logger.getLogger(GetDestroyICacheTest.class);

    public int threadCount = 3;
    public double getCacheProb=0.4;
    public double closeCacheProb=0.3;
    public double destroyCacheProb=0.3;
    public int valueSize=100000;
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;

    private byte[] value;

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
        cacheManager = cachingProvider.getCacheManager();

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);
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
        private final Counter counter = new Counter();

        public void run() {

            while (!testContext.isStopped()) {
                double chance = random.nextDouble();
                if ((chance -= getCacheProb) < 0) {
                    try{
                        Cache cache = cacheManager.getCache(basename);
                        if(cache!=null){
                            int key = random.nextInt();
                            try{
                                cache.put(key, value);
                                counter.putCache++;
                            }catch (CacheException e) {
                                counter.putCacheException++;
                            }
                        }
                        counter.getCache++;
                    } catch (IllegalStateException e){
                        counter.getCacheException++;
                    }
                } else if ((chance -= closeCacheProb) < 0){
                    try{
                        Cache cache = cacheManager.getCache(basename);
                        if(cache!=null){
                            try{
                            cache.close();
                            } catch (IllegalStateException e){
                                counter.closeCacheException++;
                            }
                            counter.closeCache++;
                        }
                    } catch (IllegalStateException e){
                        counter.getCacheException++;
                    }
                } else if ((chance -= destroyCacheProb) < 0) {
                    try{
                        cacheManager.destroyCache(basename);
                        counter.destroyCache++;
                    } catch (IllegalStateException e){
                        counter.destroyCacheException++;
                    }
                }
            }
            targetInstance.getList(basename).add(counter);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);
        }

        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    public static class Counter implements Serializable {

        public long getCache = 0;
        public long putCache = 0;
        public long closeCache=0;
        public long destroyCache = 0;

        public long getCacheException = 0;
        public long putCacheException = 0;
        public long closeCacheException = 0;
        public long destroyCacheException = 0;

        public void add(Counter c) {
            getCache += c.getCache;
            putCache += c.putCache;
            closeCache += c.closeCache;
            destroyCache += c.destroyCache;

            getCacheException += c.getCacheException;
            putCacheException += c.putCacheException;
            closeCacheException += c.closeCacheException;
            destroyCacheException += c.destroyCacheException;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "getCache=" + getCache +
                    ", putCache=" + putCache +
                    ", closeCache=" + closeCache +
                    ", destroyCache=" + destroyCache +
                    ", getCacheException=" + getCacheException +
                    ", putCacheException=" + putCacheException +
                    ", closeCacheException=" + closeCacheException +
                    ", destroyCacheException=" + destroyCacheException +
                    '}';
        }
    }
}