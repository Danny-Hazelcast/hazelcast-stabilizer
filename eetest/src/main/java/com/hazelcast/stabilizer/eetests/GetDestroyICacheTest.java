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
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.Serializable;
import java.util.Random;

public class GetDestroyICacheTest {

    private final static ILogger log = Logger.getLogger(GetDestroyICacheTest.class);

    public int threadCount = 3;
    public double getCacheProb=0.4;
    public double closeCacheProb=0.3;
    public double destroyCacheProb=0.3;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = basename+""+testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
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
        private final CacheConfig config = new CacheConfig();
        private final Counter counter = new Counter();

        public void run() {
            config.setName(basename);

            while (!testContext.isStopped()) {
                double chance = random.nextDouble();
                if ((chance -= getCacheProb) < 0) {
                    try{
                        Cache cache = cacheManager.getCache(basename);

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

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    public static class Counter implements Serializable {

        public long getCache = 0;
        public long closeCache=0;
        public long destroyCache = 0;

        public long getCacheException = 0;
        public long closeCacheException = 0;
        public long destroyCacheException = 0;

        public void add(Counter c) {
            getCache += c.getCache;
            closeCache += c.closeCache;
            destroyCache += c.destroyCache;

            getCacheException += c.getCacheException;
            closeCacheException += c.closeCacheException;
            destroyCacheException += c.destroyCacheException;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "getCache=" + getCache +
                    ", closeCache=" + closeCache +
                    ", destroyCache=" + destroyCache +
                    ", getCacheException=" + getCacheException +
                    ", closeCacheException=" + closeCacheException +
                    ", destroyCacheException=" + destroyCacheException +
                    '}';
        }
    }
}