package com.hazelcast.stabilizer.eetests;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.HazelcastInstance;
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
import javax.cache.spi.CachingProvider;
import java.util.Random;


/**
 * Unconstrained put test.  off heap cache has a eviction configured by default
 * this test should never throw an out of memory exception.  we also have a cache
 * configured in the hazelcast xml offHeapEvictionCache*,  which we can use for this test.
 */
public class CachePutTest {
    private final static ILogger log = Logger.getLogger(CachePutTest.class);

    public int threadCount = 3;
    public int maxValueLength = 1000000; //1MB
    public int minValueLength = 10000;   //0.01Mb
    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private Cache<Object, Object> cache;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
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
            CacheSimpleConfig cacheConfig = targetInstance.getConfig().getCacheConfig(basename);
            log.info(basename+": "+cacheConfig);
            log.info(basename+": "+cacheConfig.getInMemoryFormat());
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
    }

    private class Worker implements Runnable {
        Random random = new Random();

        public void run() {
            while(!testContext.isStopped()){
                int key = random.nextInt();
                int size = random.nextInt(maxValueLength-minValueLength) + minValueLength;
                byte[] bytes = new byte[size];
                random.nextBytes(bytes);

                cache.put(key, bytes);
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
    }
}