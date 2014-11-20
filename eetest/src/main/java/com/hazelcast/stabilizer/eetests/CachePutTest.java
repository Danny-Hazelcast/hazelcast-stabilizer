package com.hazelcast.stabilizer.eetests;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
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
import org.HdrHistogram.IntCountsHistogram;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Random;

public class CachePutTest {
    private final static ILogger log = Logger.getLogger(CachePutTest.class);

    public int threadCount = 3;
    public int maxValueLength = 1000000; //1MB
    public int minValueLength = 10000;   //0.01Mb
    public int durationSec = 1;
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

        CacheConfig config = new CacheConfig();
        config.setName(basename);

        try{
            cacheManager.createCache(basename, config);
        }catch (Exception e){}


        cache = cacheManager.getCache(basename);
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

        /*
        for(int i=1; i<threadCount; i++){
            workers[0].putLatencyHisto.add(workers[i].putLatencyHisto);
        }
        targetInstance.getList(basename+"putHisto").add(workers[0].putLatencyHisto);
        */
    }

    private class Worker implements Runnable {
        //IntCountsHistogram putLatencyHisto = new IntCountsHistogram(1, 1000*30, 0);
        Random random = new Random();

        public void run() {
            while(!testContext.isStopped()){
                int key = random.nextInt();
                int size = random.nextInt(maxValueLength-minValueLength) + minValueLength;
                byte[] bytes = new byte[size];
                random.nextBytes(bytes);

                //long start = System.currentTimeMillis();
                cache.put(key, bytes);
                //long stop = System.currentTimeMillis();
                //putLatencyHisto.recordValue(stop - start);
            }
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {

        if ( TestUtils.isMemberNode(targetInstance) ){
            LocalMemoryStats memoryStats = MemoryStatsUtil.getMemoryStats(targetInstance);
            log.info(basename+": "+memoryStats);
        }


        CacheSimpleConfig cacheConfig = targetInstance.getConfig().getCacheConfig(basename);
        log.info(basename+": "+cacheConfig);

        /*
        IList<IntCountsHistogram> putHistos = targetInstance.getList(basename+"putHisto");

        IntCountsHistogram putHisto = putHistos.get(0);

        for(int i=1; i<putHistos.size(); i++){
            putHisto.add(putHistos.get(i));
        }

        System.out.println(basename + ": Put Latency Histogram");
        putHisto.outputPercentileDistribution(System.out, 1.0);
        double putsPerSec = putHisto.getTotalCount() / durationSec;


        log.info(basename+": puts ="+putHisto.getTotalCount());
        log.info(basename+": avg put/sec ="+putsPerSec);
        */
    }
}