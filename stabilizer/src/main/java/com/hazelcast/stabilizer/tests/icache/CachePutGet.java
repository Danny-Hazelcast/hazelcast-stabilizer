package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import org.HdrHistogram.IntHistogram;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Random;


/* <native-memory allocator-type="POOLED" enabled="true">
        <size unit="GIGABYTES" value="5" />
        <metadata-space-percentage>5</metadata-space-percentage>
        <min-block-size>16</min-block-size>
        <page-size></page-size>
    </native-memory>
*/
public class CachePutGet {
    private final static ILogger log = Logger.getLogger(CachePutGet.class);

    public int threadCount = 3;
    public int valueLength = 1000;
    public int totalKeys = 10000;
    public int jitWarmUpMs = 1000*30;
    public int durationMs = 1000*60;
    public double putProb = 0.5;

    public String basename;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private byte[] value;

    private Cache<Object, Object> cache;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        basename = basename+""+testContex.getTestId();

        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(targetInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        cache = cacheManager.getCache(basename);

        //doing the initilization of cache data hear, so the members can put key's they own even if clients are envolved in the test, speeding things up
        value = new byte[valueLength];
        Random random = new Random();
        random.nextBytes(value);

        if(TestUtils.isMemberNode(targetInstance)){
            TestUtils.warmupPartitions(log, targetInstance);

            final Member localMember = targetInstance.getCluster().getLocalMember();
            final PartitionService partitionService = targetInstance.getPartitionService();

            for(int i=0; i<totalKeys; i++){
                Partition partition = partitionService.getPartition(i);
                if (localMember.equals(partition.getOwner())) {
                    cache.put(i, value);
                }
            }
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

        for(int i=1; i<threadCount; i++){
            workers[0].putLatencyHisto.add(workers[i].putLatencyHisto);
            workers[0].getLatencyHisto.add(workers[i].getLatencyHisto);
        }

        targetInstance.getList(basename+"putHisto").add(workers[0].putLatencyHisto);
        targetInstance.getList(basename+"getHisto").add(workers[0].getLatencyHisto);
    }

    private class Worker implements Runnable {
        IntHistogram putLatencyHisto = new IntHistogram(1, 1000*30, 0);
        IntHistogram getLatencyHisto = new IntHistogram(1, 1000*30, 0);
        Random random = new Random();

        public void run() {
            test(jitWarmUpMs);
            putLatencyHisto.reset();
            getLatencyHisto.reset();
            test(durationMs);
        }

        private void test(long maxTime){
            long runStart = System.currentTimeMillis();
            long now;
            do{
                int key = random.nextInt(totalKeys);

                if(random.nextDouble() < putProb){
                    long start = System.currentTimeMillis();
                    cache.put(key, value);
                    long stop = System.currentTimeMillis();
                    putLatencyHisto.recordValue(stop - start);
                }else{
                    long start = System.currentTimeMillis();
                    cache.get(key);
                    long stop = System.currentTimeMillis();
                    getLatencyHisto.recordValue(stop - start);
                }

                now = System.currentTimeMillis();
            }while(now - runStart < maxTime);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {

        CacheSimpleConfig cacheConfig = targetInstance.getConfig().getCacheConfig(basename);
        log.info(basename+": "+cacheConfig);

        IList<IntHistogram> putHistos = targetInstance.getList(basename+"putHisto");
        IList<IntHistogram>  getHistos = targetInstance.getList(basename+"getHisto");

        IntHistogram putHisto = putHistos.get(0);
        IntHistogram getHisto = getHistos.get(0);

        for(int i=1; i<putHistos.size(); i++){
            putHisto.add(putHistos.get(i));
        }
        for(int i=1; i<getHistos.size(); i++){
            getHisto.add(getHistos.get(i));
        }

        System.out.println(basename + ": Put Latency Histogram");
        putHisto.outputPercentileDistribution(System.out, 1.0);
        double putsPerSec = putHisto.getTotalCount() / (durationMs/1000);

        System.out.println(basename + ": Get Latency Histogram");
        getHisto.outputPercentileDistribution(System.out, 1.0);
        double getPerSec = getHisto.getTotalCount() / (durationMs/1000);

        log.info(basename+": put/sec ="+putsPerSec);
        log.info(basename+": get/Sec ="+getPerSec);
    }
}