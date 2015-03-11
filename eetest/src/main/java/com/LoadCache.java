package com;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.*;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.Arrays;
import java.util.Random;

public class LoadCache {
    private final static ILogger log = Logger.getLogger(LoadCache.class);

    public int threadCount=10;
    public int totalCaches=4;
    public int valueByteArraySize = 3000;

    public String cacheBaseName=null;

    private CacheManager cacheManager;

    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public static boolean isMemberNode(HazelcastInstance instance) {
        return instance instanceof HazelcastInstanceProxy;
    }

    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();

        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        for(int i=0; i<totalCaches; i++){
            makeCache(cacheBaseName+i);
        }
    }

    public void makeCache(String name){
        CacheConfig config = new CacheConfig();
        config.setName(name);
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setAsyncBackupCount(1);
        config.setBackupCount(1);
        CacheEvictionConfig evict = new CacheEvictionConfig();
        evict.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
        evict.setSize(99);
        evict.setEvictionPolicy(EvictionPolicy.LRU);
        config.setEvictionConfig(evict);

        log.info(id + ": config="+config);

        try {
            cacheManager.createCache(cacheBaseName, config);
        } catch (CacheException e) {
            //throw new RuntimeException(e);
        }
    }


    @Warmup(global = false)
    public void warmup() throws InterruptedException {

        for(int i=0; i<totalCaches; i++){
            ICache cache;
            do {
                Thread.sleep(500);
                cache = (ICache) cacheManager.getCache(cacheBaseName + i);
            }while(cache==null);
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

        public Random random = new Random();
        private byte[] value;

        public void run(){
            value = new byte[valueByteArraySize];
            random.nextBytes(value);

            while (!testContext.isStopped()) {
                updateDynamicCaches();
            }
        }

        public void updateDynamicCaches(){
            int i = random.nextInt(totalCaches);
            long k = random.nextLong();

            ICache cache = (ICache) cacheManager.getCache(cacheBaseName+i);
            cache.put(k, value);

            byte[] v = (byte[]) cache.get(k);

            /*
            if ( Arrays.equals(v, value) ){
            //    log.info(id + "put get MisMatch");
            }else{
            //    log.info(id + "SAME");
            }
            */
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        if(isMemberNode(targetInstance)){
            log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        }
        printInfo();
    }

    public void printInfo(){
        for(int i=0; i< totalCaches; i++){
            ICache cache  = (ICache) cacheManager.getCache(cacheBaseName+i);
            log.info(id + ": mapName=" + cache.getName() + " size=" + cache.size());
        }
        log.info(id + ": valueByteArraySize="+valueByteArraySize);
    }
}