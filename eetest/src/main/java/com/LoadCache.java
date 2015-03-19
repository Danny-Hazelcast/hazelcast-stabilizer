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
import javassist.bytecode.ByteArray;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.*;

public class LoadCache {
    private final static ILogger log = Logger.getLogger(LoadCache.class);

    public boolean createCaches=true;
    public int threadCount=10;
    public int totalCaches=4;

    public boolean dynamicValueSizes=false;
    public int valueByteArraySize=3000;


    public int progConfigEvectionPer=60;

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

        if(createCaches){
            makeCaches();
        }
    }

    public void makeCaches(){
        for(int i=0; i<totalCaches; i++){
            makeCache(cacheBaseName+i);
        }
    }

    public void makeCache(String name){
        CacheConfig config = new CacheConfig();
        config.setName(name);
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setAsyncBackupCount(0);
        config.setBackupCount(0);
        CacheEvictionConfig evict = new CacheEvictionConfig();
        evict.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evict.setSize(progConfigEvectionPer);
        evict.setEvictionPolicy(EvictionPolicy.LRU);
        config.setEvictionConfig(evict);

        try {
            cacheManager.createCache(name, config);
        } catch (CacheException e) { }
    }


    @Warmup(global = false)
    public void warmup() throws InterruptedException {

        for(int i=0; i<totalCaches; i++){
            ICache cache;
            do {
                Thread.sleep(500);
                cache = (ICache) cacheManager.getCache(cacheBaseName + i);
            }while(cache==null);
            log.info(id + ":  cacheBaseName="+cacheBaseName+""+i);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        Set<Worker> s = new HashSet();
        for (int k = 0; k < threadCount; k++) {
            Worker w = new Worker();
            spawner.spawn(w);
            s.add(w);
        }
        spawner.awaitCompletion();

        long total=0;
        for(Worker w : s){
            total += w.putCount;
            if(dynamicValueSizes){
                log.info(id + ": w.valueSet.size()=" + w.valueSet.size());
                for(byte[] b : w.valueSet){
                    log.info(id + ": valuesSet Value v size="+ b.length);
                }
            }
        }
        log.info(id + ": total puts="+total);
    }


    private class Worker implements Runnable {

        public long putCount=0;
        public Random random = new Random();
        public List<byte[]> valueSet = new ArrayList<byte[]>();
        public Map mapValueSet = new HashMap();

        public Worker(){
            byte[] value = new byte[valueByteArraySize];
            random.nextBytes(value);
            valueSet.add(value);

            if(dynamicValueSizes){
                for(int i=0; i<10; i++){
                    int diff = random.nextInt(valueByteArraySize/2);
                    value = new byte[valueByteArraySize + diff];
                    random.nextBytes(value);
                    valueSet.add(value);
                    mapValueSet.put(random.nextInt(), value);
                }
            }
        }

        public void run(){
            while (!testContext.isStopped()) {
                int i = random.nextInt(totalCaches);
                long k = random.nextLong();
                int valueIdx = random.nextInt(valueSet.size());

                ICache cache = (ICache) cacheManager.getCache(cacheBaseName+i);


                byte[] putValue = valueSet.get(valueIdx);

                cache.put(k, putValue);

                /*
                if(random.nextDouble()<0.1){
                    cache.putAsync(k, putValue);
                }else {
                    cache.put(k, putValue);
                }

                if(random.nextDouble()<0.05){
                    cache.putAll(mapValueSet);
                }
                */

                byte[] getValue = (byte[]) cache.get(k);


                putCount++;
            }
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

            CacheConfig config = (CacheConfig) cache.getConfiguration(CacheConfig.class);
            log.info(id + ": config="+config);
            log.info(id + ": getAsyncBackupCount="+config.getAsyncBackupCount());

            CacheEvictionConfig evictionConfig = config.getEvictionConfig();
            log.info(id + ": evictionConfig="+evictionConfig);
            log.info(id + ": evictionConfig.getEvictionPolicy()="+evictionConfig.getEvictionPolicy());
            log.info(id + ": evictionConfig.getMaxSizePolicy()="+evictionConfig.getMaxSizePolicy());
            log.info(id + ": evictionConfig.getSize()="+evictionConfig.getSize());
            log.info(id + ": evictionConfig.getEvictionStrategyType()="+evictionConfig.getEvictionStrategyType());
        }
    }
}