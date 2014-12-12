/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;

import javax.cache.Cache;
import javax.cache.CacheManager;;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.humanReadableByteCount;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class ExpiryICacheTest {

    private final static ILogger log = Logger.getLogger(ExpiryICacheTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private ICache<Object, Object> cache;


    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);

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

        CacheConfig<Long, Long> config = new CacheConfig<Long, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            log.severe(hack);
        }
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);

        log.info(basename+": "+cache.getName()+" config="+config);
        log.info(basename+": "+cache.getName()+"Evition config="+config.getEvictionConfig());
        log.info(basename+": "+cache.getName()+" size="+cache.size());

    }


    private double heapUsedPercentage() {
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();
        return (100d * total) / max;
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
        @Override
        public void run() {
            Random random = new Random();

            while (!testContext.isStopped()) {

                int key = random.nextInt();
                cache.put(key, key, expiryPolicy);

                if(random.nextDouble() < 0.1){


                    log.info(basename+": "+cache.getName()+" size="+cache.size());

                    long free = Runtime.getRuntime().freeMemory();
                    long total = Runtime.getRuntime().totalMemory();
                    long used = total - free;
                    long maxBytes = Runtime.getRuntime().maxMemory();
                    double usedOfMax = 100.0 * ((double) used / (double) maxBytes);

                    log.info(basename + " before Init");
                    log.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
                    log.info(basename + " used = " + humanReadableByteCount(used, true) + " = " + used);
                    log.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
                    log.info(basename + " usedOfMax = " + usedOfMax + "%");

                }

            }

            log.info(basename+": "+cache.getName()+" size="+cache.size());

            long free = Runtime.getRuntime().freeMemory();
            long total = Runtime.getRuntime().totalMemory();
            long used = total - free;
            long maxBytes = Runtime.getRuntime().maxMemory();
            double usedOfMax = 100.0 * ((double) used / (double) maxBytes);

            log.info(basename + " before Init");
            log.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
            log.info(basename + " used = " + humanReadableByteCount(used, true) + " = " + used);
            log.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
            log.info(basename + " usedOfMax = " + usedOfMax + "%");
        }
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {

    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new ExpiryICacheTest()).run();
    }
}
