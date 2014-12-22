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
package com.hazelcast.stabilizer.tests.map.tick651;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class Owner {

    private final static ILogger log = Logger.getLogger(Owner.class);

    public int threadCount = 1;
    public int keyCount = 3;
    public String basename;

    private IMap<Object, List<byte[]>> map;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String id;


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        log.info(id+": mapName="+map.getName());

        final PartitionService partitionService = targetInstance.getPartitionService();
        final Set<Partition> partitionSet = partitionService.getPartitions();
        for (Partition partition : partitionSet) {
            while (partition.getOwner() == null) {
                Thread.sleep(1000);
            }
        }

        long key=0;
        for(int i=0; i<keyCount; i++){
            key = TestUtils.nextKeyOwnedBy(key, targetInstance);

            targetInstance.getList(basename+"keys").add(key);

            log.info(id+": key = "+key);
            key++;
        }

        IAtomicLong total = targetInstance.getAtomicLong(basename+"total");
        total.set(keyCount);
    }

    @Warmup(global = true)
    public void warmup() throws InterruptedException {

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
            while (!testContext.isStopped()) {

                sleepMs(5000);

                //for(Object o : map.localKeySet()){
                //    log.info(id+": local key = "+o);
                //}
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        for(Object o : map.localKeySet()){
            log.info(id+": local key = "+o);
        }
    }
}
