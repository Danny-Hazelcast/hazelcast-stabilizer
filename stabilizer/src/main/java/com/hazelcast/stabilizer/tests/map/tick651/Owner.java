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
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class Owner {

    private final static ILogger log = Logger.getLogger(Owner.class);

    public int threadCount = 10;
    public int keyCount = 10;
    public String basename;

    private IMap<Object, Set<String>> map;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String id;


    public int maxSetSize = 1000;
    private Object[] keys;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        log.info(id+": mapName="+map.getName());

        keys = new Object[keyCount];

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
            keys[i]=key;
            key++;
        }

        IAtomicLong total = targetInstance.getAtomicLong(basename+"total");
        total.set(keyCount);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {

        for(int i=0; i<keys.length; i++){
            Set<String> set = new HashSet<String>();

            for(int j=0; j< maxSetSize; j++){
                String s = UUID.randomUUID().toString();
                set.add(s);
            }
            map.put(keys[i], set);
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

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                int keyIdx = random.nextInt(keyCount);

                Set<String> set = map.get(keys[keyIdx]);

                if(random.nextDouble() < 0.5){
                    String s = set.iterator().next();
                    set.remove(s);
                }else{
                    set.add(UUID.randomUUID().toString());
                }

                map.set(keys[keyIdx], set);
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
