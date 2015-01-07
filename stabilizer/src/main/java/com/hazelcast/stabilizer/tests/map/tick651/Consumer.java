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
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class Consumer {

    private final static ILogger log = Logger.getLogger(Consumer.class);

    public int threadCount = 10;
    public String basename;
    public boolean modifying=true;

    private String id;

    private Object[] keys;
    private IMap<Object, Set<String>> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        log.info(id+": mapName="+map.getName());


        IAtomicLong total = targetInstance.getAtomicLong(basename+"total");
        while(total.get()==0){
            Thread.sleep(2000);
        }
        IList list = targetInstance.getList(basename + "keys");
        keys = list.toArray();

        for(Object k : keys){
            log.info(id+": key = "+k);
        }
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
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                int idx = random.nextInt(keys.length);
                Set<String> set =map.get(keys[idx]);

                if(modifying){
                    if(random.nextDouble() < 0.5){
                        String s = set.iterator().next();
                        set.remove(s);
                    }else{
                        set.add(UUID.randomUUID().toString());
                    }
                    map.set(keys[idx], set);
                }
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