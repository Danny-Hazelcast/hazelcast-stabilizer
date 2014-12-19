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
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Moder {

    private final static ILogger log = Logger.getLogger(Moder.class);

    public int threadCount = 10;
    public int keyCount = 3;
    public String basename;

    private IMap<Integer, List<Byte[]>> map;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private List<Byte[]> values = new ArrayList<Byte[]>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        Random random = new Random();

        for(int i=0; i<100; i++){
            Byte[] b = new Byte[ 1000 + random.nextInt(1000)];
            values.add(b);
        }
    }

    @Warmup(global = true)
    public void warmup() throws InterruptedException {

        Random random = new Random();


        for(int i=0; i<keyCount; i++){
            List<Byte[]> list = new ArrayList();

            int max = 100+random.nextInt(100);
            for(int j= 0; j<max; j++){
                int idx = random.nextInt(values.size());
                list.add(values.get(idx));
            }

            map.put(i, list);
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

            List<Byte[]> list = new ArrayList();

            while (!testContext.isStopped()) {
                    int key = random.nextInt(keyCount);

                    list.clear();

                    int max = 100+random.nextInt(100);
                    for(int j= 0; j<max; j++){
                        int idx = random.nextInt(values.size());
                        list.add(values.get(idx));
                    }

                    map.put(key, list);
            }
        }

    }

}
