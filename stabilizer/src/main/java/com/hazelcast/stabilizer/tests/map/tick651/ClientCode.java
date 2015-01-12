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

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class ClientCode {

    private final static ILogger log = Logger.getLogger(ClientCode.class);

    public int threadCount = 10;
    public String basename;
    public boolean modifying=true;
    public int maxValueSize=1000;

    private String id;
    private Object[] keys;
    private IMap<Object, Set<String>> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private long sets=0;
    private long gets=0;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        log.info(id+": mapName="+map.getName());


        IAtomicLong owners = targetInstance.getAtomicLong(basename+"owners");
        while(owners.get()==0){
            Thread.sleep(250);
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

        Worker[] workers = new Worker[threadCount];

        for (int k = 0; k < threadCount; k++) {
            workers[k]=new Worker();
            spawner.spawn(workers[k]);
        }
        spawner.awaitCompletion();

        for (int k = 0; k < threadCount; k++) {
            gets += workers[k].gets;
            sets += workers[k].sets;
        }

    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        public long gets=0;
        public long sets=0;

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                Object key = keys[random.nextInt(keys.length)];
                Set<String> set = map.get(key);
                gets++;

                int round=0;
                if(modifying){

                    if(set==null || set.isEmpty()){
                        set = new HashSet<String>();

                        for(int j=0; j< maxValueSize; j++){
                            String s = UUID.randomUUID().toString();
                            set.add(s);
                        }
                    }

                    if(random.nextDouble() < 0.5){
                        String s = set.iterator().next();
                        set.remove(s);
                    }else{

                        byte[] a = new byte[100];
                        random.nextBytes(a);

                        String s;



                        switch (round){

                            case 0:
                        s = new String(a, StandardCharsets.US_ASCII);
                            break;

                            case 1:
                        s = new String(a, StandardCharsets.ISO_8859_1);
                            break;

                            case 2:
                        s = new String(a, StandardCharsets.UTF_16);
                            break;

                            case 3:
                        s = new String(a, StandardCharsets.UTF_16BE);
                            break;

                            case 4:
                        s = new String(a, StandardCharsets.UTF_16LE);
                            break;

                            default:
                        s = new String(a, StandardCharsets.UTF_8);
                            break;
                        }
                        round= ++round % 5;

                        set.add(s);

                        //set.add(UUID.randomUUID().toString());
                    }

                    map.set(key, set);
                    sets++;
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        log.info(id+": sets="+sets+" gets="+gets);

    }
}