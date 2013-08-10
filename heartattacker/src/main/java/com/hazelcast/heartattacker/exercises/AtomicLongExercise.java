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
package com.hazelcast.heartattacker.exercises;


import com.hazelcast.core.IAtomicLong;
import com.hazelcast.heartattacker.performance.OperationsPerSecond;
import com.hazelcast.heartattacker.performance.Performance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class AtomicLongExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(AtomicLongExercise.class);

    private int countersLength = 1000;
    private int threadCount = 1;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;
    private AtomicLong operations = new AtomicLong();

    @Override
    public void localSetup() {
        log.info("countersLength:" + countersLength + " threadCount:" + threadCount);

        totalCounter = hazelcastInstance.getAtomicLong(exerciseId + ":TotalCounter");
        counters = new IAtomicLong[countersLength];
        for (int k = 0; k < counters.length; k++) {
            counters[k] = hazelcastInstance.getAtomicLong(exerciseId + ":Counter-" + k);
        }

        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long count = 0;
        for (IAtomicLong counter : counters) {
            count += counter.get();
        }

        if (expectedCount != count) {
            throw new RuntimeException("Expected count: " + expectedCount + " but found count was: " + count);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
    }

    @Override
    public Performance calcPerformance() {
        OperationsPerSecond performance = new OperationsPerSecond();
        performance.setStartMs(getStartTimeMs());
        performance.setEndMs(getCurrentTimeMs());
        performance.setOperations(operations.get());
        return performance;
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stop) {
                int index = random.nextInt(counters.length);
                counters[index].incrementAndGet();
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if(iteration % performanceUpdateFrequency == 0){
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            totalCounter.addAndGet(iteration);
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLongExercise mapExercise = new AtomicLongExercise();
        new ExerciseRunner().run(mapExercise, 60);
    }
}

