/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.IOException;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;

public class SqlPredicateTest {

    private static final ILogger LOGGER = Logger.getLogger(SqlPredicateTest.class);
    private static final String[] NAMES = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};

    // properties
    public String basename = SqlPredicateTest.class.getSimpleName();
    public int keyCount = 10000;
    public int keyLength = 10;
    public String sql = "age = 30 AND active = true";
    public MetronomeType metronomeType = MetronomeType.SLEEPING;
    public int intervalMs = 0;
    public int maxAge = 75;
    public double maxSalary = 1000.0;

    private IMap<String, DataSerializableEmployee> map;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) {
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename);
    }

    @Teardown
    public void teardown() {
        map.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = true)
    public void warmup() {
        Random random = new Random();
        Streamer<String, DataSerializableEmployee> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            String key = generateString(keyLength);
            DataSerializableEmployee value = generateRandomEmployee(random);
            streamer.pushEntry(key, value);
        }
        streamer.await();
        LOGGER.info("Map size is: " + map.size());
        LOGGER.info("Map localKeySet size is: " + map.localKeySet().size());
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private SqlPredicate sqlPredicate = new SqlPredicate(sql);
        private Metronome metronome = withFixedIntervalMs(intervalMs, metronomeType);

        @Override
        protected void timeStep() throws Exception {
            metronome.waitForNext();
            map.values(sqlPredicate);
        }
    }

    private DataSerializableEmployee generateRandomEmployee(Random random) {
        int id = random.nextInt();
        String name = NAMES[random.nextInt(NAMES.length)];
        int age = random.nextInt(maxAge);
        boolean active = random.nextBoolean();
        double salary = random.nextDouble() * maxSalary;
        return new DataSerializableEmployee(id, name, age, active, salary);
    }

    public static class DataSerializableEmployee implements DataSerializable {

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;

        @SuppressWarnings("unused")
        public DataSerializableEmployee() {
        }

        public DataSerializableEmployee(int id, String name, int age, boolean live, double salary) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = salary;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getSalary() {
            return salary;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
            objectDataOutput.writeInt(id);
            objectDataOutput.writeUTF(name);
            objectDataOutput.writeInt(age);
            objectDataOutput.writeBoolean(active);
            objectDataOutput.writeDouble(salary);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            id = objectDataInput.readInt();
            name = objectDataInput.readUTF();
            age = objectDataInput.readInt();
            active = objectDataInput.readBoolean();
            salary = objectDataInput.readDouble();
        }

        @Override
        public String toString() {
            return "Employee{"
                    + "id=" + id
                    + ", name='" + name + '\''
                    + ", age=" + age
                    + ", active=" + active
                    + ", salary=" + salary
                    + '}';
        }
    }
}
