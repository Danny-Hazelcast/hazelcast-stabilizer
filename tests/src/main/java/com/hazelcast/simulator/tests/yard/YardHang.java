package com.hazelcast.simulator.tests.yard;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;

import java.util.Random;


public class YardHang {

    private static final ILogger log = Logger.getLogger(YardHang.class);

    public int threadCount = 16;
    public int keyDomainMax = 10000;


    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String id;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id = testContext.getTestId();
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {

    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            TestThread integrityThread = new TestThread();
            spawner.spawn(integrityThread);
        }
        spawner.awaitCompletion();
    }

    private class TestThread implements Runnable {
        private final Random random = new Random();

        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt(keyDomainMax);

                // Repeatable read isolation level is always used.
                TransactionOptions txOpts = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

                TransactionContext tCtx = targetInstance.newTransactionContext(txOpts);

                tCtx.beginTransaction();

                TransactionalMap<Object, Object> txMap = tCtx.getMap(id);

                try {
                    Object val = txMap.get(key);
                    log.info(id + ":get key="+key+" val="+val);

                    if (val != null) {
                        key = random.nextInt(keyDomainMax) + keyDomainMax;
                    }

                    SampleValue v = new SampleValue(key);
                    txMap.put(key, new SampleValue(key));
                    log.info(id + ":put key="+key+" val="+val);

                    tCtx.commitTransaction();
                }
                catch (Exception e) {
                    log.info(id + ": "+e);
                    tCtx.rollbackTransaction();
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());
    }
}
