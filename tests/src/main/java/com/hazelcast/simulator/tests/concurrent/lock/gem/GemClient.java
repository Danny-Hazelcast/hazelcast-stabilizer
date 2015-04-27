package com.hazelcast.simulator.tests.concurrent.lock.gem;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class GemClient {
    private static final ILogger log = Logger.getLogger(GemClient.class);

    public String id;
    public String mapBaseName = this.getClass().getSimpleName();
    public int lockerThreadsCount = 3;
    public int maxKeys = 100;
    public String keyPreFix = "A";
    public long timeoutMillis = TimeUnit.SECONDS.toMillis(30);

    private List<Locker> lockers = new ArrayList();
    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id = testContext.getTestId();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for(int i=0; i<lockerThreadsCount; i++){
            lockers.add(new Locker());
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for(Locker l : lockers){
            spawner.spawn(l);
        }
        spawner.awaitCompletion();
    }

    private class Locker implements Runnable {
        Random random = new Random();
        AtomicLong timeStamp = new AtomicLong(System.currentTimeMillis());
        AtomicReference<ILock> lockRef = new AtomicReference();

        public void run() {
            while (!testContext.isStopped()) {
                String key = keyPreFix + random.nextInt(maxKeys);
                ILock lock = targetInstance.getLock(key);
                try {
                    lock.lock();
                    try {
                        long now = System.currentTimeMillis();
                        lockRef.set(lock);
                        timeStamp.set(now);
                        IMap m = targetInstance.getMap(mapBaseName);
                        m.put(key, now);
                    } finally {
                        lock.unlock();
                    }
                } catch (Exception e) {
                    log.warning(e);
                }
            }
        }
    }

    private class blockedChecker extends Thread {
        private List<Locker> lockers;

        public blockedChecker(List<Locker> lockers){
            this.lockers = lockers;
        }

        public void run() {
            while (!testContext.isStopped()) {
                long now = System.currentTimeMillis();
                for(Locker l : lockers){
                    long ts = l.timeStamp.get();

                    if (ts + timeoutMillis < now) {
                        System.out.println(l.lockRef.get() + " is locked for " + TimeUnit.MILLISECONDS.toMinutes(now - ts) + " mins!");
                    }
                }
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class infoThread extends Thread {
        public void run() {
            while (!testContext.isStopped()) {

                Set<Member> members = targetInstance.getCluster().getMembers();
                System.out.println(id+": cluster sz="+members.size());
                System.out.println(id+": LocalEndpoint=" + targetInstance.getLocalEndpoint());

                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

    }
}
