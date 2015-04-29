package com.hazelcast.simulator.tests.concurrent.lock.gem;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;


public class GemTest {
    private static final ILogger log = Logger.getLogger(GemTest.class);

    public String id;
    public String mapBaseName = "lockMap";
    public int lockerThreadsCount = 3;
    public int maxKeys = 100;
    public String keyPreFix = "A";
    public long reportStuckMillis = TimeUnit.SECONDS.toMillis(30);
    public long failStuckMillis = TimeUnit.SECONDS.toMillis(301);

    private List<Locker> lockers = new ArrayList();
    private BlockedChecker  blockedChecker;
    private InfoThread infoThread;
    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id = testContext.getTestId();

        for(int i=0; i<lockerThreadsCount; i++){
            lockers.add(new Locker(i));
        }
        blockedChecker = new BlockedChecker(lockers);
        infoThread = new InfoThread();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for(Locker l : lockers){
            spawner.spawn(l);
        }
        spawner.spawn(blockedChecker);

        infoThread.start();
        spawner.awaitCompletion();
    }

    private class Locker implements Runnable {
        Random random = new Random();
        AtomicLong progressTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong itteration = new AtomicLong(0);
        String name;

        public Locker(int i){
            name = this.getClass().getSimpleName()+""+i;
        }

        public void run() {
            while (!testContext.isStopped()) {
                long now = System.currentTimeMillis();
                progressTime.set(now);
                itteration.incrementAndGet();

                String key = keyPreFix + random.nextInt(maxKeys);
                ILock lock = targetInstance.getLock(key);
                try {
                    lock.lock();
                    try {
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

    private class BlockedChecker implements Runnable {
        private List<Locker> lockers;

        public BlockedChecker(List<Locker> lockers){
            this.lockers = lockers;
        }

        public void run() {
            while (!testContext.isStopped()) {
                long now = System.currentTimeMillis();
                for(Locker l : lockers){
                    long ts = l.progressTime.get();

                    if (ts + reportStuckMillis < now) {
                        log.warning(id + ": " + l.name + " blocked at "+l.itteration.get()+" for " + TimeUnit.MILLISECONDS.toSeconds(now - ts) + " sec");
                    }
                    if (ts + failStuckMillis < now) {
                        throw new IllegalStateException(id + ": " + l.name + " blocked at "+l.itteration.get()+" for " + TimeUnit.MILLISECONDS.toSeconds(now - ts) + " sec!");
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

    private class InfoThread extends Thread {
        public void run() {
            while (!testContext.isStopped()) {
                if (isMemberNode(targetInstance)) {
                    Set<Member> members = targetInstance.getCluster().getMembers();
                    log.info(id + ": cluster sz=" + members.size());
                    log.info(id + ": LocalEndpoint=" + targetInstance.getLocalEndpoint());

                    Collection<Client> clients = targetInstance.getClientService().getConnectedClients();
                    log.info(id+ ": connected clients=" + clients.size());
                    for (Client client : clients) {
                        log.info(id+": "+client);
                    }
                }

                IMap map = targetInstance.getMap(mapBaseName);
                log.info(id+": map "+map.getName()+" sz="+map.size());

                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
