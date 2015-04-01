package com.hazelcast.simulator.tests.wang;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import java.util.*;

public class Wang {

    private final static ILogger log = Logger.getLogger(Wang.class);

    private enum Opp {
        PUT,
        GET,
        REMOVE,
        GETALL,
        SIZE,
        MAP_VALUES
    }

    public int threadCount=10;
    public int totalMaps=1;
    public int totalMultiMaps=0;
    public int maxKeysPerMap=100000;

    public int phaseItterations=100;

    public boolean dynamicValueSizes=true;
    public int valueByteArraySize=3000;

    public String mapbaseName="map";
    public String mmbaseName="multi";

    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public OperationSelectorBuilder<Opp> oppBuilder = new OperationSelectorBuilder<Opp>();


    public static boolean isMemberNode(HazelcastInstance instance) {
        return instance instanceof HazelcastInstanceProxy;
    }

    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();

        oppBuilder.addOperation(Opp.PUT, 0.3);
        oppBuilder.addOperation(Opp.GET, 0.1);
        oppBuilder.addOperation(Opp.REMOVE, 0.1);
        oppBuilder.addOperation(Opp.GETALL, 0.2);
        oppBuilder.addOperation(Opp.SIZE, 0.2);
        oppBuilder.addOperation(Opp.MAP_VALUES, 0.1);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        for(int i=0; i<totalMaps; i++){
            Map m = targetInstance.getMap(mapbaseName+i);
        }

        for(int i=0; i<totalMultiMaps; i++){
            MultiMap m = targetInstance.getMultiMap(mmbaseName+i);
        }
    }


    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        Set<Worker> s = new HashSet();
        for (int k = 0; k < threadCount; k++) {
            Worker w = new Worker();
            spawner.spawn(w);
            s.add(w);
        }
        spawner.awaitCompletion();

        for(Worker w : s){
            if(dynamicValueSizes){
                log.info(id + ": w.valueSet.size()=" + w.valueSet.size());
                for(byte[] b : w.valueSet){
                    log.info(id + ": valuesSet Value v size="+ b.length);
                }
            }
        }
    }


    private class Worker implements Runnable {
        public Random random = new Random();
        public List<byte[]> valueSet = new ArrayList<byte[]>();
        public Map mapValueSet = new HashMap();
        public Set getKeySet = new HashSet();

        public OperationSelector<Opp> opp = oppBuilder.build();

        public Worker(){

            byte[] value = new byte[valueByteArraySize];
            random.nextBytes(value);
            valueSet.add(value);

            if(dynamicValueSizes){
                for(int i=0; i<15; i++){

                    int diff = 250 + random.nextInt(valueByteArraySize * 2);
                    value = new byte[diff];
                    random.nextBytes(value);
                    valueSet.add(value);
                    mapValueSet.put(random.nextInt(), value);
                }
            }
        }

        public void run(){

            //mapSizePhase();
            //putPhase();
            //putGetPhase();
            //getAllPhase();
            //mapValuesPhase();

            log.info(id + "OPPSPhase");

            while (!testContext.isStopped()) {

                int mapNumber = random.nextInt(totalMaps);
                IMap m = targetInstance.getMap(mapbaseName+mapNumber);

                if(random.nextBoolean()){
                    m.values();
                }else{
                    m.size();
                }
                //mapSizePhase();
                //phaseRandom();

            }
        }

        public void putPhase(){
            log.info(id + "putPhase");

            for(int i=0; i<totalMaps; i++){
                IMap m = targetInstance.getMap(mapbaseName+i);
                for(int k=0; k<maxKeysPerMap; k++){
                    byte[] v = valueSet.get(random.nextInt(valueSet.size()));
                    m.put(k, v);
                }
            }
        }


        public void putGetPhase(){
            log.info(id + "putGetPhase");

            for(int i=0; i<phaseItterations; i++){
                int mapNumber = random.nextInt(totalMaps);
                IMap m = targetInstance.getMap(mapbaseName+mapNumber);

                int k = random.nextInt(maxKeysPerMap);
                byte[] v = valueSet.get(random.nextInt(valueSet.size()));

                if(random.nextBoolean()){
                    m.put(k, v);
                }else {
                    m.get(k);
                }
            }
        }

        public void getAllPhase(){
            log.info(id + "getALLPhase");
            for(int i=0; i<phaseItterations; i++){

                int mapNumber = random.nextInt(totalMaps);
                IMap m = targetInstance.getMap(mapbaseName+mapNumber);

                getKeySet.clear();
                int getSetMax = 50 + random.nextInt(100);
                for(int j=0; j<getSetMax; j++){
                    getKeySet.add(random.nextInt(maxKeysPerMap));
                }
                m.getAll(getKeySet);
            }
        }

        public void mapValuesPhase(){
            log.info(id + "mapValuesPhase");
            for(int i=0; i<phaseItterations; i++){

                int mapNumber = random.nextInt(totalMaps);
                IMap m = targetInstance.getMap(mapbaseName+mapNumber);

                m.values();
            }
        }

        public void mapSizePhase(){
            log.info(id + "mapSizePhase");
            for(int i=0; i<phaseItterations; i++){

                int mapNumber = random.nextInt(totalMaps);
                IMap m = targetInstance.getMap(mapbaseName+mapNumber);

                m.size();
            }
        }




        public void phaseRandom(){
            log.info(id + "phaseRandom");

            int i = random.nextInt(totalMaps);
            IMap m = targetInstance.getMap(mapbaseName+i);

            int k = random.nextInt(maxKeysPerMap);
            byte[] v = valueSet.get(random.nextInt(valueSet.size()));


            switch (opp.select()) {
                case SIZE:
                    m.size();
                    break;

                case PUT:
                    m.put(k, v);
                    break;

                case GET:
                    m.get(k);
                    break;

                case REMOVE:
                    m.remove(k);
                    break;

                case GETALL:
                    getKeySet.clear();
                    int getSetMax = 50 + random.nextInt(100);
                    for(i=0; i<getSetMax; i++){
                        getKeySet.add(random.nextInt(maxKeysPerMap));
                    }
                    m.getAll(getKeySet);
                    break;

                case MAP_VALUES:
                    Collection c = m.values();
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        if(isMemberNode(targetInstance)){
            log.info(id + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        }
    }

}