package com.hazelcast.simulator.common.messaging;

import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@MessageSpec(value = "oom", description = "Starts a new thread allocating memory in JVM heap indefinitely.")
public class UseAllMemoryMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(UseAllMemoryMessage.class);
    private static final List<Object> ALLOCATION_LIST = new ArrayList<Object>();

    private final int bufferSize = 1000;
    private final int delay;

    public UseAllMemoryMessage(MessageAddress messageAddress, int delay) {
        super(messageAddress);
        this.delay = delay;
    }

    public UseAllMemoryMessage(MessageAddress messageAddress) {
        this(messageAddress, 0);
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return super.disableMemberFailureDetection();
    }

    @Override
    public boolean removeFromAgentList() {
        return super.removeFromAgentList();
    }

    @Override
    public void run() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                LOGGER.debug("Starting a thread to consume all memory");
                while (!interrupted()) {
                    try {
                        allocateMemory();
                    } catch (OutOfMemoryError e) {
                        EmptyStatement.ignore(e);
                    }
                }
            }

            private void allocateMemory() {
                while (!interrupted()) {
                    byte[] buff = new byte[bufferSize];
                    ALLOCATION_LIST.add(buff);
                    sleepMillisInterruptThread(delay);
                }
            }

            private void sleepMillisInterruptThread(int sleepMillis) {
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted during sleep on map size: " + ALLOCATION_LIST.size());
                    Thread.currentThread().interrupt();
                }
            }
        };
        thread.start();
    }
}
