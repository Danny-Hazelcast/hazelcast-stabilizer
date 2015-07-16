package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.test.TestContext;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.fillString;
import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.formatLong;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static java.lang.String.format;

/**
 * Monitors the performance of all running tests on {@link MemberWorker} and {@link ClientWorker} instances.
 */
class WorkerPerformanceMonitor {

    private static final int DEFAULT_MONITORING_INTERVAL_SECONDS = 5;

    private final AtomicBoolean started = new AtomicBoolean();

    private final MonitorThread thread;

    WorkerPerformanceMonitor(Collection<TestContainer<TestContext>> testContainers) {
        this(testContainers, DEFAULT_MONITORING_INTERVAL_SECONDS);
    }

    WorkerPerformanceMonitor(Collection<TestContainer<TestContext>> testContainers, int intervalSeconds) {
        this.thread = new MonitorThread(testContainers, intervalSeconds);
    }

    boolean start() {
        if (!started.compareAndSet(false, true)) {
            return false;
        }

        thread.start();
        return true;
    }

    void stop() {
        thread.stop = true;
        thread.interrupt();
    }

    private static final class MonitorThread extends Thread {

        private static final Logger LOGGER = Logger.getLogger(MonitorThread.class);

        private final File globalPerformanceFile = new File("performance.txt");
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, TestStats> testStats = new HashMap<String, TestStats>();
        private final Collection<TestContainer<TestContext>> testContainers;
        private final int intervalSeconds;

        private long globalLastOpsCount;
        private long globalLastTimeMillis = System.currentTimeMillis();

        private volatile boolean stop;

        private MonitorThread(Collection<TestContainer<TestContext>> testContainers, int intervalSeconds) {
            super("WorkerPerformanceMonitorThread");
            setDaemon(true);

            this.testContainers = testContainers;
            this.intervalSeconds = intervalSeconds;

            writeHeaderToFile(globalPerformanceFile, true);
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    sleepSeconds(intervalSeconds);
                    writeStatsToFiles();
                } catch (Throwable t) {
                    LOGGER.fatal("Failed to run performance monitor", t);
                }
            }
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private void writeStatsToFiles() {
            String timestamp = simpleDateFormat.format(new Date());
            long currentTimeMillis = System.currentTimeMillis();

            long numberOfTests = 0;
            long globalDeltaOps = 0;
            long deltaTimeMillis = currentTimeMillis - globalLastTimeMillis;

            // test performance stats
            for (TestContainer testContainer : testContainers) {
                String testId = testContainer.getTestContext().getTestId();

                long currentOpsCount = getOpsCount(testContainer);

                TestStats stats = testStats.get(testId);
                if (stats == null) {
                    if (currentOpsCount == 0) {
                        // skip tests without performance counts
                        continue;
                    }
                    File testFile = new File("performance-" + (testId.isEmpty() ? "default" : testId) + ".txt");
                    writeHeaderToFile(testFile, false);

                    stats = new TestStats(testFile);
                    testStats.put(testId, stats);
                }

                long deltaOps = currentOpsCount - stats.lastOpsCount;
                double opsPerSecond = (deltaOps * 1000d) / deltaTimeMillis;

                stats.lastOpsCount = currentOpsCount;
                globalDeltaOps += deltaOps;
                numberOfTests++;

                writeStatsToFile(stats.performanceFile, timestamp, currentOpsCount, deltaOps, opsPerSecond, 0, 0);
            }

            // global performance stats
            double globalOpsPerSecond = (globalDeltaOps * 1000d) / deltaTimeMillis;

            globalLastOpsCount += globalDeltaOps;
            globalLastTimeMillis = currentTimeMillis;

            if (numberOfTests > 0) {
                writeStatsToFile(globalPerformanceFile, timestamp, globalLastOpsCount, globalDeltaOps, globalOpsPerSecond,
                        numberOfTests, testContainers.size());
            }
        }

        private long getOpsCount(TestContainer container) {
            long operationCount = container.getOperationCount();
            if (operationCount > 0) {
                return operationCount;
            }
            return 0;
        }

        private void writeHeaderToFile(File file, boolean isGlobal) {
            String columns = "Timestamp                      Ops (sum)        Ops (delta)                Ops/s";
            if (isGlobal) {
                columns += " Number of tests";
            }
            appendText(format("%s%n%s%n", columns, fillString(columns.length(), '-')), file);
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private void writeStatsToFile(File file, String timestamp, long opsSum, long opsDelta, double opsPerSecDelta,
                                      long numberOfTests, long totalTests) {
            String dataString = "[%s] %s ops %s ops %s ops/s";
            if (totalTests > 0) {
                dataString += " %s/%s";
            }
            int fieldLength = 1;
            if (totalTests >= 10) {
                fieldLength = 2;
            } else if (totalTests >= 100) {
                fieldLength = 3;
            }
            appendText(format(dataString + "\n", timestamp, formatLong(opsSum, 14), formatLong(opsDelta, 14),
                    formatDouble(opsPerSecDelta, 14), formatLong(numberOfTests, 14 - fieldLength),
                    formatLong(totalTests, fieldLength)), file);
        }

        private static final class TestStats {
            private final File performanceFile;
            private long lastOpsCount;

            private TestStats(File performanceFile) {
                this.performanceFile = performanceFile;
            }
        }
    }
}
