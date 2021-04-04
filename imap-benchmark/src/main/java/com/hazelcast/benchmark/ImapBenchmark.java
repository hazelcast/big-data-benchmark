package com.hazelcast.benchmark;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.HdrHistogram.Histogram;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ImapBenchmark implements Runnable {

    public static final String DEFAULT_PROPERTIES_FILENAME = "imap-benchmark.properties";
    public static final String LATENCY_HISTOGRAM_FILENAME = "lat-histo.txt";

    public static final String PROP_NUM_DISTINCT_KEYS = "num-distinct-keys";
    public static final String PROP_OPS_PER_SECOND = "ops-per-second";
    public static final String PROP_NUM_PARALLEL_CLIENTS = "num-parallel-clients";
    public static final String PROP_WARMUP_SECONDS = "warmup-seconds";
    public static final String PROP_MEASUREMENT_SECONDS = "measurement-seconds";

    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private static final long REPORT_PERIOD_SECONDS = 2;

    private final int threadIndex;
    private final double opsPerNanosecond;
    private final long startNanoTime;
    private final int lowKey;
    private final int keyLimit;
    private final IMap<Long, byte[]> imap;
    private final HazelcastInstance client;
    private final byte[] value = new byte[1000];
    private final Histogram histo = new Histogram(3);
    private final long warmupOpCount;
    private final long totalOpCount;

    private long lastReportNanoTime;
    private long nowNanos;
    private long opCount;
    private long producedAtLastReport;
    private long worstLatencySinceLastReport;

    public static void main(String[] args) throws InterruptedException, IOException {
        String propsPath = args.length > 0 ? args[0] : DEFAULT_PROPERTIES_FILENAME;
        Properties props = loadProps(propsPath);
        try {
            int numThreads = parseIntProp(props, PROP_NUM_PARALLEL_CLIENTS);
            int opsPerSecond = parseIntProp(props, PROP_OPS_PER_SECOND);
            int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            double opsPerSecondPerThread = (double) opsPerSecond / numThreads;
            Range[] keysByThread = distributeRange(numThreads, numDistinctKeys);
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            long startNanoTime = System.nanoTime() + MILLISECONDS.toNanos(100);
            List<ImapBenchmark> benchmarkTasks = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                Range threadKeys = keysByThread[i];
                ImapBenchmark benchmarkTask = new ImapBenchmark(
                        i, threadKeys.lowerInclusive, threadKeys.upperExclusive,
                        opsPerSecondPerThread, startNanoTime, warmupSeconds, measurementSeconds);
                executorService.submit(benchmarkTask);
                benchmarkTasks.add(benchmarkTask);
            }
            printParams(
                    PROP_NUM_DISTINCT_KEYS, numDistinctKeys,
                    PROP_OPS_PER_SECOND, opsPerSecond,
                    PROP_NUM_PARALLEL_CLIENTS, numThreads
            );
            executorService.shutdown();
            boolean didTerminate = executorService.awaitTermination(
                    warmupSeconds + measurementSeconds + 30, SECONDS);
            if (!didTerminate) {
                System.err.println("Benchmark seems to be stuck. Aborting.");
                executorService.shutdownNow();
                //noinspection ResultOfMethodCallIgnored
                executorService.awaitTermination(2, SECONDS);
                System.exit(1);
            }
            Histogram latencyHisto = benchmarkTasks
                    .stream()
                    .map(bt -> bt.histo)
                    .reduce(new Histogram(3), (hg1, hg2) -> { hg1.add(hg2); return hg1; });
            try (FileOutputStream out = new FileOutputStream(LATENCY_HISTOGRAM_FILENAME)) {
                latencyHisto.outputPercentileDistribution(new PrintStream(out), 1000.0);
            }
            latencyHisto.outputPercentileDistribution(System.out, 1000.0);
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println("Usage:");
            System.err.println("  " + ImapBenchmark.class.getSimpleName() + " [props-file]");
            System.exit(1);
        }
    }

    private ImapBenchmark(
            int threadIndex, int lowKey, int keyLimit, double opsPerSecond, long startNanoTime,
            long warmupSeconds, long measurementSeconds
    ) {
        if (opsPerSecond <= 0) {
            throw new RuntimeException("opsPerSecond = " + opsPerSecond);
        }
        this.lowKey = lowKey;
        this.keyLimit = keyLimit;
        this.threadIndex = threadIndex;
        this.startNanoTime = startNanoTime;
        client = HazelcastClient.newHazelcastClient();
        imap = client.getMap("bencmark-map");
        lastReportNanoTime = startNanoTime;
        warmupOpCount = (long) (warmupSeconds * opsPerSecond);
        totalOpCount = (long) ((warmupSeconds + measurementSeconds) * opsPerSecond);
        opsPerNanosecond = opsPerSecond / NANOS_PER_SECOND;
        ThreadLocalRandom.current().nextBytes(value);
    }

    @Override
    public void run() {
        try {
            nowNanos = System.nanoTime();
            long currentKey = lowKey;
            while (opCount < totalOpCount) {
                long expectedOpCount = (long) ((nowNanos - startNanoTime) * opsPerNanosecond) + 1;
                if (opCount >= expectedOpCount) {
                    sleepUntilDue(expectedOpCount + 1);
                    nowNanos = System.nanoTime();
                    continue;
                }
                imap.set(currentKey, value);
                nowNanos = System.nanoTime();
                opCount++;
                long latencyNanos = nowNanos - startNanoTime - (long) (opCount / opsPerNanosecond);
                if (opCount > warmupOpCount) {
                    histo.recordValue(latencyNanos);
                }
                currentKey++;
                if (currentKey == keyLimit) {
                    currentKey = lowKey;
                }
                reportThroughput();
            }
        } catch (Exception e) {
            System.err.println("Worker #" + threadIndex + " failed");
            e.printStackTrace();
        } finally {
            client.shutdown();
        }
    }

    private void sleepUntilDue(long expectedProduced) {
        long due = startNanoTime + (long) (expectedProduced / opsPerNanosecond);
        long nanosUntilDue = due - nowNanos;
        long sleepNanos = nanosUntilDue - MICROSECONDS.toNanos(10);
        if (sleepNanos > 0) {
            LockSupport.parkNanos(sleepNanos);
        }
    }

    private void reportThroughput() {
        final long nanosSinceLastReport = nowNanos - lastReportNanoTime;
        if (NANOSECONDS.toSeconds(nanosSinceLastReport) < REPORT_PERIOD_SECONDS) {
            return;
        }
        System.out.printf("%,2d: %,.0f events/second, %,d ms worst latency%n",
                threadIndex,
                (double) NANOS_PER_SECOND * (opCount - producedAtLastReport) / nanosSinceLastReport,
                NANOSECONDS.toMillis(worstLatencySinceLastReport));
        worstLatencySinceLastReport = 0;
        producedAtLastReport = opCount;
        lastReportNanoTime = nowNanos;
    }

    private static Range[] distributeRange(int numDivisions, int fullRange) {
        double lengthPerThread = (double) fullRange / numDivisions;
        Range[] rangesByThread = new Range[numDivisions];
        for (int i = 0; i < numDivisions; i++) {
            rangesByThread[i] = new Range(
                    (int) Math.round(i * lengthPerThread),
                    (int) Math.round((i + 1) * lengthPerThread));
        }
        return rangesByThread;
    }

    private static final class Range {
        final int lowerInclusive;
        final int upperExclusive;

        Range(int lowerInclusive, int upperExclusive) {
            this.lowerInclusive = lowerInclusive;
            this.upperExclusive = upperExclusive;
        }
    }

    static Properties loadProps(String propsFilename) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propsFilename));
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Can't read file " + propsFilename);
            System.exit(2);
        }
        return props;
    }

    public static String ensureProp(Properties props, String propName) throws ValidationException {
        String prop = props.getProperty(propName);
        if (prop == null || prop.isEmpty()) {
            throw new ValidationException("Missing property: " + propName);
        }
        return prop;
    }

    public static int parseIntProp(Properties props, String propName) throws ValidationException {
        String prop = ensureProp(props, propName);
        try {
            return parseInt(prop.replace("_", ""));
        } catch (NumberFormatException e) {
            throw new ValidationException(
                    "Invalid property format, correct example is 9_999: " + propName + "=" + prop);
        }
    }

    public static void printParams(Object... namesAndValues) {
        for (int i = 0; i < namesAndValues.length; i += 2) {
            String name = (String) namesAndValues[i];
            Object value = namesAndValues[i + 1];
            if (value instanceof Integer) {
                value = String.format("%,d", value);
            }
            System.out.println("    " + name + "=" + value);
        }
    }
}
